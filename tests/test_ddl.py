from datetime import date

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.runs import fail_run, finish_run, start_run


@pytest.fixture
def con(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    apply_ddl(connection)
    yield connection
    connection.close()


def _row(con, run_id):
    return con.execute(
        "SELECT status, finished_at IS NOT NULL, duration_ms, rows_by_stage, error, git_sha "
        "FROM ops.pipeline_runs WHERE run_id = ?",
        [run_id],
    ).fetchone()


def test_apply_ddl_twice_is_noop(con):
    start_run(con, "r1", date(2025, 3, 16))
    apply_ddl(con)
    apply_ddl(con)
    schemas = {
        r[0] for r in con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    }
    assert {"raw", "staging", "marts", "ops"} <= schemas
    assert con.execute("SELECT count(*) FROM ops.pipeline_runs").fetchone()[0] == 1
    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='ops'"
        ).fetchall()
    }
    assert {"pipeline_runs", "dq_results"} <= tables


def test_run_running_to_success(con):
    start_run(con, "r1", date(2025, 3, 16), settlement_file="x.csv")
    status, finished, *_ = _row(con, "r1")
    assert status == "RUNNING"
    assert not finished

    finish_run(con, "r1", {"raw.transactions": 1060})
    status, finished, duration, rows, error, _ = _row(con, "r1")
    assert status == "SUCCESS"
    assert finished
    assert duration >= 0
    assert '"raw.transactions"' in rows
    assert error is None


def test_run_running_to_failed(con):
    start_run(con, "r2", date(2025, 3, 16))
    fail_run(con, "r2", ValueError("boom"))
    status, finished, _, _, error, _ = _row(con, "r2")
    assert status == "FAILED"
    assert finished
    assert error == "boom"


def test_invalid_status_rejected(con):
    with pytest.raises(Exception, match="CHECK|constraint"):
        con.execute(
            "INSERT INTO ops.pipeline_runs (run_id, reference_date, status, started_at) "
            "VALUES ('x', DATE '2025-03-16', 'BOGUS', now())"
        )
