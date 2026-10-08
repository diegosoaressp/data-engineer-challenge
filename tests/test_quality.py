from datetime import date

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.logging import get_logger
from recon.quality.checks import BlockingCheckFailed, Check
from recon.quality.runner import run_checks

D = date(2025, 3, 16)


@pytest.fixture
def con(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    apply_ddl(connection)
    connection.execute("CREATE TABLE staging.t AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) v(k)")
    yield connection
    connection.close()


@pytest.fixture
def log():
    return get_logger("run-1", str(D))


def _write_marts(con):
    con.execute("CREATE TABLE IF NOT EXISTS marts.m (x INT)")
    con.execute("INSERT INTO marts.m VALUES (1)")


def _rows(con):
    return con.execute(
        "SELECT check_name, severity, passed, observed_value, sample_keys "
        "FROM ops.dq_results ORDER BY check_name"
    ).fetchall()


def test_block_failure_stops_before_marts(con, log):
    checks = [
        Check("bad_rows", "staging", "block", "SELECT k FROM staging.t WHERE k <> 'a'"),
        Check("ok", "staging", "block", "SELECT k FROM staging.t WHERE false"),
    ]
    with pytest.raises(BlockingCheckFailed) as exc:
        run_checks(con, "run-1", D, log, checks)
        _write_marts(con)  # nunca alcançado
    assert [r.check.name for r in exc.value.failed] == ["bad_rows"]
    assert (
        con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='m'"
        ).fetchone()[0]
        == 0
    )
    # todos os resultados, inclusive o que passou, ficam registrados
    assert _rows(con) == [
        ("bad_rows", "block", False, 2.0, "b,c"),
        ("ok", "block", True, 0.0, None),
    ]


def test_alert_failure_is_recorded_and_run_continues(con, log):
    checks = [Check("warn", "staging", "alert", "SELECT k FROM staging.t")]
    results = run_checks(con, "run-1", D, log, checks)
    _write_marts(con)
    assert not results[0].passed
    assert con.execute("SELECT count(*) FROM marts.m").fetchone()[0] == 1
    assert _rows(con) == [("warn", "alert", False, 3.0, "a,b,c")]


def test_threshold_allows_up_to_n_rows(con, log):
    checks = [Check("lenient", "staging", "block", "SELECT k FROM staging.t", threshold=3)]
    assert run_checks(con, "run-1", D, log, checks)[0].passed


def test_layer_filter(con, log):
    checks = [
        Check("a", "staging", "alert", "SELECT k FROM staging.t"),
        Check("b", "marts", "alert", "SELECT k FROM staging.t"),
    ]
    results = run_checks(con, "run-1", D, log, checks, layer="marts")
    assert [r.check.name for r in results] == ["b"]


def test_invalid_severity_rejected():
    with pytest.raises(ValueError):
        Check("x", "staging", "warn", "SELECT 1")  # type: ignore[arg-type]


def test_metric_check_records_scalar_and_compares_to_threshold(con, log):
    checks = [
        Check("rate_ok", "staging", "alert", "SELECT 0.04", threshold=0.05, kind="metric"),
        Check("rate_high", "staging", "alert", "SELECT 0.2", threshold=0.05, kind="metric"),
        Check(
            "rate_null", "staging", "alert", "SELECT NULL::DOUBLE", threshold=0.05, kind="metric"
        ),
    ]
    run_checks(con, "run-1", D, log, checks)
    assert _rows(con) == [
        ("rate_high", "alert", False, 0.2, None),
        ("rate_null", "alert", True, None, None),
        ("rate_ok", "alert", True, 0.04, None),
    ]


def test_metric_block_failure_raises(con, log):
    checks = [Check("diff", "staging", "block", "SELECT 3", kind="metric")]
    with pytest.raises(BlockingCheckFailed):
        run_checks(con, "run-1", D, log, checks)


def test_invalid_kind_rejected():
    with pytest.raises(ValueError):
        Check("x", "staging", "alert", "SELECT 1", kind="scalar")  # type: ignore[arg-type]
