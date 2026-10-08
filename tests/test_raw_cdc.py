from pathlib import Path

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.ingest.cdc import load_raw_cdc

SAMPLE = Path(__file__).parent.parent / "docs" / "sample-data"


@pytest.fixture
def con(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    apply_ddl(connection)
    yield connection
    connection.close()


def test_batch1_payment_method_is_null(con):
    load_raw_cdc(con, SAMPLE, "r1")
    by_file = dict(
        con.execute(
            "SELECT _source_file, count(payment_method) FROM raw.transactions GROUP BY 1"
        ).fetchall()
    )
    assert by_file["transactions_batch_1.parquet"] == 0
    assert by_file["transactions_batch_2.parquet"] > 0
    total = con.execute("SELECT count(*) FROM raw.transactions").fetchone()[0]
    assert total == 1060


def test_lineage_columns(con):
    load_raw_cdc(con, SAMPLE, "run-42")
    assert con.execute(
        "SELECT DISTINCT _pipeline_run_id FROM raw.reconciliation_runs"
    ).fetchall() == [("run-42",)]


def test_rerun_keeps_same_counts(con):
    first = load_raw_cdc(con, SAMPLE, "r1")
    second = load_raw_cdc(con, SAMPLE, "r2")
    assert first == second
    assert first["raw.reconciliation_results"] == 837


def test_missing_source_raises(con, tmp_path):
    with pytest.raises(FileNotFoundError):
        load_raw_cdc(con, tmp_path, "r1")
