from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.ingest.cdc import load_raw_cdc
from recon.transform.staging import build_cdc_staging

SAMPLE = Path(__file__).parent.parent / "docs" / "sample-data"

T1, T2, T3 = "2025-03-01T10:00:00+00:00", "2025-03-02T10:00:00+00:00", "2025-03-03T10:00:00+00:00"
UUID_A, UUID_B, UUID_C, UUID_D, UUID_E = (
    f"00000000-0000-0000-0000-00000000000{i}" for i in range(1, 6)
)


def _sql(value) -> str:
    return "NULL" if value is None else repr(value)


def _write(path: Path, rows: list[dict], columns: list[str]) -> None:
    mem = duckdb.connect()
    cols = ", ".join(columns)
    values = ", ".join("(" + ", ".join(_sql(r.get(c)) for c in columns) + ")" for r in rows)
    mem.execute(f"COPY (SELECT * FROM (VALUES {values}) t({cols})) TO '{path}' (FORMAT parquet)")
    mem.close()


def _txn(id_, tid, op, ts, status="PENDING", merchant="merch_001", amount=10.5):
    return {
        "id": id_, "transaction_id": tid, "merchant_id": merchant, "amount": amount,
        "currency": "BRL", "status": status, "description": "x",
        "created_at": T1, "updated_at": ts, "Op": op, "_timestamp": ts,
    }  # fmt: skip


TXN_COLS = [*_txn(1, "a", "I", T1), "payment_method"]
RUN_COLS = ["id", "reference_date", "file_name", "status", "total_transactions",
            "started_at", "completed_at", "created_at", "Op", "_timestamp"]  # fmt: skip
RES_COLS = ["id", "run_id", "transaction_id", "merchant_id", "category", "internal_amount",
            "processor_amount", "difference", "created_at", "Op", "_timestamp"]  # fmt: skip
COMPANY_COLS = ["id", "merchant_id", "legal_name", "trade_name", "document", "primary_cnae",
                "created_at", "updated_at", "Op", "_timestamp"]  # fmt: skip


def _run(id_, op, ts, status):
    return {"id": id_, "reference_date": "2025-03-01", "file_name": "f.csv", "status": status,
            "total_transactions": 5, "started_at": T1, "completed_at": None, "created_at": T1,
            "Op": op, "_timestamp": ts}  # fmt: skip


def _result(id_, run_id):
    return {"id": id_, "run_id": run_id, "transaction_id": UUID_A, "merchant_id": "MERCH_001",
            "category": "MATCHED", "internal_amount": 1.0, "processor_amount": 1.0,
            "difference": 0.0, "created_at": T1, "Op": "I", "_timestamp": T1}  # fmt: skip


def _company(id_, op, ts, legal):
    return {"id": id_, "merchant_id": "MERCH_001", "legal_name": legal, "trade_name": "L",
            "document": "d", "primary_cnae": "c", "created_at": T1, "updated_at": ts,
            "Op": op, "_timestamp": ts}  # fmt: skip


@pytest.fixture
def stg(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    _write(
        src / "transactions_batch_1.parquet",
        [
            _txn(1, UUID_A, "I", T1, "PENDING"),
            _txn(2, UUID_A, "U", T2, "COMPLETED"),  # I -> U
            _txn(3, UUID_B, "I", T1, "COMPLETED"),
            _txn(3, UUID_B, "D", T2, "COMPLETED"),  # deleted
            _txn(4, UUID_C, "I", T1, "COMPLETED"),
            _txn(4, UUID_C, "I", T1, "COMPLETED"),  # CDC replay
            _txn(6, UUID_E, "I", T1, "COMPLETED"),
            _txn(6, UUID_E, "U", T2, "COMPLETED"),
            _txn(6, UUID_E, "D", T2, "COMPLETED"),  # D empata com U no _timestamp: D vence
            _txn(5, UUID_D, "I", T1, "COMPLETED", merchant=" merch_009 "),
        ],
        TXN_COLS,
    )
    _write(
        src / "reconciliation_runs.parquet",
        [_run(1, "I", T1, "IN_PROGRESS"), _run(1, "U", T2, "COMPLETED")],
        RUN_COLS,
    )
    _write(src / "reconciliation_results.parquet", [_result(1, 1), _result(2, 99)], RES_COLS)
    _write(
        src / "enterprise_company.parquet",
        [
            _company(1, "I", T1, "v1"),
            _company(1, "U", T2, "v2"),
            _company(1, "U", T3, "v3"),
        ],
        COMPANY_COLS,
    )
    con = connect(tmp_path / "wh.duckdb")
    apply_ddl(con)
    load_raw_cdc(con, src, "r1")
    build_cdc_staging(con)
    yield con
    con.close()


def test_latest_version_wins(stg):
    row = stg.execute(
        "SELECT status, last_op, cdc_timestamp::VARCHAR FROM staging.stg_transactions "
        "WHERE transaction_id = ?",
        [UUID_A],
    ).fetchone()
    assert row[0] == "COMPLETED"
    assert row[1] == "U"
    assert row[2].startswith("2025-03-02")


def test_one_row_per_transaction(stg):
    assert stg.execute("SELECT count(*) FROM staging.stg_transactions").fetchone()[0] == 5
    assert (
        stg.execute(
            "SELECT count(*) FROM (SELECT 1 FROM staging.stg_transactions "
            "GROUP BY transaction_id HAVING count(*) > 1)"
        ).fetchone()[0]
        == 0
    )


def test_delete_is_flagged(stg):
    flags = dict(
        stg.execute("SELECT transaction_id, is_deleted FROM staging.stg_transactions").fetchall()
    )
    assert flags[UUID_B] is True
    assert flags[UUID_A] is False
    assert flags[UUID_E] is True


def test_cdc_replay_collapsed_and_counted(stg):
    assert stg.execute(
        "SELECT insert_count FROM staging.stg_transactions WHERE transaction_id = ?", [UUID_C]
    ).fetchone() == (2,)


def test_types_and_merchant_normalization(stg):
    amount, merchant, ts_type = stg.execute(
        "SELECT amount, merchant_id, typeof(created_at) FROM staging.stg_transactions "
        "WHERE transaction_id = ?",
        [UUID_D],
    ).fetchone()
    assert amount == Decimal("10.50")
    assert merchant == "MERCH_009"
    assert ts_type == "TIMESTAMP WITH TIME ZONE"


def test_run_latest_state(stg):
    assert stg.execute(
        "SELECT status, reference_date FROM staging.stg_reconciliation_runs"
    ).fetchall() == [("COMPLETED", date(2025, 3, 1))]


def test_orphan_run_flag(stg):
    flags = dict(
        stg.execute("SELECT id, has_orphan_run FROM staging.stg_reconciliation_results").fetchall()
    )
    assert flags == {1: False, 2: True}


def test_merchant_scd2_three_versions(stg):
    rows = stg.execute(
        "SELECT legal_name, valid_from::VARCHAR, valid_to::VARCHAR, is_current "
        "FROM staging.stg_merchants_history ORDER BY valid_from"
    ).fetchall()
    assert [r[0] for r in rows] == ["v1", "v2", "v3"]
    assert [r[3] for r in rows] == [False, False, True]
    assert rows[0][2] == rows[1][1]  # valid_to(v1) == valid_from(v2)
    assert rows[1][2] == rows[2][1]
    assert rows[2][2] is None


def test_rebuild_is_idempotent(stg):
    first = build_cdc_staging(stg)
    second = build_cdc_staging(stg)
    assert first == second


def test_sample_data_counts(tmp_path):
    con = connect(tmp_path / "wh.duckdb")
    apply_ddl(con)
    load_raw_cdc(con, SAMPLE, "r1")
    counts = build_cdc_staging(con)
    assert counts["staging.stg_transactions"] == 1000
    assert counts["staging.stg_reconciliation_runs"] == 10
    assert counts["staging.stg_reconciliation_results"] == 837
    # 6 linhas Op D; 1 transação foi reinserida depois do D, então 5 seguem deletadas
    assert (
        con.execute("SELECT count(*) FROM staging.stg_transactions WHERE is_deleted").fetchone()[0]
        == 5
    )
    assert (
        con.execute(
            "SELECT count(*) FROM staging.stg_reconciliation_results WHERE has_orphan_run"
        ).fetchone()[0]
        == 5
    )
    assert (
        con.execute(
            "SELECT count(*) FROM staging.stg_merchants_history WHERE is_current"
        ).fetchone()[0]
        == 50
    )
    con.close()


def test_staging_tolerates_missing_payment_method(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    cols = TXN_COLS[:-1]  # só o schema base, como no batch_1
    _write(src / "transactions_batch_1.parquet", [_txn(1, UUID_A, "I", T1)], cols)
    _write(src / "reconciliation_runs.parquet", [_run(1, "I", T1, "IN_PROGRESS")], RUN_COLS)
    _write(src / "reconciliation_results.parquet", [_result(1, 1)], RES_COLS)
    _write(src / "enterprise_company.parquet", [_company(1, "I", T1, "v1")], COMPANY_COLS)
    con = connect(tmp_path / "wh.duckdb")
    apply_ddl(con)
    load_raw_cdc(con, src, "r1")
    build_cdc_staging(con)
    assert con.execute("SELECT payment_method FROM staging.stg_transactions").fetchall() == [
        (None,)
    ]
    con.close()
