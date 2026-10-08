import json
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.pipeline import run_pipeline
from recon.transform.marts import build_aggregates, build_fct_reconciliation, publish_marts

D = date(2025, 3, 16)
SAMPLE = Path(__file__).parent.parent / "docs" / "sample-data"
SAMPLE_CSV = SAMPLE / "settlement_paysettler.csv"
MARTS = (
    "fct_reconciliation",
    "fct_reconciliation_daily",
    "fct_reconciliation_run",
    "fct_financial_daily",
    "fct_internal_volume_daily",
    "dim_merchant",
)


def _tid(n: int) -> str:
    return f"00000000-0000-0000-0000-{n:012d}"


@pytest.fixture
def con(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    apply_ddl(connection)
    connection.execute(
        """
        CREATE TABLE staging.stg_transactions (
            transaction_id VARCHAR, merchant_id VARCHAR, currency VARCHAR,
            amount DECIMAL(18, 2), status VARCHAR, created_at TIMESTAMPTZ, is_deleted BOOLEAN);
        CREATE TABLE staging.stg_settlements (
            processor VARCHAR, transaction_id VARCHAR, merchant_id VARCHAR, currency VARCHAR,
            amount DECIMAL(18, 2), status VARCHAR, has_reversal BOOLEAN, settled_at TIMESTAMPTZ);
        CREATE TABLE staging.stg_reconciliation_runs (
            id BIGINT, reference_date DATE, file_name VARCHAR, status VARCHAR,
            total_transactions VARCHAR, started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ);
        CREATE TABLE staging.stg_reconciliation_results (
            id BIGINT, run_id BIGINT, transaction_id VARCHAR, category VARCHAR, last_op VARCHAR);
        CREATE TABLE raw.settlements (transaction_id VARCHAR);
        CREATE TABLE staging.stg_merchants_history (
            merchant_id VARCHAR, legal_name VARCHAR, trade_name VARCHAR, document VARCHAR,
            primary_cnae VARCHAR, valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ,
            is_current BOOLEAN, op VARCHAR);
        """
    )
    # (n, internal amount/currency/status, created) x (settlement amount/currency/status/at)
    internal = [
        (1, "10.00", "BRL", "COMPLETED", "2025-03-15T10:00:00Z"),
        (2, "20.00", "BRL", "COMPLETED", "2025-03-15T10:00:00Z"),
        (3, "5.00", "BRL", "COMPLETED", "2025-03-14T10:00:00Z"),
        (5, "3.00", "BRL", "FAILED", "2025-03-16T01:00:00Z"),
        (6, "8.00", "USD", "PENDING", "2025-03-14T10:00:00Z"),
    ]
    settled = [
        (1, "10.00", "BRL", "SETTLED", "2025-03-16T10:00:00Z"),
        (2, "25.00", "BRL", "SETTLED", "2025-03-16T10:00:00Z"),
        (3, "5.00", "BRL", "REVERSED", "2025-03-16T11:00:00Z"),
        (4, "7.00", "USD", "SETTLED", "2025-03-16T12:00:00Z"),
        (6, "8.00", "USD", "SETTLED", "2025-03-16T12:00:00Z"),
    ]
    for n, amount, cur, status, created in internal:
        connection.execute(
            "INSERT INTO staging.stg_transactions VALUES (?, 'M1', ?, ?, ?, ?, false)",
            [_tid(n), cur, amount, status, created],
        )
    for n, amount, cur, status, at in settled:
        connection.execute(
            "INSERT INTO staging.stg_settlements VALUES ('paysettler', ?, 'M1', ?, ?, ?, ?, ?)",
            [_tid(n), cur, amount, status, status == "REVERSED", at],
        )
    connection.execute(
        "INSERT INTO staging.stg_reconciliation_runs VALUES "
        "(1, '2025-03-16', 'settlement_20250316_02_00.csv', 'COMPLETED', '5', "
        "'2025-03-16T02:00:00Z', '2025-03-16T02:05:30Z'), "
        "(2, '2025-03-13', 'settlement_20250313_02_00.csv', 'FAILED', '9', "
        "'2025-03-13T02:00:00Z', NULL)"
    )
    connection.execute(
        "INSERT INTO staging.stg_reconciliation_results VALUES "
        "(1, 1, ?, 'MATCHED', 'I'), (2, 1, ?, 'MATCHED', 'I'), (3, 1, ?, 'MATCHED', 'D')",
        [_tid(1), _tid(2), _tid(3)],
    )
    connection.execute("INSERT INTO raw.settlements VALUES (?)", [_tid(1)])
    connection.execute("INSERT INTO raw.settlements SELECT ? FROM range(4)", [_tid(2)])
    connection.execute(
        "INSERT INTO staging.stg_merchants_history VALUES "
        "('M1', 'Old', 'T', 'D', 'C', '2024-01-01T00:00:00Z', '2024-06-01T00:00:00Z', false, 'I'), "
        "('M1', 'New', 'T', 'D', 'C', '2024-06-01T00:00:00Z', NULL, true, 'U')"
    )
    yield connection
    connection.close()


def _publish(con):
    build_fct_reconciliation(con, D, "paysettler")
    build_aggregates(con, D)
    return publish_marts(con, D, "paysettler", run_id=uuid.uuid4().hex)


def test_daily_totals_match_the_fact(con):
    _publish(con)
    fact = {
        (cur, cat): (n, amt)
        for cur, cat, n, amt in con.execute(
            "SELECT currency, category, count(*), sum(coalesce(processor_amount, internal_amount)) "
            "FROM marts.fct_reconciliation GROUP BY ALL"
        ).fetchall()
    }
    daily = con.execute(
        "SELECT currency, matched_count, mismatched_count, unreconciled_processor_count, "
        "unreconciled_internal_count, matched_amount, mismatched_amount, "
        "unreconciled_processor_amount, unreconciled_internal_amount, total_count "
        "FROM marts.fct_reconciliation_daily"
    ).fetchall()
    cats = ("MATCHED", "MISMATCHED", "UNRECONCILED_PROCESSOR", "UNRECONCILED_INTERNAL")
    for cur, *vals in daily:
        for i, cat in enumerate(cats):
            n, amt = fact.get((cur, cat), (0, Decimal("0.00")))
            assert (vals[i], vals[4 + i]) == (n, amt), (cur, cat)
        assert vals[8] == sum(vals[:4])
    total_fact = con.execute("SELECT count(*) FROM marts.fct_reconciliation").fetchone()[0]
    assert sum(r[-1] for r in daily) == total_fact


def test_daily_rates_and_service_divergence(con):
    _publish(con)
    rows = {
        r[0]: r[1:]
        for r in con.execute(
            "SELECT currency, mismatch_rate, reversed_count, reversed_rate, "
            "service_compared_count, service_divergence_count, "
            "mismatched_difference_amount "
            "FROM marts.fct_reconciliation_daily"
        ).fetchall()
    }
    # BRL: 1 MATCHED, 1 MISMATCHED(5.00), 1 MATCHED reversed, 1 UNRECONCILED_INTERNAL
    mismatch_rate, reversed_count, reversed_rate, compared, diverged, diff = rows["BRL"]
    assert mismatch_rate == Decimal("0.25")
    assert (reversed_count, reversed_rate) == (1, Decimal("0.333333"))
    assert diff == Decimal("5.00")
    # serviço só tem resultado MATCHED p/ 1 e 2 (3 deletado): 2 comparadas, 1 divergente
    assert (compared, diverged) == (2, 1)


def test_run_mart_declared_vs_file_and_results(con):
    _publish(con)
    rows = con.execute(
        "SELECT run_id, status, duration_seconds, declared_total, file_rows, result_rows, "
        "declared_matches_file, has_results FROM marts.fct_reconciliation_run"
    ).fetchall()
    assert rows == [(1, "COMPLETED", 330, 5, 5, 2, True, True)]


def test_financial_daily_has_only_settlements_by_settled_date(con):
    _publish(con)
    rows = {
        r[0]: r[1:]
        for r in con.execute(
            "SELECT currency, sum(settled_amount), sum(reversed_amount), sum(net_amount), "
            "sum(settled_count), sum(reversed_count) FROM marts.fct_financial_daily "
            "GROUP BY currency"
        ).fetchall()
    }
    assert rows["BRL"] == (Decimal("35.00"), Decimal("5.00"), Decimal("30.00"), 2, 1)
    assert rows["USD"] == (Decimal("15.00"), Decimal("0.00"), Decimal("15.00"), 2, 0)
    # tudo liquidado em D; a interna sem liquidação (tx 5) não entra aqui
    assert con.execute(
        "SELECT DISTINCT settled_date FROM marts.fct_financial_daily"
    ).fetchall() == [(D,)]
    fact = con.execute(
        "SELECT sum(processor_amount) FROM marts.fct_reconciliation "
        "WHERE settlement_status IS NOT NULL"
    ).fetchone()[0]
    assert fact == sum(r[0] + r[1] for r in rows.values())


def test_internal_volume_is_one_row_per_transaction_equal_to_staging(con):
    _publish(con)
    assert con.execute(
        "SELECT count(*), count(DISTINCT transaction_id), sum(amount) "
        "FROM marts.fct_internal_volume_daily"
    ).fetchone() == (5, 5, Decimal("46.00"))
    assert con.execute("SELECT sum(amount) FROM staging.stg_transactions").fetchone()[0] == Decimal(
        "46.00"
    )
    assert con.execute(
        "SELECT created_date FROM marts.fct_internal_volume_daily WHERE transaction_id = ?",
        [_tid(3)],
    ).fetchone() == (date(2025, 3, 14),)


def test_internal_volume_ignores_deleted_and_is_fully_rebuilt(con):
    _publish(con)
    con.execute(
        "UPDATE staging.stg_transactions SET is_deleted = true WHERE transaction_id = ?", [_tid(1)]
    )
    _publish(con)
    assert con.execute(
        "SELECT count(*) FROM marts.fct_internal_volume_daily WHERE transaction_id = ?", [_tid(1)]
    ).fetchone() == (0,)
    assert con.execute("SELECT count(*) FROM marts.fct_internal_volume_daily").fetchone() == (4,)


def test_dim_merchant_keeps_scd2_and_is_replaced_not_duplicated(con):
    _publish(con)
    first = con.execute(
        "SELECT COLUMNS(*)::VARCHAR FROM marts.dim_merchant ORDER BY valid_from"
    ).fetchall()
    assert [r[7] for r in first] == ["false", "true"]
    _publish(con)
    assert (
        con.execute(
            "SELECT COLUMNS(*)::VARCHAR FROM marts.dim_merchant ORDER BY valid_from"
        ).fetchall()
        == first
    )


def test_publish_is_idempotent_for_every_mart(con):
    first_written = _publish(con)
    first = {
        m: con.execute(f"SELECT COLUMNS(*)::VARCHAR FROM marts.{m} ORDER BY ALL").fetchall()
        for m in MARTS
    }
    assert _publish(con) == first_written
    assert {
        m: con.execute(f"SELECT COLUMNS(*)::VARCHAR FROM marts.{m} ORDER BY ALL").fetchall()
        for m in MARTS
    } == first


def test_pipeline_populates_every_mart_on_sample_and_rerun_is_stable(tmp_path):
    wh = tmp_path / "wh.duckdb"

    def run():
        con = connect(wh)
        try:
            run_id = run_pipeline(
                D, source_dir=SAMPLE, settlement_file=SAMPLE_CSV, con=con, processor="paysettler"
            )
            snap = {
                m: con.execute(f"SELECT COLUMNS(*)::VARCHAR FROM marts.{m} ORDER BY ALL").fetchall()
                for m in MARTS
            }
            counts = con.execute(
                "SELECT rows_by_stage FROM ops.pipeline_runs WHERE run_id = ?", [run_id]
            ).fetchone()[0]
            return snap, json.loads(counts)
        finally:
            con.close()

    first, counts = run()
    assert all(first[m] for m in MARTS)
    assert counts["marts.fct_financial_daily"] == len(first["fct_financial_daily"])
    second, _ = run()
    # alerts_count é do run (muda o run_id, não o valor); demais marts idênticos
    assert second == first
