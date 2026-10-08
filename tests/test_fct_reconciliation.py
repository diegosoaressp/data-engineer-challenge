from datetime import date
from decimal import Decimal

import duckdb
import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.transform.marts import build_fct_reconciliation

D = date(2025, 3, 16)


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
        CREATE TABLE staging.stg_reconciliation_runs (id BIGINT, reference_date DATE);
        CREATE TABLE staging.stg_reconciliation_results (
            id BIGINT, run_id BIGINT, transaction_id VARCHAR, category VARCHAR, last_op VARCHAR);
        """
    )
    yield connection
    connection.close()


def _internal(
    con, n, amount="10.00", created="2025-03-15T10:00:00Z", deleted=False, status="COMPLETED"
):
    con.execute(
        "INSERT INTO staging.stg_transactions VALUES (?, 'MERCH_001', 'BRL', ?, ?, ?, ?)",
        [_tid(n), amount, status, created, deleted],
    )


def _settle(con, n, amount="10.00", settled="2025-03-16T10:00:00Z", status="SETTLED"):
    con.execute(
        "INSERT INTO staging.stg_settlements VALUES "
        "('paysettler', ?, 'MERCH_001', 'BRL', ?, ?, ?, ?)",
        [_tid(n), amount, status, status == "REVERSED", settled],
    )


def _categories(con):
    build_fct_reconciliation(con, D, "paysettler")
    return dict(
        con.execute("SELECT transaction_id, category FROM fct_reconciliation_stage").fetchall()
    )


@pytest.mark.parametrize(
    ("processor_amount", "expected"),
    [("10.00", "MATCHED"), ("10.01", "MATCHED"), ("9.99", "MATCHED"),
     ("10.02", "MISMATCHED"), ("9.98", "MISMATCHED")],
)  # fmt: skip
def test_tolerance_boundary(con, processor_amount, expected):
    _internal(con, 1)
    _settle(con, 1, amount=processor_amount)
    cats = _categories(con)
    assert cats == {_tid(1): expected}
    diff = con.execute("SELECT difference FROM fct_reconciliation_stage").fetchone()[0]
    assert diff == abs(con.execute(f"SELECT 10.00 - {processor_amount}").fetchone()[0])


def test_window_edges(con):
    _internal(con, 1, created="2025-03-09T00:00:00Z")  # D-7 início do dia: dentro
    _internal(con, 2, created="2025-03-16T23:59:59Z")  # D fim do dia: dentro
    _internal(con, 3, created="2025-03-08T23:59:59Z")  # D-8: fora
    _internal(con, 4, created="2025-03-17T00:00:00Z")  # D+1: fora
    assert _categories(con) == {
        _tid(1): "UNRECONCILED_INTERNAL",
        _tid(2): "UNRECONCILED_INTERNAL",
    }
    flags = con.execute("SELECT DISTINCT in_window FROM fct_reconciliation_stage").fetchall()
    assert flags == [(True,)]


def test_late_settlement_of_internal_outside_window_is_matched(con):
    _internal(con, 1, created="2025-03-06T10:00:00Z")  # D-10
    _settle(con, 1)
    assert _categories(con) == {_tid(1): "MATCHED"}
    row = con.execute(
        "SELECT in_window, is_late_settlement, internal_amount FROM fct_reconciliation_stage"
    ).fetchone()
    assert row == (False, True, Decimal("10.00"))


def test_internal_outside_window_without_pair_is_out_of_scope(con):
    _internal(con, 1, created="2025-03-06T10:00:00Z")  # D-10, sem par
    _internal(con, 2)
    assert _categories(con) == {_tid(2): "UNRECONCILED_INTERNAL"}


def test_processor_only_when_internal_missing_or_deleted(con):
    _settle(con, 1)
    _internal(con, 2, deleted=True, created="2025-03-06T10:00:00Z")
    _settle(con, 2)
    assert _categories(con) == {
        _tid(1): "UNRECONCILED_PROCESSOR",
        _tid(2): "UNRECONCILED_PROCESSOR",
    }


def test_orphan_reversed_is_unreconciled_processor(con):
    _settle(con, 1, status="REVERSED")
    assert _categories(con) == {_tid(1): "UNRECONCILED_PROCESSOR"}
    row = con.execute(
        "SELECT settlement_status, has_reversal, internal_status, internal_amount "
        "FROM fct_reconciliation_stage"
    ).fetchone()
    assert row == ("REVERSED", True, None, None)


def test_deleted_internal_is_ignored(con):
    _internal(con, 1, deleted=True)
    _internal(con, 2)
    _settle(con, 1)
    assert _categories(con) == {
        _tid(1): "UNRECONCILED_PROCESSOR",
        _tid(2): "UNRECONCILED_INTERNAL",
    }


def test_internal_only_gets_processor_and_dates(con):
    _internal(con, 1, created="2025-03-15T10:00:00Z")
    build_fct_reconciliation(con, D, "paysettler")
    row = con.execute(
        "SELECT reference_date, processor, created_date, settled_date, is_late_settlement "
        "FROM fct_reconciliation_stage"
    ).fetchone()
    assert row == (D, "paysettler", date(2025, 3, 15), None, False)


def test_late_settlement_is_by_date(con):
    _internal(con, 1, created="2025-03-09T23:00:00Z")
    _internal(con, 2, created="2025-03-09T01:00:00Z")
    _internal(con, 3, created="2025-03-09T01:00:00Z")
    _settle(con, 1, settled="2025-03-16T01:00:00Z")  # 7 dias de calendário: não é tardia
    _settle(con, 2, settled="2025-03-16T23:59:00Z")  # 7 dias, mesmo com 7d22h: não
    _settle(con, 3, settled="2025-03-17T00:30:00Z")  # 8 dias: tardia
    build_fct_reconciliation(con, D, "paysettler")
    rows = dict(
        con.execute(
            "SELECT transaction_id, is_late_settlement FROM fct_reconciliation_stage"
        ).fetchall()
    )
    assert rows == {_tid(1): False, _tid(2): False, _tid(3): True}


def test_late_flag_false_when_not_applicable(con):
    _internal(con, 1)
    _settle(con, 2)
    build_fct_reconciliation(con, D, "paysettler")
    flags = con.execute("SELECT is_late_settlement FROM fct_reconciliation_stage").fetchall()
    assert flags == [(False,), (False,)]


def test_currency_mismatch(con):
    _internal(con, 1)
    con.execute("UPDATE staging.stg_transactions SET currency = 'USD'")
    _settle(con, 1, amount="10.00")  # mesmo valor, moeda BRL no CSV
    assert _categories(con) == {_tid(1): "MISMATCHED"}
    row = con.execute(
        "SELECT mismatch_reason, difference, signed_difference, currency, internal_currency "
        "FROM fct_reconciliation_stage"
    ).fetchone()
    assert row == ("CURRENCY", None, None, "BRL", "USD")


def test_amount_mismatch_reason_and_signed_difference(con):
    _internal(con, 1, amount="10.00")
    _internal(con, 2, amount="10.00")
    _internal(con, 3, amount="10.00")
    _settle(con, 1, amount="12.50")
    _settle(con, 2, amount="7.50")
    _settle(con, 3, amount="10.01")
    build_fct_reconciliation(con, D, "paysettler")
    rows = {
        r[0]: r[1:]
        for r in con.execute(
            "SELECT transaction_id, category, mismatch_reason, difference, signed_difference "
            "FROM fct_reconciliation_stage"
        ).fetchall()
    }
    assert rows[_tid(1)] == ("MISMATCHED", "AMOUNT", Decimal("2.50"), Decimal("2.50"))
    assert rows[_tid(2)] == ("MISMATCHED", "AMOUNT", Decimal("2.50"), Decimal("-2.50"))
    assert rows[_tid(3)] == ("MATCHED", None, Decimal("0.01"), Decimal("0.01"))


@pytest.mark.parametrize("null_side", ["processor", "internal"])
def test_null_amount_never_becomes_mismatched(con, null_side):
    _internal(con, 1)
    _settle(con, 1)
    table = "stg_settlements" if null_side == "processor" else "stg_transactions"
    con.execute(f"UPDATE staging.{table} SET amount = NULL")
    build_fct_reconciliation(con, D, "paysettler")
    row = con.execute(
        "SELECT category, mismatch_reason, difference FROM fct_reconciliation_stage"
    ).fetchone()
    assert row == (None, None, None)
    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO marts.fct_reconciliation SELECT * FROM fct_reconciliation_stage")


def test_transaction_id_is_normalized_on_both_sides(con):
    con.execute(
        "INSERT INTO staging.stg_transactions VALUES "
        "(?, 'MERCH_001', 'BRL', 10.00, 'COMPLETED', TIMESTAMPTZ '2025-03-15 10:00:00+00', false)",
        [f" {_tid(1).upper()} "],
    )
    _settle(con, 1)
    assert _categories(con) == {_tid(1): "MATCHED"}


def test_source_category_from_service_run_of_the_date(con):
    _internal(con, 1)
    _settle(con, 1)
    con.execute(
        "INSERT INTO staging.stg_reconciliation_runs VALUES (1, '2025-03-16'), (2, '2025-03-13')"
    )
    con.execute(
        "INSERT INTO staging.stg_reconciliation_results VALUES "
        "(1, 1, ?, 'MISMATCHED', 'I'), (2, 2, ?, 'MATCHED', 'I')",
        [_tid(1), _tid(1)],
    )
    build_fct_reconciliation(con, D, "paysettler")
    assert con.execute("SELECT source_category FROM fct_reconciliation_stage").fetchone()[0] == (
        "MISMATCHED"
    )


def test_rebuild_is_idempotent_and_key_unique(con):
    _internal(con, 1)
    _settle(con, 1)
    n1 = build_fct_reconciliation(con, D, "paysettler")
    n2 = build_fct_reconciliation(con, D, "paysettler")
    assert n1 == n2 == 1


def test_stage_fits_mart_ddl(con):
    _internal(con, 1)
    _settle(con, 1, amount="11.00")
    build_fct_reconciliation(con, D, "paysettler")
    con.execute("INSERT INTO marts.fct_reconciliation SELECT * FROM fct_reconciliation_stage")
    assert (
        con.execute("SELECT category FROM marts.fct_reconciliation").fetchone()[0] == "MISMATCHED"
    )
