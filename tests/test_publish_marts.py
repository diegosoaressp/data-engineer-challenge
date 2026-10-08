from datetime import date

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.transform import marts
from recon.transform.marts import (
    PublishTarget,
    build_aggregates,
    build_fct_reconciliation,
    publish_marts,
)

D = date(2025, 3, 16)
OTHER = date(2025, 3, 15)


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
        CREATE TABLE raw.settlements (transaction_id VARCHAR);
        CREATE TABLE staging.stg_merchants_history (
            merchant_id VARCHAR, legal_name VARCHAR, trade_name VARCHAR, document VARCHAR,
            primary_cnae VARCHAR, valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ,
            is_current BOOLEAN, op VARCHAR);
        INSERT INTO staging.stg_merchants_history VALUES
            ('M1', 'L', 'T', 'D', 'C', '2024-01-01T00:00:00Z', NULL, true, 'I');
        CREATE TABLE staging.stg_reconciliation_results (
            id BIGINT, run_id BIGINT, transaction_id VARCHAR, category VARCHAR, last_op VARCHAR);
        """
    )
    for n, amount in ((1, "10.00"), (2, "20.00")):
        connection.execute(
            "INSERT INTO staging.stg_transactions VALUES (?, 'M1', 'BRL', ?, 'COMPLETED', "
            "'2025-03-15T10:00:00Z', false)",
            [_tid(n), amount],
        )
        connection.execute(
            "INSERT INTO staging.stg_settlements VALUES "
            "('paysettler', ?, 'M1', 'BRL', ?, 'SETTLED', false, '2025-03-16T10:00:00Z')",
            [_tid(n), amount],
        )
    yield connection
    connection.close()


def _snapshot(con):
    return con.execute(
        "SELECT * FROM marts.fct_reconciliation ORDER BY reference_date, processor, transaction_id"
    ).fetchall()


def _build(con, ref=D, processor="paysettler"):
    build_fct_reconciliation(con, ref, processor)
    build_aggregates(con, ref)


def _run(con, ref=D):
    _build(con, ref)
    return publish_marts(con, ref, "paysettler")


def test_rerun_is_idempotent(con):
    assert _run(con)["marts.fct_reconciliation"] == 2
    first = _snapshot(con)
    assert _run(con)["marts.fct_reconciliation"] == 2
    assert _snapshot(con) == first


def test_rerun_replaces_changed_rows_and_keeps_other_dates(con):
    _run(con, OTHER)
    other = [r for r in _snapshot(con) if r[0] == OTHER]
    _run(con)
    con.execute(
        "UPDATE staging.stg_settlements SET amount = 99.00 WHERE transaction_id = ?", [_tid(1)]
    )
    _run(con)
    rows = _snapshot(con)
    assert [r for r in rows if r[0] == OTHER] == other
    today = {r[2]: r[10] for r in rows if r[0] == D}
    assert today == {_tid(1): "MISMATCHED", _tid(2): "MATCHED"}


def test_two_processors_same_date_do_not_overwrite_each_other(con):
    con.execute(
        "INSERT INTO staging.stg_settlements SELECT 'other', transaction_id, merchant_id, "
        "currency, amount + 5, status, has_reversal, settled_at FROM staging.stg_settlements"
    )
    _run(con)
    _build(con, D, "other")
    publish_marts(con, D, "other")
    rows = _snapshot(con)
    assert {(r[1], r[10]) for r in rows} == {("paysettler", "MATCHED"), ("other", "MISMATCHED")}
    assert len(rows) == 4
    # reprocessar um processor não toca nem duplica o outro
    _run(con)
    assert _snapshot(con) == rows


def test_injected_exception_rolls_back_every_mart(con, monkeypatch):
    _run(con)
    before = _snapshot(con)
    con.execute("UPDATE staging.stg_settlements SET amount = 99.00")
    _build(con, D, "paysettler")

    monkeypatch.setattr(
        marts,
        "PUBLISH_TARGETS",
        (
            *marts.PUBLISH_TARGETS,
            PublishTarget("marts.missing_table", marts.FCT_RECONCILIATION_STAGE, ()),
        ),
    )
    with pytest.raises(Exception, match="missing_table"):
        publish_marts(con, D, "paysettler")
    assert _snapshot(con) == before


def test_constraint_violation_mid_publish_rolls_back(con):
    _run(con)
    before = _snapshot(con)
    _build(con, D, "paysettler")
    con.execute(f"UPDATE {marts.FCT_RECONCILIATION_STAGE} SET category = NULL")
    with pytest.raises(Exception, match="NOT NULL"):
        publish_marts(con, D, "paysettler")
    assert _snapshot(con) == before
    # a conexão segue utilizável após o rollback
    assert _run(con)["marts.fct_reconciliation"] == 2


def test_empty_stage_clears_old_rows_of_the_scope(con):
    _run(con)
    _build(con, D, "other")  # outro processor, também publicado
    publish_marts(con, D, "other")
    assert len(_snapshot(con)) == 4  # 2 de cada processor
    con.execute("DELETE FROM staging.stg_settlements")
    con.execute("DELETE FROM staging.stg_transactions")
    _build(con, D, "paysettler")
    assert con.execute(f"SELECT count(*) FROM {marts.FCT_RECONCILIATION_STAGE}").fetchone()[0] == 0
    assert publish_marts(con, D, "paysettler")["marts.fct_reconciliation"] == 0
    remaining = {r[1] for r in _snapshot(con)}
    assert remaining == {"other"}  # só (D, paysettler) foi limpo
