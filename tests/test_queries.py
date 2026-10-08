"""Etapa 14: cada query de exemplo roda sem erro contra o banco gerado no backfill (etapa 13)."""

import re
from pathlib import Path

import pytest

from recon import config
from recon.db import connect

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"
PLACEHOLDER_TXN = "00000000-0000-0000-0000-000000000000"
FILES = ("ops.sql", "cfo.sql", "compliance.sql")
NAME = re.compile(r"^-- name: (\w+)\s*$", re.MULTILINE)


def _load(file: str) -> list[tuple[str, str]]:
    text = (QUERIES_DIR / file).read_text(encoding="utf-8")
    parts = NAME.split(text)
    return list(zip(parts[1::2], (p.strip() for p in parts[2::2]), strict=True))


ALL = [(n, sql.rstrip(";")) for f in FILES for n, sql in _load(f)]


def _count(con, sql: str) -> int:
    # contar no DuckDB evita materializar TIMESTAMPTZ em Python (exigiria pytz)
    return con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]


@pytest.fixture(scope="module")
def wh(backfilled_warehouse):
    con = connect(backfilled_warehouse / config.WAREHOUSE_FILE)
    yield con
    con.close()


@pytest.mark.parametrize("file", FILES)
def test_every_file_has_queries(file):
    assert len(_load(file)) >= 5


@pytest.mark.parametrize(("name", "sql"), ALL, ids=[a[0] for a in ALL])
def test_query_runs(wh, name, sql):
    _count(wh, sql)


@pytest.mark.parametrize(
    "name",
    [
        "ops_daily_health",
        "cfo_net_settled_by_day",
        "cfo_internal_volume_by_created_date",
        "compliance_snapshots_per_run",
        "cfo_amount_at_risk_by_date",
    ],
)
def test_core_queries_return_rows(wh, name):
    sql = next(s for n, s in ALL if n == name)
    assert _count(wh, sql) > 0


def _query(name: str) -> str:
    return next(s for n, s in ALL if n == name)


def test_transaction_history_filters_by_param(wh):
    txn = wh.execute(
        "SELECT transaction_id FROM marts.audit_reconciliation_history "
        "GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1"
    ).fetchone()[0]
    sql = _query("compliance_transaction_history").replace(PLACEHOLDER_TXN, txn)
    assert PLACEHOLDER_TXN not in sql
    assert _count(wh, sql) > 0
    assert _count(wh, _query("compliance_transaction_history")) == 0


def test_as_of_returns_latest_snapshot_before_instant(wh):
    # um instante antes do primeiro snapshot não vê nada; depois do último vê o estado atual
    ref = wh.execute(
        "SELECT max(reference_date) FROM marts.audit_reconciliation_history"
    ).fetchone()[0]
    base = _query("compliance_as_of").replace("DATE '2025-03-16'", f"DATE '{ref}'")
    before = base.replace("current_timestamp", "TIMESTAMPTZ '2000-01-01 00:00:00+00'")
    after = base.replace("current_timestamp", "TIMESTAMPTZ '2100-01-01 00:00:00+00'")
    assert _count(wh, before) == 0
    # padrão (current_timestamp) enxerga todos os snapshots já publicados
    assert _count(wh, base) == _count(wh, after)
    n_after = _count(wh, after)
    assert (
        n_after
        == wh.execute(
            "SELECT count(*) FROM marts.fct_reconciliation WHERE reference_date = ?", [ref]
        ).fetchone()[0]
    )
    # no máximo uma linha por transação
    assert (
        n_after
        == wh.execute(
            f"SELECT count(DISTINCT (processor, transaction_id)) FROM ({after})"
        ).fetchone()[0]
    )
