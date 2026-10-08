"""Etapa 13: backfill ponta a ponta sobre dados do gerador (coerentes, ao contrário da amostra)."""

import shutil
from collections import Counter

import pytest
from conftest import csv_days, days_without_csv, run_backfill

from recon import config
from recon.db import connect

# Tabelas com escopo por reference_date ou reconstruídas; a auditoria é só acrescentada.
SNAPSHOT_TABLES = [
    "fct_reconciliation",
    "fct_reconciliation_daily",
    "fct_reconciliation_run",
    "fct_financial_daily",
    "fct_internal_volume_daily",
    "dim_merchant",
]

# Mistura do gerador: 85/5/5/5. Tolerância absoluta em pontos de proporção; orphan REVERSED
# (+0,3%) e a sobreposição das janelas entram na folga.
EXPECTED_MIX = {
    "MATCHED": 0.85,
    "MISMATCHED": 0.05,
    "UNRECONCILED_PROCESSOR": 0.05,
    "UNRECONCILED_INTERNAL": 0.05,
}
MIX_TOLERANCE = 0.02


@pytest.fixture
def wh(backfilled_warehouse):
    con = connect(backfilled_warehouse / config.WAREHOUSE_FILE)
    yield con
    con.close()


def _snapshot(data_path):
    con = connect(data_path / config.WAREHOUSE_FILE)
    try:
        return {
            # hash do conteúdo: evita materializar TIMESTAMPTZ em Python (exigiria pytz)
            t: con.execute(
                f"SELECT count(*), md5(string_agg(CAST(r AS VARCHAR), '|' ORDER BY CAST(r AS VARCHAR))) "
                f"FROM marts.{t} r"
            ).fetchone()
            for t in SNAPSHOT_TABLES
        }
    finally:
        con.close()


def test_dates_with_csv_succeed_and_missing_csv_days_are_blocked(wh, generated_source):
    missing = set(days_without_csv(generated_source))
    runs = wh.execute("SELECT reference_date, status FROM ops.pipeline_runs").fetchall()
    assert {d for d, s in runs if s == "SUCCESS"} == set(csv_days(generated_source))
    assert {d for d, s in runs if s == "FAILED"} == missing
    # único block reprovado é o arquivo ausente, e só nos dias sem CSV
    blocks = wh.execute(
        "SELECT reference_date, check_name FROM ops.dq_results "
        "WHERE severity = 'block' AND NOT passed"
    ).fetchall()
    assert {c for _, c in blocks} <= {"settlement_file_missing"}
    assert {d for d, _ in blocks} == missing
    # run FAILED => marts intocados: nenhuma linha para essas datas
    published = {
        r[0]
        for r in wh.execute(
            "SELECT DISTINCT reference_date FROM marts.fct_reconciliation"
        ).fetchall()
    }
    assert not published & missing


def test_category_mix_is_close_to_generator_mix(wh):
    # Uma transação aparece em várias datas (interna na janela de D-7..D sem CSV em D vira
    # UNRECONCILED_INTERNAL até a liquidação chegar). A categoria final é a da linha com lado
    # processador; só UNRECONCILED_INTERNAL restante é de fato sem liquidação.
    rows = wh.execute("SELECT transaction_id, category FROM marts.fct_reconciliation").fetchall()
    final: dict[str, str] = {}
    for txn, category in rows:
        if category != "UNRECONCILED_INTERNAL" or txn not in final:
            final[txn] = category
    mix = Counter(final.values())
    total = sum(mix.values())
    assert total >= 10_000  # 10k transações + REVERSED órfãos
    for category, expected in EXPECTED_MIX.items():
        assert abs(mix[category] / total - expected) <= MIX_TOLERANCE, (category, mix)


def test_rerun_backfill_changes_nothing_but_appends_audit(
    backfilled_warehouse, generated_source, tmp_path
):
    before = _snapshot(backfilled_warehouse)
    audit_before = connect(backfilled_warehouse / config.WAREHOUSE_FILE)
    try:
        audit_count = audit_before.execute(
            "SELECT count(*) FROM marts.audit_reconciliation_history"
        ).fetchone()[0]
    finally:
        audit_before.close()

    copy = tmp_path / "copy"
    copy.mkdir()
    shutil.copy(backfilled_warehouse / config.WAREHOUSE_FILE, copy / config.WAREHOUSE_FILE)
    assert run_backfill(generated_source, copy) == (1 if days_without_csv(generated_source) else 0)

    assert _snapshot(copy) == before
    con = connect(copy / config.WAREHOUSE_FILE)
    try:
        assert (
            con.execute("SELECT count(*) FROM marts.audit_reconciliation_history").fetchone()[0]
            == 2 * audit_count
        )
    finally:
        con.close()
