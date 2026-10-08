"""Um teste por check do registry (etapa 12).

Dois estilos:
- alertas dos dados sujos da amostra: o pipeline completo roda uma vez e lê ops.dq_results;
- block/alert sem ocorrência na amostra: monta o pipeline até os agregados (tudo em tabelas
  stage/staging), adultera a tabela relevante e avalia o check isolado.
"""

from datetime import date
from pathlib import Path

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.ingest.cdc import load_raw_cdc
from recon.ingest.settlement import load_raw_settlements
from recon.logging import get_logger
from recon.pipeline import run_pipeline
from recon.quality.checks import BlockingCheckFailed
from recon.quality.registry import CHECKS
from recon.quality.runner import evaluate
from recon.transform.marts import build_aggregates, build_fct_reconciliation
from recon.transform.staging import build_cdc_staging, build_settlement_staging

SAMPLE = Path(__file__).parent.parent / "docs" / "sample-data"
SAMPLE_CSV = SAMPLE / "settlement_paysettler.csv"
D = date(2025, 3, 16)
BY_NAME = {c.name: c for c in CHECKS}


def test_registry_names_are_unique_and_layers_known():
    assert len(BY_NAME) == len(CHECKS)
    assert {c.layer for c in CHECKS} == {"raw", "staging", "marts", "aggregates"}


# --------------------------------------------------------------------------- amostra completa


@pytest.fixture(scope="module")
def sample_results(tmp_path_factory):
    con = connect(tmp_path_factory.mktemp("wh") / "wh.duckdb")
    try:
        run_id = run_pipeline(D, source_dir=SAMPLE, settlement_file=SAMPLE_CSV, con=con)
        rows = con.execute(
            (
                "SELECT check_name, severity, passed, observed_value, threshold "
                "FROM ops.dq_results WHERE run_id = ?"
            ),
            [run_id],
        ).fetchall()
        mismatch_rate = con.execute(
            (
                "SELECT count(*) FILTER (WHERE category = 'MISMATCHED')::DOUBLE "
                "/ count(*) FILTER (WHERE category IN ('MATCHED', 'MISMATCHED')) "
                "FROM marts.fct_reconciliation WHERE reference_date = ?"
            ),
            [D],
        ).fetchone()[0]
    finally:
        con.close()
    return {r[0]: r for r in rows}, mismatch_rate


def test_every_check_runs_once_and_no_block_fails_on_sample(sample_results):
    results, _ = sample_results
    assert set(results) == set(BY_NAME)
    assert [n for n, r in results.items() if r[1] == "block" and not r[2]] == []


# alertas com a contagem esperada nos dados sujos da amostra (ver docs/architecture.md)
SAMPLE_ALERTS = {
    "settlement_identical_duplicates": 10,
    "settlement_amount_br_format": 22,
    "settlement_of_failed_transaction": 20,
    "results_orphan_run": 5,  # run_id=99
    "results_difference_inconsistent": 5,
    "cdc_insert_replay": 1,
    "cdc_timestamp_tie": 4,
    "reversed_without_internal": 1,
    "service_run_total_differs_from_file": 1,  # run 5 declara 98, o CSV tem 510 linhas
    "settlement_settled_at_no_timezone": 25,
}


@pytest.mark.parametrize(("name", "expected"), SAMPLE_ALERTS.items())
def test_sample_alert_fires_with_expected_count(sample_results, name, expected):
    _, severity, passed, observed, _ = sample_results[0][name]
    assert (severity, passed, observed) == ("alert", False, float(expected))


@pytest.mark.parametrize(
    "name",
    [
        "settlement_conflicting_versions",
        "settlement_settled_after_reversed",
        "settlement_merchant_lowercase",
        "transaction_merchant_null",
        "transaction_merchant_not_in_dim",
        "transaction_amount_nonpositive",
        "service_run_missing",
        "service_run_failed_or_empty",
    ],
)
def test_sample_alert_silent_when_data_is_clean(sample_results, name):
    assert sample_results[0][name][2] is True


def test_sample_rate_alerts_record_real_rate(sample_results):
    results, mismatch_rate = sample_results
    _, severity, passed, observed, threshold = results["mismatch_rate_high"]
    assert (severity, passed, threshold) == ("alert", False, 0.05)
    assert observed == pytest.approx(mismatch_rate) and observed > 0.05
    # late settlement: a amostra tem liquidações fora da janela de 7 dias
    assert results["late_settlement"][3] > 0
    assert results["category_differs_from_service"][3] > 0


# --------------------------------------------------------------------------- adulteração


@pytest.fixture
def staged(tmp_path):
    """Pipeline da amostra até os agregados, sem checks nem publish."""
    con = connect(tmp_path / "wh.duckdb")
    apply_ddl(con)
    load_raw_cdc(con, SAMPLE, "r")
    load_raw_settlements(con, SAMPLE_CSV, "r", "paysettler")
    build_cdc_staging(con)
    build_settlement_staging(con)
    build_fct_reconciliation(con, D, "paysettler")
    build_aggregates(con, D)
    yield con
    con.close()


def _result(con, name):
    return evaluate(con, BY_NAME[name], D)


def test_staged_sample_has_no_block_failures(staged):
    failed = [
        c.name for c in CHECKS if c.severity == "block" and not _result(staged, c.name).passed
    ]
    assert failed == []


FIRST_SETTLEMENT = "(SELECT min(transaction_id) FROM staging.stg_settlements)"
FIRST_TXN = "(SELECT min(transaction_id) FROM staging.stg_transactions)"
MISMATCHED_IN_WINDOW = "category = 'MISMATCHED' AND in_window"

TAMPER = [
    # (check, SQL de adulteração, observado esperado)
    ("raw_settlements_missing_columns", "ALTER TABLE raw.settlements DROP COLUMN currency", 1),
    ("raw_transactions_missing_columns", "ALTER TABLE raw.transactions DROP COLUMN Op", 1),
    (
        "raw_reconciliation_runs_missing_columns",
        "ALTER TABLE raw.reconciliation_runs DROP COLUMN total_transactions",
        1,
    ),
    (
        "raw_reconciliation_results_missing_columns",
        "ALTER TABLE raw.reconciliation_results DROP COLUMN difference",
        1,
    ),
    (
        "raw_enterprise_company_missing_columns",
        "ALTER TABLE raw.enterprise_company DROP COLUMN legal_name",
        1,
    ),
    (
        "settlement_source_files_not_single",
        (
            f"UPDATE staging.stg_settlements SET _source_file = 'b.csv' "
            f"WHERE transaction_id = {FIRST_SETTLEMENT}"
        ),
        1,
    ),
    (
        "settlement_transaction_id_invalid",
        (
            f"UPDATE staging.stg_settlements SET transaction_id = 'not-a-uuid' "
            f"WHERE transaction_id = {FIRST_SETTLEMENT}"
        ),
        1,
    ),
    (
        "settlement_transaction_id_invalid",
        (
            f"UPDATE staging.stg_settlements SET transaction_id = NULL "
            f"WHERE transaction_id = {FIRST_SETTLEMENT}"
        ),
        1,
    ),
    (
        "transaction_id_invalid",
        (
            f"UPDATE staging.stg_transactions SET transaction_id = '123' "
            f"WHERE transaction_id = {FIRST_TXN}"
        ),
        1,
    ),
    (
        "settlement_amount_unparseable",
        f"UPDATE staging.stg_settlements SET amount = NULL WHERE transaction_id = {FIRST_SETTLEMENT}",
        1,
    ),
    (
        "settlement_amount_nonpositive",
        f"UPDATE staging.stg_settlements SET amount = 0 WHERE transaction_id = {FIRST_SETTLEMENT}",
        1,
    ),
    # REVERSED também: o valor é sempre positivo, o sinal vem do status
    (
        "settlement_amount_nonpositive",
        "UPDATE staging.stg_settlements SET amount = -5 WHERE status = 'REVERSED'",
        27,
    ),
    (
        "settlement_amount_precision_loss",
        (
            f"UPDATE staging.stg_settlements SET amount_precision_loss = true "
            f"WHERE transaction_id = {FIRST_SETTLEMENT}"
        ),
        1,
    ),
    (
        "settlement_currency_invalid",
        f"UPDATE staging.stg_settlements SET currency = 'XXX' WHERE transaction_id = {FIRST_SETTLEMENT}",
        1,
    ),
    (
        "settlement_status_invalid",
        f"UPDATE staging.stg_settlements SET status = 'PENDING' WHERE transaction_id = {FIRST_SETTLEMENT}",
        1,
    ),
    (
        "settlement_settled_at_unparseable",
        f"UPDATE staging.stg_settlements SET settled_at = NULL WHERE transaction_id = {FIRST_SETTLEMENT}",
        1,
    ),
    (
        "transaction_currency_invalid",
        f"UPDATE staging.stg_transactions SET currency = 'GBP' WHERE transaction_id = {FIRST_TXN}",
        1,
    ),
    (
        "transaction_amount_nonpositive",
        f"UPDATE staging.stg_transactions SET amount = NULL WHERE transaction_id = {FIRST_TXN} AND NOT is_deleted",
        1,
    ),
    (
        "transaction_amount_nonpositive",
        f"UPDATE staging.stg_transactions SET amount = 0 WHERE transaction_id = {FIRST_TXN} AND NOT is_deleted",
        None,
    ),
    (
        "transaction_status_invalid",
        f"UPDATE staging.stg_transactions SET status = 'REFUNDED' WHERE transaction_id = {FIRST_TXN}",
        1,
    ),
    (
        "settlement_conflicting_versions",
        f"UPDATE staging.stg_settlements SET version_count = 2 WHERE transaction_id = {FIRST_SETTLEMENT}",
        1,
    ),
    (
        "settlement_settled_after_reversed",
        "UPDATE staging.stg_settlements SET has_reversal = true WHERE status = 'SETTLED'",
        473,
    ),
    (
        "settlement_merchant_lowercase",
        (
            f"UPDATE staging.stg_settlements SET merchant_was_lowercase = true "
            f"WHERE transaction_id = {FIRST_SETTLEMENT}"
        ),
        1,
    ),
    (
        "transaction_merchant_null",
        (
            f"UPDATE staging.stg_transactions SET merchant_id = NULL "
            f"WHERE transaction_id = {FIRST_TXN} AND NOT is_deleted"
        ),
        None,
    ),
    (
        "transaction_merchant_not_in_dim",
        (
            f"UPDATE staging.stg_transactions SET merchant_id = 'ZZZ' "
            f"WHERE transaction_id = {FIRST_TXN} AND NOT is_deleted"
        ),
        None,
    ),
    # versão distinta no mesmo transaction_id + _timestamp (a amostra já tem 4 empates)
    (
        "cdc_timestamp_tie",
        (
            "INSERT INTO raw.transactions SELECT * REPLACE ('REFUNDED' AS status) FROM raw.transactions "
            "WHERE Op = 'I' LIMIT 1"
        ),
        None,
    ),
    (
        "fct_reconciliation_duplicate_key",
        (
            f"INSERT INTO fct_reconciliation_stage SELECT * FROM fct_reconciliation_stage "
            f"WHERE {MISMATCHED_IN_WINDOW} LIMIT 1"
        ),
        1,
    ),
    (
        "fct_reconciliation_category_null",
        (
            "UPDATE fct_reconciliation_stage SET category = NULL WHERE transaction_id = "
            "(SELECT min(transaction_id) FROM fct_reconciliation_stage)"
        ),
        1,
    ),
    # linha a mais no fato: quebra a conservação dos dois lados (settlement e interna da janela)
    (
        "conservation_settlements",
        (
            f"INSERT INTO fct_reconciliation_stage SELECT * FROM fct_reconciliation_stage "
            f"WHERE {MISMATCHED_IN_WINDOW} LIMIT 1"
        ),
        1,
    ),
    (
        "conservation_internal_in_window",
        (
            f"INSERT INTO fct_reconciliation_stage SELECT * FROM fct_reconciliation_stage "
            f"WHERE {MISMATCHED_IN_WINDOW} LIMIT 1"
        ),
        1,
    ),
    (
        "conservation_internal_in_window",
        f"DELETE FROM fct_reconciliation_stage WHERE {MISMATCHED_IN_WINDOW}",
        None,
    ),
    # diário adulterado: contagem, valor e linha ausente
    (
        "conservation_daily_vs_fact",
        "UPDATE fct_reconciliation_daily_stage SET mismatched_count = mismatched_count + 1",
        1,
    ),
    (
        "conservation_daily_vs_fact",
        "UPDATE fct_reconciliation_daily_stage SET mismatched_amount = mismatched_amount + 0.01",
        1,
    ),
    ("conservation_daily_vs_fact", "DELETE FROM fct_reconciliation_daily_stage", None),
    ("service_run_missing", "DELETE FROM fct_reconciliation_run_stage", 1),
    ("service_run_failed_or_empty", "UPDATE fct_reconciliation_run_stage SET status = 'FAILED'", 1),
    (
        "service_run_failed_or_empty",
        "UPDATE fct_reconciliation_run_stage SET has_results = false",
        1,
    ),
    (
        "service_run_total_differs_from_file",
        "UPDATE fct_reconciliation_run_stage SET declared_matches_file = false",
        1,
    ),
]


def _baseline(con, name):
    return _result(con, name).observed


@pytest.mark.parametrize(
    ("name", "sql", "expected"), TAMPER, ids=[f"{n}-{i}" for i, (n, _, _) in enumerate(TAMPER)]
)
def test_tampered_data_trips_check(staged, name, sql, expected):
    before = _baseline(staged, name)
    staged.execute(sql)
    after = _result(staged, name)
    assert not after.passed, f"{name} should fail after: {sql}"
    if expected is not None:
        assert after.observed == float(expected)
    else:
        assert after.observed > before or before is None


def test_missing_column_names_the_column(staged):
    staged.execute("ALTER TABLE raw.settlements DROP COLUMN currency")
    assert _result(staged, "raw_settlements_missing_columns").sample_keys == ["currency"]


def test_metric_check_records_difference(staged):
    staged.execute(
        "INSERT INTO fct_reconciliation_stage SELECT * FROM fct_reconciliation_stage "
        f"WHERE {MISMATCHED_IN_WINDOW} LIMIT 3"
    )
    result = _result(staged, "conservation_settlements")
    assert (result.check.kind, result.observed, result.sample_keys) == ("metric", 3.0, [])


def test_mismatch_rate_threshold_and_null_rate(staged):
    high = _result(staged, "mismatch_rate_high")
    assert not high.passed and high.observed > 0.05

    staged.execute("UPDATE fct_reconciliation_stage SET category = 'MATCHED'")
    ok = _result(staged, "mismatch_rate_high")
    assert (ok.passed, ok.observed) == (True, 0.0)

    # só UNRECONCILED_*: nenhum par comparado, taxa indefinida (fora do denominador)
    staged.execute("UPDATE fct_reconciliation_stage SET category = 'UNRECONCILED_PROCESSOR'")
    none_compared = _result(staged, "mismatch_rate_high")
    assert (none_compared.passed, none_compared.observed) == (True, None)

    staged.execute("DELETE FROM fct_reconciliation_stage")
    empty = _result(staged, "mismatch_rate_high")
    assert (empty.passed, empty.observed) == (True, None)  # taxa indefinida não alerta


def test_daily_mismatch_rate_uses_compared_pairs_only(staged):
    staged.execute(
        "UPDATE fct_reconciliation_stage SET category = CASE "
        "WHEN category = 'MISMATCHED' THEN 'MISMATCHED' ELSE 'UNRECONCILED_PROCESSOR' END"
    )
    n = staged.execute(
        "SELECT count(*) FROM fct_reconciliation_stage WHERE category = 'MISMATCHED'"
    ).fetchone()[0]
    build_aggregates(staged, D)
    rate = staged.execute("SELECT mismatch_rate FROM fct_reconciliation_daily_stage").fetchall()
    assert n > 0 and {r[0] for r in rate} == {1}  # 100% dos pares comparados

    staged.execute("UPDATE fct_reconciliation_stage SET category = 'UNRECONCILED_PROCESSOR'")
    build_aggregates(staged, D)
    assert staged.execute(
        "SELECT mismatch_rate FROM fct_reconciliation_daily_stage"
    ).fetchall() == [(None,)]


def test_tampered_daily_stage_blocks_before_publish(staged):
    from recon.quality.runner import run_checks

    staged.execute("UPDATE fct_reconciliation_daily_stage SET total_count = total_count + 1")
    with pytest.raises(BlockingCheckFailed) as exc:
        run_checks(staged, "r", D, get_logger("r", str(D)), layer="aggregates")
    assert [r.check.name for r in exc.value.failed] == ["conservation_daily_vs_fact"]


# --------------------------------------------------------------------------- ponta a ponta


def _run_with_csv(tmp_path, mutate):
    lines = SAMPLE_CSV.read_text(encoding="utf-8").splitlines()
    csv = tmp_path / "bad.csv"
    csv.write_text("\n".join(mutate(lines)) + "\n", encoding="utf-8")
    con = connect(tmp_path / "wh.duckdb")
    try:
        with pytest.raises(BlockingCheckFailed) as exc:
            run_pipeline(D, source_dir=SAMPLE, settlement_file=csv, con=con)
        failed = con.execute(
            "SELECT check_name FROM ops.dq_results WHERE NOT passed AND severity = 'block'"
        ).fetchall()
        run = con.execute("SELECT status FROM ops.pipeline_runs").fetchall()
        marts = con.execute("SELECT count(*) FROM marts.fct_reconciliation").fetchone()[0]
    finally:
        con.close()
    return exc.value, [f[0] for f in failed], run, marts


def test_csv_with_ambiguous_thousand_separator_blocks(tmp_path):
    def mutate(lines):
        header, first, *rest = lines
        cols = first.split(",")
        cols[header.split(",").index("amount")] = "1.234"  # milhar ou decimal? ambíguo
        return [header, ",".join(cols), *rest]

    _, failed, run, marts = _run_with_csv(tmp_path, mutate)
    assert failed == ["settlement_amount_precision_loss"]
    assert run == [("FAILED",)] and marts == 0


def test_csv_missing_required_column_blocks_before_staging(tmp_path):
    def mutate(lines):
        idx = lines[0].split(",").index("currency")
        return [",".join(c for i, c in enumerate(line.split(",")) if i != idx) for line in lines]

    _, failed, run, marts = _run_with_csv(tmp_path, mutate)
    assert failed == ["raw_settlements_missing_columns"]
    assert run == [("FAILED",)] and marts == 0
