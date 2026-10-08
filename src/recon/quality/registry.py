from recon import config
from recon.quality.checks import Check

UUID_RE = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def _in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


CURRENCY_LIST = _in_list(config.CURRENCIES)
SETTLEMENT_STATUS_LIST = _in_list(config.SETTLEMENT_STATUSES)
TRANSACTION_STATUS_LIST = _in_list(config.TRANSACTION_STATUSES)

# Colunas que o staging lê de cada tabela raw. `payment_method` (schema drift) é opcional.
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "settlements": (
        "transaction_id", "merchant_id", "amount", "currency", "settled_at",
        "processor_reference", "status",
    ),
    "transactions": (
        "id", "transaction_id", "merchant_id", "amount", "currency", "status", "description",
        "created_at", "updated_at", "Op", "_timestamp",
    ),
    "reconciliation_runs": (
        "id", "reference_date", "file_name", "status", "total_transactions", "started_at",
        "completed_at", "created_at", "Op", "_timestamp",
    ),
    "reconciliation_results": (
        "id", "run_id", "transaction_id", "merchant_id", "category", "internal_amount",
        "processor_amount", "difference", "created_at", "Op", "_timestamp",
    ),
    "enterprise_company": (
        "id", "merchant_id", "legal_name", "trade_name", "document", "primary_cnae", "Op",
        "_timestamp",
    ),
}  # fmt: skip

CATEGORIES = ("MATCHED", "MISMATCHED", "UNRECONCILED_PROCESSOR", "UNRECONCILED_INTERNAL")


def _required_columns_check(table: str, columns: tuple[str, ...]) -> Check:
    values = ", ".join(f"('{c}')" for c in columns)
    return Check(
        name=f"raw_{table}_missing_columns",
        layer="raw",
        severity="block",
        sql=f"""
            SELECT req.col FROM (VALUES {values}) AS req(col)
            WHERE lower(req.col) NOT IN (
                SELECT lower(column_name) FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = '{table}'
            )
        """,
    )


def _not_uuid(table: str) -> str:
    return f"""
        SELECT transaction_id FROM staging.{table}
        WHERE coalesce(NOT regexp_matches(transaction_id, '{UUID_RE}'), true)
    """


def _fact_by_currency() -> str:
    """Counts and amounts per category and currency, recomputed from the fact stage."""
    parts = []
    for cat in CATEGORIES:
        name = cat.lower()
        parts.append(f"count(*) FILTER (WHERE category = '{cat}') AS {name}_count")
        parts.append(
            "coalesce(sum(coalesce(processor_amount, internal_amount)) "
            f"FILTER (WHERE category = '{cat}'), 0) AS {name}_amount"
        )
    return (
        "SELECT coalesce(currency, 'UNKNOWN') AS currency, count(*) AS total_count, "
        + ", ".join(parts)
        + " FROM fct_reconciliation_stage GROUP BY 1"
    )


_COMPARED = ["total_count"] + [
    f"{c.lower()}_{kind}" for c in CATEGORIES for kind in ("count", "amount")
]
_DAILY_DIFFERS = " OR ".join(f"f.{c} <> d.{c}" for c in _COMPARED)

# Ordem de execução no pipeline: raw -> staging -> marts (stage do fato) -> aggregates.
# Cada camada roda toda (block e alert) antes do publish; um block reprovado aborta o run.
CHECKS: tuple[Check, ...] = (
    # ------------------------------------------------------------------ raw / block
    *(_required_columns_check(t, c) for t, c in REQUIRED_COLUMNS.items()),
    # ------------------------------------------------------------------ staging / block
    Check(
        name="settlement_source_files_not_single",
        layer="staging",
        severity="block",
        # fct_reconciliation não filtra o arquivo de D: depende de raw ter só o CSV de D
        sql="""
            SELECT count(DISTINCT _source_file) AS source_files FROM staging.stg_settlements
            HAVING count(DISTINCT _source_file) <> 1
        """,
    ),
    Check(
        name="settlement_transaction_id_invalid",
        layer="staging",
        severity="block",
        sql=_not_uuid("stg_settlements"),
    ),
    Check(
        name="transaction_id_invalid",
        layer="staging",
        severity="block",
        sql=_not_uuid("stg_transactions"),
    ),
    Check(
        name="settlement_amount_unparseable",
        layer="staging",
        severity="block",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE amount IS NULL",
    ),
    Check(
        name="settlement_amount_nonpositive",
        layer="staging",
        severity="block",
        # amount é sempre positivo; o sinal vem do status (REVERSED -> signed_amount negativo)
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE amount <= 0",
    ),
    Check(
        name="settlement_amount_precision_loss",
        layer="staging",
        severity="block",
        # mais de 2 casas decimais seria arredondado; "1.234" é ambíguo entre milhar e decimal
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE amount_precision_loss",
    ),
    Check(
        name="settlement_currency_invalid",
        layer="staging",
        severity="block",
        sql=f"""
            SELECT transaction_id FROM staging.stg_settlements
            WHERE coalesce(currency NOT IN ({CURRENCY_LIST}), true)
        """,
    ),
    Check(
        name="settlement_status_invalid",
        layer="staging",
        severity="block",
        sql=f"""
            SELECT transaction_id FROM staging.stg_settlements
            WHERE coalesce(status NOT IN ({SETTLEMENT_STATUS_LIST}), true)
        """,
    ),
    Check(
        name="settlement_settled_at_unparseable",
        layer="staging",
        severity="block",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE settled_at IS NULL",
    ),
    Check(
        name="transaction_currency_invalid",
        layer="staging",
        severity="block",
        sql=f"""
            SELECT transaction_id FROM staging.stg_transactions
            WHERE NOT is_deleted AND coalesce(upper(trim(currency)) NOT IN ({CURRENCY_LIST}), true)
        """,
    ),
    Check(
        name="transaction_status_invalid",
        layer="staging",
        severity="block",
        sql=f"""
            SELECT transaction_id FROM staging.stg_transactions
            WHERE NOT is_deleted
              AND coalesce(upper(trim(status)) NOT IN ({TRANSACTION_STATUS_LIST}), true)
        """,
    ),
    # ------------------------------------------------------------------ staging / alert
    Check(
        name="transaction_amount_nonpositive",
        layer="staging",
        severity="alert",
        # NULL também alerta: sem isso a interna sem par entraria em silêncio
        sql="""
            SELECT transaction_id FROM staging.stg_transactions
            WHERE NOT is_deleted AND (amount IS NULL OR amount <= 0)
        """,
    ),
    Check(
        name="settlement_identical_duplicates",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE dup_count > 1",
    ),
    Check(
        name="settlement_conflicting_versions",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE version_count > 1",
    ),
    Check(
        name="settlement_settled_after_reversed",
        layer="staging",
        severity="alert",
        # a última versão (por settled_at) é SETTLED, mas existe uma REVERSED anterior
        sql="""
            SELECT transaction_id FROM staging.stg_settlements
            WHERE status = 'SETTLED' AND has_reversal
        """,
    ),
    Check(
        name="settlement_amount_br_format",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE amount_was_br_format",
    ),
    Check(
        name="settlement_settled_at_no_timezone",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE settled_at_no_tz",
    ),
    Check(
        name="settlement_merchant_lowercase",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE merchant_was_lowercase",
    ),
    Check(
        name="transaction_merchant_null",
        layer="staging",
        severity="alert",
        sql="""
            SELECT transaction_id FROM staging.stg_transactions
            WHERE NOT is_deleted AND merchant_id IS NULL
        """,
    ),
    Check(
        name="transaction_merchant_not_in_dim",
        layer="staging",
        severity="alert",
        sql="""
            SELECT t.transaction_id FROM staging.stg_transactions t
            WHERE NOT t.is_deleted AND t.merchant_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM staging.stg_merchants_history m WHERE m.merchant_id = t.merchant_id
              )
        """,
    ),
    Check(
        name="cdc_insert_replay",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_transactions WHERE insert_count > 1",
    ),
    Check(
        name="cdc_timestamp_tie",
        layer="staging",
        severity="alert",
        # versões distintas com o mesmo _timestamp; desempate D > U > I em stg_transactions
        sql="""
            SELECT transaction_id FROM raw.transactions
            GROUP BY transaction_id, _timestamp
            HAVING count(DISTINCT (id, Op, merchant_id, amount, currency, status, description,
                                   created_at, updated_at)) > 1
        """,
    ),
    Check(
        name="results_orphan_run",
        layer="staging",
        severity="alert",
        sql="SELECT id FROM staging.stg_reconciliation_results WHERE has_orphan_run",
    ),
    Check(
        name="results_difference_inconsistent",
        layer="staging",
        severity="alert",
        sql="""
            SELECT id FROM staging.stg_reconciliation_results
            WHERE last_op <> 'D' AND internal_amount IS NOT NULL AND processor_amount IS NOT NULL
              AND difference IS DISTINCT FROM abs(internal_amount - processor_amount)
        """,
    ),
    # ------------------------------------------------------------------ fato (stage) / block
    Check(
        name="fct_reconciliation_duplicate_key",
        layer="marts",
        severity="block",
        sql="""
            SELECT transaction_id FROM fct_reconciliation_stage
            GROUP BY reference_date, processor, transaction_id HAVING count(*) > 1
        """,
    ),
    Check(
        name="fct_reconciliation_category_null",
        layer="marts",
        severity="block",
        sql="SELECT transaction_id FROM fct_reconciliation_stage WHERE category IS NULL",
    ),
    Check(
        name="conservation_settlements",
        layer="marts",
        severity="block",
        kind="metric",
        # settlements deduplicados = MATCHED + MISMATCHED + UNRECONCILED_PROCESSOR
        sql="""
            SELECT abs(
                (SELECT count(*) FROM staging.stg_settlements)
                - (SELECT count(*) FROM fct_reconciliation_stage
                   WHERE category IN ('MATCHED', 'MISMATCHED', 'UNRECONCILED_PROCESSOR'))
            )
        """,
    ),
    Check(
        name="conservation_internal_in_window",
        layer="marts",
        severity="block",
        kind="metric",
        # internas não deletadas da janela = MATCHED + MISMATCHED + UNRECONCILED_INTERNAL
        # (as de fora da janela só entram no fato se casaram com o CSV)
        sql="""
            SELECT abs(
                (SELECT count(*) FROM staging.stg_transactions
                 WHERE NOT is_deleted
                   AND CAST(created_at AT TIME ZONE 'UTC' AS DATE)
                       BETWEEN DATE '{reference_date}' - {window_days} AND DATE '{reference_date}')
                - (SELECT count(*) FROM fct_reconciliation_stage
                   WHERE in_window
                     AND category IN ('MATCHED', 'MISMATCHED', 'UNRECONCILED_INTERNAL'))
            )
        """,
    ),
    # ------------------------------------------------------------------ fato (stage) / alert
    Check(
        name="settlement_of_failed_transaction",
        layer="marts",
        severity="alert",
        sql="""
            SELECT transaction_id FROM fct_reconciliation_stage
            WHERE settlement_status IS NOT NULL AND internal_status = 'FAILED'
        """,
    ),
    Check(
        name="reversed_without_internal",
        layer="marts",
        severity="alert",
        sql="""
            SELECT transaction_id FROM fct_reconciliation_stage
            WHERE settlement_status = 'REVERSED' AND internal_amount IS NULL
        """,
    ),
    Check(
        name="late_settlement",
        layer="marts",
        severity="alert",
        sql="SELECT transaction_id FROM fct_reconciliation_stage WHERE is_late_settlement",
    ),
    Check(
        name="category_differs_from_service",
        layer="marts",
        severity="alert",
        sql="""
            SELECT transaction_id FROM fct_reconciliation_stage
            WHERE source_category IS NOT NULL AND source_category <> category
        """,
    ),
    Check(
        name="mismatch_rate_high",
        layer="marts",
        severity="alert",
        kind="metric",
        threshold=config.MISMATCH_RATE_ALERT,
        sql="""
            SELECT CAST(count(*) FILTER (WHERE category = 'MISMATCHED') AS DOUBLE)
                   / nullif(count(*) FILTER (WHERE category IN ('MATCHED', 'MISMATCHED')), 0)
            FROM fct_reconciliation_stage
        """,
    ),
    # ------------------------------------------------------------------ agregados (stage)
    Check(
        name="conservation_daily_vs_fact",
        layer="aggregates",
        severity="block",
        # contagens e valores por categoria e moeda do diário = os do fato
        sql=f"""
            WITH f AS ({_fact_by_currency()}),
                 d AS (SELECT * FROM fct_reconciliation_daily_stage)
            SELECT coalesce(f.currency, d.currency) AS currency
            FROM f FULL OUTER JOIN d ON d.currency = f.currency
            WHERE f.currency IS NULL OR d.currency IS NULL OR {_DAILY_DIFFERS}
        """,
    ),
    Check(
        name="service_run_missing",
        layer="aggregates",
        severity="alert",
        sql="SELECT 'no service run' WHERE NOT EXISTS (SELECT 1 FROM fct_reconciliation_run_stage)",
    ),
    Check(
        name="service_run_failed_or_empty",
        layer="aggregates",
        severity="alert",
        sql="""
            SELECT run_id FROM fct_reconciliation_run_stage
            WHERE status = 'FAILED' OR NOT has_results
        """,
    ),
    Check(
        name="service_run_total_differs_from_file",
        layer="aggregates",
        severity="alert",
        sql="""
            SELECT run_id FROM fct_reconciliation_run_stage
            WHERE declared_matches_file IS DISTINCT FROM true
        """,
    ),
)
