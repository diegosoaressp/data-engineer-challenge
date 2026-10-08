from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).parent.parent / "sql" / "staging"

# ordem importa: results depende de runs (has_orphan_run)
CDC_STAGING = (
    "stg_transactions",
    "stg_reconciliation_runs",
    "stg_reconciliation_results",
    "stg_merchants_history",
)


def _ensure_optional_columns(con: duckdb.DuckDBPyConnection) -> None:
    """Colunas de schema drift ausentes em todos os batches viram NULL no raw."""
    cols = {r[0] for r in con.execute("DESCRIBE raw.transactions").fetchall()}
    if "payment_method" not in cols:
        con.execute("ALTER TABLE raw.transactions ADD COLUMN payment_method VARCHAR")


def build_cdc_staging(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Recreate staging.stg_* from raw CDC. Returns row count per staging table."""
    _ensure_optional_columns(con)
    counts: dict[str, int] = {}
    for name in CDC_STAGING:
        con.execute((SQL_DIR / f"{name}.sql").read_text(encoding="utf-8"))
        counts[f"staging.{name}"] = con.execute(f"SELECT count(*) FROM staging.{name}").fetchone()[
            0
        ]
    return counts
