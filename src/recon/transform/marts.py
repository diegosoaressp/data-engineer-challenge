from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from recon import config
from recon.db import transaction

SQL_DIR = Path(__file__).parent.parent / "sql" / "marts"
FCT_RECONCILIATION_STAGE = "fct_reconciliation_stage"
AUDIT_TABLE = "marts.audit_reconciliation_history"


@dataclass(frozen=True)
class PublishTarget:
    """A mart, its temp stage table and the columns that scope DELETE (empty = full replace)."""

    mart: str
    stage: str
    scope: tuple[str, ...]


PUBLISH_TARGETS: tuple[PublishTarget, ...] = (
    PublishTarget(
        "marts.fct_reconciliation", FCT_RECONCILIATION_STAGE, ("reference_date", "processor")
    ),
    PublishTarget(
        "marts.fct_reconciliation_daily",
        "fct_reconciliation_daily_stage",
        ("reference_date", "processor"),
    ),
    PublishTarget(
        "marts.fct_financial_daily", "fct_financial_daily_stage", ("reference_date", "processor")
    ),
    PublishTarget("marts.fct_internal_transactions", "fct_internal_transactions_stage", ()),
    PublishTarget(
        "marts.fct_reconciliation_run", "fct_reconciliation_run_stage", ("reference_date",)
    ),
    PublishTarget("marts.dim_merchant", "dim_merchant_stage", ()),
)

# Estágios dos agregados, na ordem de construção (todos derivam do stage do fato ou do staging).
AGGREGATE_SQL = (
    "fct_reconciliation_daily",
    "fct_financial_daily",
    "fct_internal_transactions",
    "fct_reconciliation_run",
    "dim_merchant",
)


def _render(name: str, values: dict[str, str]) -> str:
    sql = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
    for key, value in values.items():
        sql = sql.replace("{" + key + "}", value.replace("'", "''"))
    return sql


def build_fct_reconciliation(
    con: duckdb.DuckDBPyConnection,
    reference_date: date,
    processor: str,
    tolerance: Decimal = config.TOLERANCE,
    window_days: int = config.WINDOW_DAYS,
) -> int:
    """Build the temp stage table for one reference_date/processor. Returns row count.

    Does not touch marts: publishing (DELETE + INSERT) happens after block checks.
    """
    con.execute(
        _render(
            "fct_reconciliation",
            {
                "reference_date": reference_date.isoformat(),
                "processor": processor,
                "tolerance": str(tolerance),
                "window_days": str(int(window_days)),
            },
        )
    )
    return con.execute(f"SELECT count(*) FROM {FCT_RECONCILIATION_STAGE}").fetchone()[0]


def build_aggregates(con: duckdb.DuckDBPyConnection, reference_date: date) -> dict[str, int]:
    """Build the aggregate/dimension stage tables. Requires the fct_reconciliation stage.

    Temp tables only; marts are untouched. Returns row count per stage.
    """
    values = {"reference_date": reference_date.isoformat()}
    counts: dict[str, int] = {}
    for name in AGGREGATE_SQL:
        con.execute(_render(name, values))
        counts[f"stage.{name}"] = con.execute(f"SELECT count(*) FROM {name}_stage").fetchone()[0]
    return counts


def publish_marts(
    con: duckdb.DuckDBPyConnection,
    reference_date: date,
    processor: str,
    run_id: str,
) -> dict[str, int]:
    """Replace the rows of (`reference_date`, `processor`) in every mart from its stage table.

    The DELETE scope comes from the arguments, never from the stage: an empty stage still
    clears the old rows. `dim_merchant` (no scope) is replaced entirely. Single transaction
    (DELETE + INSERT per mart): any exception rolls back all marts, so a mid-run failure
    leaves the previous state intact and a re-run never duplicates rows. Call only after
    block checks passed on the stage tables.
    `run_id` is required (fail closed): the same transaction also appends the published fct_reconciliation rows to
    `marts.audit_reconciliation_history` (append-only; never deleted), so a rollback leaves
    no audit snapshot either.
    Returns rows written per mart for the scope.
    """
    values = {"reference_date": reference_date, "processor": processor}
    written: dict[str, int] = {}
    with transaction(con):
        for target in PUBLISH_TARGETS:
            where = " AND ".join(f"{col} = ?" for col in target.scope) or "true"
            params = [values[col] for col in target.scope]
            con.execute(f"DELETE FROM {target.mart} WHERE {where}", params)
            con.execute(f"INSERT INTO {target.mart} BY NAME SELECT * FROM {target.stage}")
            written[target.mart] = con.execute(
                f"SELECT count(*) FROM {target.mart} WHERE {where}", params
            ).fetchone()[0]
        written[AUDIT_TABLE] = _append_audit(con, run_id)
    return written


def _append_audit(con: duckdb.DuckDBPyConnection, run_id: str) -> int:
    """Append a snapshot of the fct_reconciliation stage. Never deletes or updates."""
    con.execute(
        f"INSERT INTO {AUDIT_TABLE} BY NAME "
        f"SELECT ? AS pipeline_run_id, now() AS audited_at, * FROM {FCT_RECONCILIATION_STAGE}",
        [run_id],
    )
    return con.execute(
        f"SELECT count(*) FROM {AUDIT_TABLE} WHERE pipeline_run_id = ?", [run_id]
    ).fetchone()[0]
