from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from recon import config
from recon.db import transaction

SQL_DIR = Path(__file__).parent.parent / "sql" / "marts"
FCT_RECONCILIATION_STAGE = "fct_reconciliation_stage"

# (tabela do mart, tabela temporária de stage, escopo também por processor).
# Novos fatos (etapa 10) entram aqui.
PUBLISH_TARGETS: tuple[tuple[str, str, bool], ...] = (
    ("marts.fct_reconciliation", FCT_RECONCILIATION_STAGE, True),
)


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
    sql = (SQL_DIR / "fct_reconciliation.sql").read_text(encoding="utf-8")
    for key, value in {
        "reference_date": reference_date.isoformat(),
        "processor": processor.replace("'", "''"),
        "tolerance": str(tolerance),
        "window_days": str(int(window_days)),
    }.items():
        sql = sql.replace("{" + key + "}", value)
    con.execute(sql)
    return con.execute(f"SELECT count(*) FROM {FCT_RECONCILIATION_STAGE}").fetchone()[0]


def publish_marts(
    con: duckdb.DuckDBPyConnection, reference_date: date, processor: str
) -> dict[str, int]:
    """Replace the rows of (`reference_date`, `processor`) in every mart from its stage table.

    The DELETE scope comes from the arguments, never from the stage: an empty stage still
    clears the old rows. Single transaction (DELETE + INSERT per mart): any exception rolls
    back all marts, so a mid-run failure leaves the previous state intact and a re-run never
    duplicates rows. Call only after block checks passed on the stage tables.
    Returns rows written per mart for the scope.
    """
    written: dict[str, int] = {}
    with transaction(con):
        for mart, stage, by_processor in PUBLISH_TARGETS:
            scope_sql = " AND processor = ?" if by_processor else ""
            scope = [reference_date, processor] if by_processor else [reference_date]
            con.execute(f"DELETE FROM {mart} WHERE reference_date = ?{scope_sql}", scope)
            con.execute(f"INSERT INTO {mart} BY NAME SELECT * FROM {stage}")
            written[mart] = con.execute(
                f"SELECT count(*) FROM {mart} WHERE reference_date = ?{scope_sql}", scope
            ).fetchone()[0]
    return written
