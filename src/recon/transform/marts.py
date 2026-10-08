from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from recon import config

SQL_DIR = Path(__file__).parent.parent / "sql" / "marts"
FCT_RECONCILIATION_STAGE = "fct_reconciliation_stage"


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
