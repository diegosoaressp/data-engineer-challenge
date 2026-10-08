from pathlib import Path

import duckdb

DDL_DIR = Path(__file__).parent / "sql" / "ddl"


def apply_ddl(con: duckdb.DuckDBPyConnection) -> None:
    """Apply every DDL file in name order. Idempotent: all statements use IF NOT EXISTS."""
    for path in sorted(DDL_DIR.glob("*.sql")):
        con.execute(path.read_text(encoding="utf-8"))
