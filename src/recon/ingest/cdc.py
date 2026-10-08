from pathlib import Path

import duckdb

SQL_PATH = Path(__file__).parent.parent / "sql" / "raw" / "cdc_table.sql"

# raw table -> parquet glob inside the source dir
CDC_SOURCES = {
    "transactions": "transactions_batch_*.parquet",
    "reconciliation_runs": "reconciliation_runs.parquet",
    "reconciliation_results": "reconciliation_results.parquet",
    "enterprise_company": "enterprise_company.parquet",
}


def load_raw_cdc(con: duckdb.DuckDBPyConnection, source_dir: Path, run_id: str) -> dict[str, int]:
    """Recreate raw.* from the CDC parquet extracts. Returns row count per raw table.

    Columns keep the source types; batches are merged with union_by_name so columns
    missing from older batches (payment_method) come back NULL.
    """
    template = SQL_PATH.read_text(encoding="utf-8")
    counts: dict[str, int] = {}
    for table, pattern in CDC_SOURCES.items():
        files = sorted(str(p) for p in Path(source_dir).glob(pattern))
        if not files:
            raise FileNotFoundError(f"no files matching {pattern} in {source_dir}")
        con.execute(template.format(table=table), [run_id, files])
        counts[f"raw.{table}"] = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    return counts
