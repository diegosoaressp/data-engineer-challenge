from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from recon import config


def connect(path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    target = Path(path) if path is not None else config.warehouse_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(target))
    # janela, datas e TIMESTAMPTZ naive são sempre interpretados em UTC
    con.execute("SET TimeZone = 'UTC'")
    return con


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection) -> Iterator[duckdb.DuckDBPyConnection]:
    """Commit on success, roll back on any exception."""
    con.execute("BEGIN TRANSACTION")
    try:
        yield con
    except BaseException:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")
