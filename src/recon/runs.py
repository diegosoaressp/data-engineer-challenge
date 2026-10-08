import json
import os
import subprocess
from datetime import UTC, date, datetime

import duckdb


def git_sha() -> str | None:
    env_sha = os.environ.get("GIT_SHA")
    if env_sha:
        return env_sha
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.stdout.strip() or None


def start_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    reference_date: date,
    settlement_file: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO ops.pipeline_runs
            (run_id, reference_date, status, started_at, settlement_file, git_sha)
        VALUES (?, ?, 'RUNNING', ?, ?, ?)
        """,
        [run_id, reference_date, datetime.now(UTC), settlement_file, git_sha()],
    )


def set_settlement_file(con: duckdb.DuckDBPyConnection, run_id: str, path: str) -> None:
    con.execute("UPDATE ops.pipeline_runs SET settlement_file = ? WHERE run_id = ?", [path, run_id])


def finish_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    rows_by_stage: dict[str, int] | None = None,
) -> None:
    _close(con, run_id, "SUCCESS", rows_by_stage, None)


def fail_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    error: BaseException | str,
    rows_by_stage: dict[str, int] | None = None,
) -> None:
    _close(con, run_id, "FAILED", rows_by_stage, str(error))


def _close(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
    rows_by_stage: dict[str, int] | None,
    error: str | None,
) -> None:
    finished = datetime.now(UTC)
    con.execute(
        """
        UPDATE ops.pipeline_runs
        SET status = ?,
            finished_at = ?,
            duration_ms = CAST(epoch_ms(?) - epoch_ms(started_at) AS BIGINT),
            rows_by_stage = ?,
            error = ?
        WHERE run_id = ?
        """,
        [status, finished, finished, json.dumps(rows_by_stage or {}), error, run_id],
    )
