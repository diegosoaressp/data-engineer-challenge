from collections.abc import Sequence
from datetime import date

import duckdb

from recon.logging import RunLogger
from recon.quality.checks import BlockingCheckFailed, Check, CheckResult
from recon.quality.registry import CHECKS

SAMPLE_SIZE = 5


def evaluate(con: duckdb.DuckDBPyConnection, check: Check) -> CheckResult:
    observed = con.execute(f"SELECT count(*) FROM ({check.sql}) AS q").fetchone()[0]
    sample = [
        str(r[0]) for r in con.execute(f"SELECT * FROM ({check.sql}) AS q LIMIT {SAMPLE_SIZE}").fetchall()
    ]  # fmt: skip
    return CheckResult(check, observed <= check.threshold, float(observed), sample)


def _record(
    con: duckdb.DuckDBPyConnection, run_id: str, reference_date: date, result: CheckResult
) -> None:
    c = result.check
    con.execute(
        """
        INSERT INTO ops.dq_results
            (run_id, reference_date, check_name, layer, severity, passed,
             observed_value, threshold, sample_keys)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id, reference_date, c.name, c.layer, c.severity, result.passed,
            result.observed, c.threshold, ",".join(result.sample_keys) or None,
        ],
    )  # fmt: skip


def record_block(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    reference_date: date,
    name: str,
    layer: str,
    detail: str,
) -> None:
    """Record a failed block that is detected outside the SQL checks (e.g. missing file)."""
    check = Check(name, layer, "block", sql="SELECT 1")
    _record(con, run_id, reference_date, CheckResult(check, False, 1.0, [detail]))


def run_checks(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    reference_date: date,
    log: RunLogger,
    checks: Sequence[Check] = CHECKS,
    layer: str | None = None,
    severity: str | None = None,
) -> list[CheckResult]:
    """Run every check, persist all results to ops.dq_results, then raise if any block failed.

    Alerts are logged and the run continues. Block failures are raised only after all
    checks ran, so the operator sees every problem at once.
    """
    results: list[CheckResult] = []
    for check in checks:
        if layer is not None and check.layer != layer:
            continue
        if severity is not None and check.severity != severity:
            continue
        result = evaluate(con, check)
        _record(con, run_id, reference_date, result)
        results.append(result)
        if not result.passed:
            level = log.error if check.severity == "block" else log.warning
            level(
                f"dq check failed: {check.name}",
                extra={
                    "check": check.name,
                    "severity": check.severity,
                    "observed": result.observed,
                    "threshold": check.threshold,
                    "sample_keys": result.sample_keys,
                },
            )
    blocked = [r for r in results if not r.passed and r.check.severity == "block"]
    if blocked:
        raise BlockingCheckFailed(blocked)
    return results
