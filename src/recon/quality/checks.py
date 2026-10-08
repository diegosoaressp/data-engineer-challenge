from dataclasses import dataclass
from typing import Literal

Severity = Literal["block", "alert"]
Kind = Literal["rows", "metric"]


@dataclass(frozen=True)
class Check:
    """Declarative data-quality check.

    kind="rows" (default): `sql` returns the offending rows (first column = key shown in
    the sample). Observed value = number of rows; passes when observed <= threshold.

    kind="metric": `sql` returns one scalar (e.g. a rate or a difference), recorded as the
    observed value; passes when observed <= threshold. NULL (metric undefined, e.g. rate
    over zero rows) passes and is recorded as NULL.

    `sql` may use {reference_date} and {window_days}.
    """

    name: str
    layer: str
    severity: Severity
    sql: str
    threshold: float = 0
    kind: Kind = "rows"

    def __post_init__(self) -> None:
        if self.severity not in ("block", "alert"):
            raise ValueError(f"invalid severity {self.severity!r} for check {self.name}")
        if self.kind not in ("rows", "metric"):
            raise ValueError(f"invalid kind {self.kind!r} for check {self.name}")


@dataclass(frozen=True)
class CheckResult:
    check: Check
    passed: bool
    observed: float | None
    sample_keys: list[str]


class BlockingCheckFailed(Exception):
    def __init__(self, failed: list[CheckResult]) -> None:
        self.failed = failed
        names = ", ".join(r.check.name for r in failed)
        super().__init__(f"blocking data-quality checks failed: {names}")
