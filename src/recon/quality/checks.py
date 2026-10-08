from dataclasses import dataclass
from typing import Literal

Severity = Literal["block", "alert"]


@dataclass(frozen=True)
class Check:
    """Declarative data-quality check.

    `sql` returns the offending rows (first column = key shown in the sample).
    Observed value = number of rows; the check passes when observed <= threshold.
    """

    name: str
    layer: str
    severity: Severity
    sql: str
    threshold: float = 0

    def __post_init__(self) -> None:
        if self.severity not in ("block", "alert"):
            raise ValueError(f"invalid severity {self.severity!r} for check {self.name}")


@dataclass(frozen=True)
class CheckResult:
    check: Check
    passed: bool
    observed: float
    sample_keys: list[str]


class BlockingCheckFailed(Exception):
    def __init__(self, failed: list[CheckResult]) -> None:
        self.failed = failed
        names = ", ".join(r.check.name for r in failed)
        super().__init__(f"blocking data-quality checks failed: {names}")
