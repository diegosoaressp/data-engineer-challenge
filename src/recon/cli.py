import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from recon.pipeline import run_pipeline


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date '{value}', expected YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="recon", description="Settlement reconciliation pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run the pipeline for one reference_date")
    run.add_argument("--reference-date", type=_parse_date, required=True)
    run.add_argument("--source-dir", type=Path, default=None)
    run.add_argument("--settlement-file", type=Path, default=None)

    backfill = sub.add_parser("backfill", help="Run the pipeline for a date range")
    backfill.add_argument("--start", type=_parse_date, required=True)
    backfill.add_argument("--end", type=_parse_date, required=True)
    backfill.add_argument("--source-dir", type=Path, required=True)
    return parser


def _run_one(reference_date: date, args: argparse.Namespace) -> bool:
    """Run one date. Failures are already logged and recorded by the pipeline."""
    try:
        run_pipeline(
            reference_date,
            source_dir=args.source_dir,
            settlement_file=getattr(args, "settlement_file", None),
        )
    except Exception:  # noqa: BLE001 - já logado e gravado em ops.pipeline_runs; backfill segue
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return 0 if _run_one(args.reference_date, args) else 1
    if args.end < args.start:
        build_parser().error("--end must be >= --start")
    failed = []
    day = args.start
    while day <= args.end:  # cada data é atômica e independente
        if not _run_one(day, args):
            failed.append(day.isoformat())
        day += timedelta(days=1)
    if failed:
        print(f"backfill: {len(failed)} date(s) failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0
