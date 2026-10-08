import argparse
import uuid
from datetime import date, timedelta
from pathlib import Path

from recon.logging import get_logger


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


def _run_one(reference_date: date, args: argparse.Namespace) -> None:
    run_id = uuid.uuid4().hex
    log = get_logger(run_id, reference_date.isoformat())
    log.info("run started (skeleton: pipeline not implemented yet)")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        _run_one(args.reference_date, args)
    else:
        if args.end < args.start:
            build_parser().error("--end must be >= --start")
        day = args.start
        while day <= args.end:
            _run_one(day, args)
            day += timedelta(days=1)
    return 0
