import uuid
from datetime import date
from pathlib import Path

import duckdb

from recon import config
from recon.db import connect
from recon.ddl import apply_ddl
from recon.ingest.cdc import load_raw_cdc
from recon.ingest.settlement import (
    SettlementFileError,
    load_raw_settlements,
    locate_settlement_file,
)
from recon.logging import RunLogger, get_logger
from recon.quality.runner import record_block, run_checks
from recon.runs import fail_run, finish_run, set_settlement_file, start_run
from recon.transform.marts import build_aggregates, build_fct_reconciliation, publish_marts
from recon.transform.staging import build_cdc_staging, build_settlement_staging


def run_pipeline(
    reference_date: date,
    source_dir: Path | None = None,
    settlement_file: Path | None = None,
    processor: str = "paysettler",
    con: duckdb.DuckDBPyConnection | None = None,
    log: RunLogger | None = None,
) -> str:
    """Run raw -> staging -> checks -> stage -> checks -> publish for one date.

    Returns the run_id on success. On any failure the run is marked FAILED in
    ops.pipeline_runs (with the error) and the exception is re-raised. Marts are only
    touched by `publish_marts`, after every block check passed, so a failed run leaves
    them as they were.
    """
    run_id = uuid.uuid4().hex
    log = log or get_logger(run_id, reference_date.isoformat())
    source_dir = Path(source_dir) if source_dir is not None else config.SOURCE_DIR
    owns_con = con is None
    con = con or connect()
    rows: dict[str, int] = {}
    try:
        apply_ddl(con)
        start_run(con, run_id, reference_date, str(settlement_file) if settlement_file else None)
        log.info("run started", extra={"processor": processor, "source_dir": str(source_dir)})
        try:
            _execute(con, run_id, reference_date, source_dir, settlement_file, processor, log, rows)
        except Exception as exc:
            fail_run(con, run_id, f"{type(exc).__name__}: {exc}", rows)
            log.error("run failed", extra={"error": str(exc), "rows_by_stage": rows})
            raise
        finish_run(con, run_id, rows)
        log.info("run succeeded", extra={"rows_by_stage": rows})
        return run_id
    finally:
        if owns_con:
            con.close()


def _execute(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    reference_date: date,
    source_dir: Path,
    settlement_file: Path | None,
    processor: str,
    log: RunLogger,
    rows: dict[str, int],
) -> None:
    # Toda falha na carga raw grava um block em ops.dq_results antes de o run virar FAILED.
    try:
        path = locate_settlement_file(reference_date, source_dir, settlement_file, processor)
        set_settlement_file(con, run_id, str(path))
    except SettlementFileError as exc:
        record_block(con, run_id, reference_date, "settlement_file_missing", "raw", str(exc))
        raise
    try:
        rows.update(load_raw_cdc(con, source_dir, run_id))
    except Exception as exc:
        record_block(con, run_id, reference_date, "raw_cdc_load_failed", "raw", str(exc))
        raise
    try:
        rows["raw.settlements"] = load_raw_settlements(con, path, run_id, processor)
    except SettlementFileError as exc:  # arquivo vazio
        record_block(con, run_id, reference_date, "settlement_file_missing", "raw", str(exc))
        raise
    except Exception as exc:
        record_block(con, run_id, reference_date, "raw_settlement_read_failed", "raw", str(exc))
        raise
    run_checks(con, run_id, reference_date, log, layer="raw")
    rows.update(build_cdc_staging(con))
    rows["staging.stg_settlements"] = build_settlement_staging(con)
    log.info("staging built", extra={"rows_by_stage": rows})

    # Invariante: run FAILED => marts intocados. Todos os checks (block e alert) rodam sobre
    # staging e stage ANTES do publish; publish_marts é o último passo antes de finish_run.
    run_checks(con, run_id, reference_date, log, layer="staging")
    rows["stage.fct_reconciliation"] = build_fct_reconciliation(con, reference_date, processor)
    run_checks(con, run_id, reference_date, log, layer="marts")
    # agregados derivam do stage já validado; só tabelas temporárias, marts intocados
    rows.update(build_aggregates(con, reference_date))
    run_checks(con, run_id, reference_date, log, layer="aggregates")

    for mart, count in publish_marts(con, reference_date, processor, run_id=run_id).items():
        rows[mart] = count
    log.info("marts published", extra={"rows_by_stage": rows})
