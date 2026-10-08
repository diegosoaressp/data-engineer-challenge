import json
from datetime import date
from pathlib import Path

import pytest

from recon import cli, config, pipeline
from recon.db import connect
from recon.pipeline import run_pipeline
from recon.quality.checks import BlockingCheckFailed

SAMPLE = Path(__file__).parent.parent / "docs" / "sample-data"
SAMPLE_CSV = SAMPLE / "settlement_paysettler.csv"
D = date(2025, 3, 16)


@pytest.fixture
def wh(tmp_path):
    return tmp_path / "wh.duckdb"


def _query(wh, sql, params=None):
    con = connect(wh)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def _run(wh, settlement_file=SAMPLE_CSV, ref=D):
    con = connect(wh)
    try:
        return run_pipeline(
            ref, source_dir=SAMPLE, settlement_file=settlement_file, con=con, processor="paysettler"
        )
    finally:
        con.close()


def _marts(wh):
    return _query(wh, "SELECT * FROM marts.fct_reconciliation ORDER BY transaction_id")


def test_successful_run_is_recorded_and_publishes_marts(wh):
    run_id = _run(wh)
    (status, error, rows, sfile) = _query(
        wh,
        "SELECT status, error, rows_by_stage, settlement_file FROM ops.pipeline_runs "
        "WHERE run_id = ?",
        [run_id],
    )[0]
    assert (status, error) == ("SUCCESS", None)
    assert sfile.endswith("settlement_paysettler.csv")
    counts = json.loads(rows)
    assert counts["raw.settlements"] == 510
    assert counts["marts.fct_reconciliation"] == len(_marts(wh)) > 0
    # alertas dos dados sujos da amostra gravados em ops.dq_results, sem bloquear
    failed = _query(
        wh, "SELECT check_name FROM ops.dq_results WHERE run_id = ? AND NOT passed", [run_id]
    )
    assert ("settlement_identical_duplicates",) in failed


def test_rerun_same_date_is_idempotent(wh):
    _run(wh)
    first = _marts(wh)
    _run(wh)
    assert _marts(wh) == first
    assert _query(wh, "SELECT count(*) FROM ops.pipeline_runs")[0][0] == 2


def test_block_failure_marks_run_failed_and_leaves_marts_intact(wh, tmp_path):
    _run(wh)
    before = _marts(wh)

    lines = SAMPLE_CSV.read_text(encoding="utf-8").splitlines()
    header, first, *rest = lines
    cols = first.split(",")
    cols[2] = ""  # amount nulo -> settlement_amount_unparseable (block)
    bad = tmp_path / "bad.csv"
    bad.write_text("\n".join([header, ",".join(cols), *rest]) + "\n", encoding="utf-8")

    with pytest.raises(BlockingCheckFailed, match="settlement_amount_unparseable"):
        _run(wh, settlement_file=bad)

    status, error = _query(
        wh,
        "SELECT status, error FROM ops.pipeline_runs WHERE settlement_file LIKE '%bad.csv'",
    )[0]
    assert status == "FAILED"
    assert "settlement_amount_unparseable" in error
    reason = _query(
        wh,
        "SELECT severity, passed, observed_value FROM ops.dq_results "
        "WHERE check_name = 'settlement_amount_unparseable' AND NOT passed",
    )
    assert reason == [("block", False, 1.0)]
    assert _marts(wh) == before


def test_missing_settlement_file_fails_run_and_is_recorded(wh, tmp_path):
    with pytest.raises(Exception, match="no settlement file"):
        con = connect(wh)
        try:
            run_pipeline(D, source_dir=tmp_path, con=con)
        finally:
            con.close()
    status, error = _query(wh, "SELECT status, error FROM ops.pipeline_runs")[0]
    assert status == "FAILED"
    assert "SettlementFileError" in error
    rows = _query(wh, "SELECT check_name, severity, passed, layer FROM ops.dq_results")
    assert rows == [("settlement_file_missing", "block", False, "raw")]


def test_publish_is_last_step_failed_run_leaves_marts_intact(wh, monkeypatch):
    _run(wh)
    before = _marts(wh)
    seen = {}

    def boom(con, ref, processor):
        # no momento do publish, checks (inclusive alert) desta execução já foram gravados
        seen["alerts"] = con.execute(
            "SELECT count(*) FROM ops.dq_results WHERE severity = 'alert' AND run_id NOT IN "
            "(SELECT run_id FROM ops.pipeline_runs WHERE status = 'SUCCESS')"
        ).fetchone()[0]
        raise RuntimeError("publish exploded")

    monkeypatch.setattr(pipeline, "publish_marts", boom)
    with pytest.raises(RuntimeError, match="publish exploded"):
        _run(wh)
    assert seen["alerts"] > 0
    assert _query(wh, "SELECT status FROM ops.pipeline_runs ORDER BY started_at") == [
        ("SUCCESS",),
        ("FAILED",),
    ]
    assert _marts(wh) == before


def test_cli_run_returns_exit_codes_and_logs_with_run_context(wh, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(config, "DATA_PATH", wh.parent)
    monkeypatch.setattr(config, "WAREHOUSE_FILE", wh.name)
    args = ["run", "--reference-date", "2025-03-16", "--source-dir", str(SAMPLE)]
    assert cli.main([*args, "--settlement-file", str(SAMPLE_CSV)]) == 0
    assert cli.main(["run", "--reference-date", "2025-03-16", "--source-dir", str(tmp_path)]) == 1
    last = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert last["reference_date"] == "2025-03-16"
    assert last["run_id"]
    assert last["level"] == "ERROR"
