import io
import json

import pytest

from recon import cli, config
from recon.db import connect, transaction
from recon.logging import get_logger


def test_help_works(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    assert "recon" in capsys.readouterr().out


def test_run_requires_reference_date():
    with pytest.raises(SystemExit) as exc:
        cli.main(["run"])
    assert exc.value.code != 0


def test_log_has_run_id_and_reference_date():
    buf = io.StringIO()
    log = get_logger("abc123", "2025-03-16", stream=buf)
    log.info("hello", extra={"rows": 3})
    record = json.loads(buf.getvalue())
    assert record["run_id"] == "abc123"
    assert record["reference_date"] == "2025-03-16"
    assert record["message"] == "hello"
    assert record["rows"] == 3


def test_run_command_emits_structured_log(capsys, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_PATH", tmp_path)
    assert cli.main(["run", "--reference-date", "2025-03-16", "--source-dir", str(tmp_path)]) == 1
    record = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert record["reference_date"] == "2025-03-16"
    assert record["run_id"]


def test_transaction_rolls_back_on_error(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    con.execute("CREATE TABLE t (x INTEGER)")
    with pytest.raises(RuntimeError), transaction(con):
        con.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    with transaction(con):
        con.execute("INSERT INTO t VALUES (2)")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 1


def test_connection_timezone_is_utc(tmp_path):
    from recon.db import connect

    con = connect(tmp_path / "tz.duckdb")
    try:
        assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
        # TIMESTAMPTZ a partir de texto naive é UTC, sem depender do fuso da máquina
        got = con.execute(
            "SELECT strftime(CAST('2025-03-16 10:00:00' AS TIMESTAMPTZ) AT TIME ZONE 'UTC', "
            "'%H:%M')"
        ).fetchone()[0]
        assert got == "10:00"
    finally:
        con.close()
