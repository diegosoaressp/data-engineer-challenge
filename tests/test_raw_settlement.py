from datetime import date
from pathlib import Path

import pytest

from recon.db import connect
from recon.ingest.settlement import (
    SettlementFileError,
    load_raw_settlements,
    locate_settlement_file,
)

SAMPLE_CSV = Path(__file__).parent.parent / "docs" / "sample-data" / "settlement_paysettler.csv"
D = date(2025, 3, 16)
HEADER = "transaction_id,merchant_id,amount,currency,settled_at,processor_reference,status\n"


@pytest.fixture
def con(tmp_path):
    from recon.ddl import apply_ddl

    connection = connect(tmp_path / "wh.duckdb")
    apply_ddl(connection)
    yield connection
    connection.close()


def _touch(tmp_path, name):
    folder = tmp_path / "paysettler"
    folder.mkdir(exist_ok=True)
    path = folder / name
    path.write_text(HEADER)
    return path


@pytest.mark.parametrize("name", ["settlement_2025-03-16.csv", "settlement_20250316_02_00.csv"])
def test_both_name_patterns_found(tmp_path, name):
    expected = _touch(tmp_path, name)
    assert locate_settlement_file(D, source_dir=tmp_path) == expected


def test_explicit_file_has_priority(tmp_path):
    _touch(tmp_path, "settlement_2025-03-16.csv")
    assert locate_settlement_file(D, tmp_path, SAMPLE_CSV) == SAMPLE_CSV


def test_missing_file_blocks(tmp_path):
    with pytest.raises(SettlementFileError):
        locate_settlement_file(D, source_dir=tmp_path)
    with pytest.raises(SettlementFileError):
        locate_settlement_file(D, settlement_file=tmp_path / "nope.csv")
    with pytest.raises(SettlementFileError):
        locate_settlement_file(D)


def test_ambiguous_files_block(tmp_path):
    _touch(tmp_path, "settlement_2025-03-16.csv")
    _touch(tmp_path, "settlement_20250316_02_00.csv")
    with pytest.raises(SettlementFileError, match="ambiguous"):
        locate_settlement_file(D, source_dir=tmp_path)


def test_load_sample_is_varchar_and_idempotent(con):
    assert load_raw_settlements(con, SAMPLE_CSV, "r1") == 510
    assert load_raw_settlements(con, SAMPLE_CSV, "r2") == 510
    types = {r[0]: r[1] for r in con.execute("DESCRIBE raw.settlements").fetchall()}
    assert types["amount"] == "VARCHAR" and types["settled_at"] == "VARCHAR"
    assert con.execute(
        "SELECT DISTINCT processor, _source_file, _pipeline_run_id FROM raw.settlements"
    ).fetchall() == [("paysettler", "settlement_paysettler.csv", "r2")]
    assert con.execute("SELECT count(DISTINCT _row_number) FROM raw.settlements").fetchone()[0] == 510
    # valores em formato BR chegam intactos
    assert con.execute("SELECT count(*) FROM raw.settlements WHERE amount LIKE 'R$%'").fetchone()[0] == 22


def test_empty_file_blocks(con, tmp_path):
    path = _touch(tmp_path, "settlement_2025-03-16.csv")
    with pytest.raises(SettlementFileError, match="empty"):
        load_raw_settlements(con, path, "r1")
