from decimal import Decimal
from pathlib import Path

import pytest

from recon.db import connect
from recon.ddl import apply_ddl
from recon.ingest.settlement import load_raw_settlements
from recon.transform.staging import build_settlement_staging

SAMPLE_CSV = Path(__file__).parent.parent / "docs" / "sample-data" / "settlement_paysettler.csv"
HEADER = "transaction_id,merchant_id,amount,currency,settled_at,processor_reference,status\n"
TID, TID2 = "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"


@pytest.fixture
def con(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    apply_ddl(connection)
    yield connection
    connection.close()


def _stage(con, tmp_path, lines: list[str]) -> int:
    csv = tmp_path / "s.csv"
    csv.write_text(HEADER + "\n".join(lines) + "\n", encoding="utf-8")
    load_raw_settlements(con, csv, "run-1")
    return build_settlement_staging(con)


def _line(amount="10.00", settled="2025-03-16T10:00:00Z", status="SETTLED", tid=TID,
          merchant="MERCH_001", ref="PS-1"):  # fmt: skip
    return f'{tid},{merchant},"{amount}",BRL,{settled},{ref},{status}'


def _one(con, sql):
    return con.execute(sql).fetchone()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("R$ 32.245,91", Decimal("32245.91")),
        ("1234,56", Decimal("1234.56")),
        ("1234.56", Decimal("1234.56")),
        ("R$ 0,01", Decimal("0.01")),
        ("abc", None),
    ],
)
def test_amount_normalization(con, tmp_path, raw, expected):
    _stage(con, tmp_path, [_line(amount=raw)])
    assert _one(con, "SELECT amount FROM staging.stg_settlements")[0] == expected


def test_amount_br_flag(con, tmp_path):
    _stage(con, tmp_path, [_line(amount="R$ 1,00"), _line(amount="2.00", tid=TID2)])
    rows = dict(
        con.execute(
            "SELECT transaction_id, amount_was_br_format FROM staging.stg_settlements"
        ).fetchall()
    )
    assert rows == {TID: True, TID2: False}


@pytest.mark.parametrize(
    "settled",
    ["2025-03-16T13:35:13Z", "2025-03-16T13:35:13+00:00", "2025-03-16T13:35:13"],
)
def test_settled_at_formats_are_utc(con, tmp_path, settled):
    _stage(con, tmp_path, [_line(settled=settled)])
    got, no_tz = _one(
        con,
        "SELECT strftime(settled_at AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S'), settled_at_no_tz FROM staging.stg_settlements",
    )
    assert got == "2025-03-16 13:35:13"
    assert no_tz == (not settled.endswith(("Z", "+00:00")))


def test_session_timezone_does_not_shift_naive_settled_at(con, tmp_path):
    con.execute("SET TimeZone = 'America/Sao_Paulo'")
    _stage(con, tmp_path, [_line(settled="2025-03-16T13:35:13")])
    got = _one(
        con,
        "SELECT strftime(settled_at AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S') FROM staging.stg_settlements",
    )[0]
    assert got == "2025-03-16 13:35:13"


def test_invalid_settled_at_is_null(con, tmp_path):
    _stage(con, tmp_path, [_line(settled="not-a-date")])
    assert _one(con, "SELECT settled_at FROM staging.stg_settlements")[0] is None


def test_identical_duplicates_collapse(con, tmp_path):
    assert _stage(con, tmp_path, [_line(), _line(), _line(), _line(tid=TID2)]) == 2
    rows = dict(
        con.execute("SELECT transaction_id, dup_count FROM staging.stg_settlements").fetchall()
    )
    assert rows == {TID: 3, TID2: 1}
    assert _one(con, "SELECT max(version_count) FROM staging.stg_settlements")[0] == 1


def test_latest_by_settled_at_with_reversal_flag(con, tmp_path):
    _stage(
        con,
        tmp_path,
        [
            _line(status="REVERSED", settled="2025-03-16T12:00:00Z", ref="PS-2"),
            _line(status="SETTLED", settled="2025-03-16T08:00:00Z", ref="PS-1"),
        ],
    )
    sql = "SELECT status, has_reversal, version_count, signed_amount FROM staging.stg_settlements"
    assert con.execute(sql).fetchall() == [("REVERSED", True, 2, Decimal("-10.00"))]


def test_latest_settled_wins_even_if_older_version_is_reversal(con, tmp_path):
    _stage(
        con,
        tmp_path,
        [
            _line(status="SETTLED", settled="2025-03-16T12:00:00Z", ref="PS-2"),
            _line(status="REVERSED", settled="2025-03-16T08:00:00Z", ref="PS-1"),
        ],
    )
    sql = "SELECT status, has_reversal, signed_amount FROM staging.stg_settlements"
    assert con.execute(sql).fetchall() == [("SETTLED", True, Decimal("10.00"))]


def test_merchant_uppercased_and_flagged(con, tmp_path):
    _stage(con, tmp_path, [_line(merchant="merch_001")])
    sql = "SELECT merchant_id, merchant_was_lowercase FROM staging.stg_settlements"
    assert _one(con, sql) == ("MERCH_001", True)


def test_sample_csv(con):
    load_raw_settlements(con, SAMPLE_CSV, "run-1")
    assert build_settlement_staging(con) == 500
    assert _one(con, "SELECT sum(dup_count - 1) FROM staging.stg_settlements")[0] == 10
    assert (
        _one(con, "SELECT count(*) FROM staging.stg_settlements WHERE amount_was_br_format")[0]
        == 22
    )
    assert (
        _one(
            con,
            "SELECT count(*) FROM staging.stg_settlements WHERE amount IS NULL OR settled_at IS NULL",
        )[0]
        == 0
    )
    assert _one(con, "SELECT count(*) FROM staging.stg_settlements WHERE settled_at_no_tz")[0] > 0


def test_rebuild_is_idempotent(con, tmp_path):
    _stage(con, tmp_path, [_line(), _line(tid=TID2)])
    assert build_settlement_staging(con) == 2


@pytest.mark.parametrize(
    ("raw", "loss"),
    [("1.234", True), ("1234,567", True), ("10.5", False), ("1.234,56", False), ("R$ 9,99", False)],
)
def test_amount_precision_loss_flag(con, tmp_path, raw, loss):
    _stage(con, tmp_path, [_line(amount=raw)])
    assert _one(con, "SELECT amount_precision_loss FROM staging.stg_settlements")[0] is loss


def test_sample_csv_has_no_precision_loss(con):
    load_raw_settlements(con, SAMPLE_CSV, "run-1")
    build_settlement_staging(con)
    sql = "SELECT count(*) FROM staging.stg_settlements WHERE amount_precision_loss"
    assert _one(con, sql)[0] == 0
