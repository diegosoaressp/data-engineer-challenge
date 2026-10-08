import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from recon.db import connect
from recon.pipeline import run_pipeline

ROOT = Path(__file__).parent.parent
# 3 reference_dates consecutivas: as janelas de 7 dias se sobrepõem
DATES = [date(2025, 3, 5) + timedelta(days=i) for i in range(3)]


@pytest.fixture(scope="module")
def gen_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen")
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "generate_sample_data.py"),
            "--rows",
            "600",
            "--days",
            "10",
            "--clean",
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
    )
    return out


def test_volumes_are_not_counted_once_per_reference_date(gen_dir, tmp_path):
    """Aceitação do risco de contagem dupla: com um CSV DIFERENTE por reference_date e
    janelas sobrepostas, o interno aparece uma vez por transação e as liquidações somam
    exatamente o que os CSVs processados contêm."""
    con = connect(tmp_path / "wh.duckdb")
    try:
        for ref in DATES:
            run_pipeline(ref, source_dir=gen_dir, con=con, processor="paysettler")

        internal = con.execute(
            "SELECT sum(amount), count(*), count(DISTINCT transaction_id) "
            "FROM marts.fct_internal_volume_daily"
        ).fetchone()
        staging = con.execute(
            "SELECT sum(amount), count(*) FROM staging.stg_transactions WHERE NOT is_deleted"
        ).fetchone()
        assert internal[0] == staging[0]
        assert internal[1] == internal[2] == staging[1]

        files = ", ".join(
            f"'{(gen_dir / 'paysettler' / f'settlement_{d.isoformat()}.csv').as_posix()}'"
            for d in DATES
        )
        csv = con.execute(
            "SELECT "
            "coalesce(sum(CAST(amount AS DECIMAL(18, 2))) FILTER (WHERE status = 'SETTLED'), 0), "
            "coalesce(sum(CAST(amount AS DECIMAL(18, 2))) FILTER (WHERE status = 'REVERSED'), 0) "
            f"FROM read_csv([{files}], all_varchar = true)"
        ).fetchone()
        financial = con.execute(
            "SELECT sum(settled_amount), sum(reversed_amount) FROM marts.fct_financial_daily"
        ).fetchone()
        assert csv[0] > Decimal(0)
        assert financial == csv
    finally:
        con.close()
