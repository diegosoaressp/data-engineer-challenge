import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

from recon import cli, config

REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATOR = REPO_ROOT / "scripts" / "generate_sample_data.py"


def csv_days(source_dir: Path) -> list[date]:
    """Dias com CSV diário gerado em `<source_dir>/paysettler/`, em ordem."""
    return sorted(
        date.fromisoformat(p.stem.removeprefix("settlement_"))
        for p in (source_dir / "paysettler").glob("settlement_*.csv")
    )


def backfill_range(source_dir: Path) -> tuple[date, date]:
    """Primeiro e último dia com CSV diário."""
    days = csv_days(source_dir)
    return days[0], days[-1]


def days_without_csv(source_dir: Path) -> list[date]:
    """Dias dentro do intervalo do backfill sem CSV (o gerador não escreve arquivo vazio)."""
    start, end = backfill_range(source_dir)
    have = set(csv_days(source_dir))
    return [start + timedelta(days=i) for i in range((end - start).days + 1)
            if start + timedelta(days=i) not in have]  # fmt: skip


def run_backfill(source_dir: Path, data_path: Path) -> int:
    """`recon backfill` do primeiro ao último CSV diário, gravando em `data_path`.

    Retorna 1 quando alguma data falha; dias sem CSV falham por design (block).
    """
    start, end = backfill_range(source_dir)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(config, "DATA_PATH", data_path)
        return cli.main(
            [
                "backfill",
                "--start", start.isoformat(),
                "--end", end.isoformat(),
                "--source-dir", str(source_dir),
            ]
        )  # fmt: skip


def generate_dataset(
    out: Path, rows: int, days: int, merchants: int, seed: int, clean: bool = False
) -> Path:
    """Roda o gerador em `out` (parquets, CSVs diários e gabarito reconciliation_results)."""
    cmd = [
        sys.executable, str(GENERATOR),
        "--rows", str(rows),
        "--days", str(days),
        "--merchants", str(merchants),
        "--seed", str(seed),
        "--out", str(out),
    ]  # fmt: skip
    if clean:
        cmd.append("--clean")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT, capture_output=True)
    return out


@pytest.fixture(scope="session")
def generated_source(tmp_path_factory) -> Path:
    """Dataset do gerador (10k linhas, 30 dias, CSVs diários), gerado uma vez por sessão."""
    return generate_dataset(tmp_path_factory.mktemp("generated"), 10_000, 30, 100, 42)


@pytest.fixture(scope="session")
def backfilled_warehouse(generated_source, tmp_path_factory) -> Path:
    """Diretório DATA_PATH com o warehouse após `recon backfill` sobre o dataset gerado."""
    data_path = tmp_path_factory.mktemp("backfilled")
    expected_exit = 1 if days_without_csv(generated_source) else 0
    assert run_backfill(generated_source, data_path) == expected_exit
    return data_path
