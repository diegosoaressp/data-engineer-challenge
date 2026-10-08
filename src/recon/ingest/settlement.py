from datetime import date
from pathlib import Path

import duckdb

SQL_PATH = Path(__file__).parent.parent / "sql" / "raw" / "settlements.sql"


class SettlementFileError(Exception):
    """Arquivo de liquidação da reference_date ausente, ambíguo ou vazio (block)."""


def locate_settlement_file(
    reference_date: date,
    source_dir: Path | None = None,
    settlement_file: Path | None = None,
    processor: str = "paysettler",
) -> Path:
    """Localiza o CSV da `reference_date`. `settlement_file` explícito tem prioridade.

    Senão, procura em `<source_dir>/<processor>/` por `settlement_YYYY-MM-DD.csv`
    (gerador) ou `settlement_YYYYMMDD_*.csv` (reconciliation_runs.file_name).
    """
    if settlement_file is not None:
        if not Path(settlement_file).is_file():
            raise SettlementFileError(f"settlement file not found: {settlement_file}")
        return Path(settlement_file)
    if source_dir is None:
        raise SettlementFileError("no --settlement-file and no --source-dir given")

    folder = Path(source_dir) / processor
    patterns = [
        f"settlement_{reference_date:%Y-%m-%d}.csv",
        f"settlement_{reference_date:%Y%m%d}_*.csv",
    ]
    found = sorted({p for pattern in patterns for p in folder.glob(pattern)})
    if not found:
        raise SettlementFileError(f"no settlement file for {reference_date} in {folder}")
    if len(found) > 1:
        names = ", ".join(p.name for p in found)
        raise SettlementFileError(f"ambiguous settlement files for {reference_date}: {names}")
    return found[0]


def load_raw_settlements(
    con: duckdb.DuckDBPyConnection, path: Path, run_id: str, processor: str = "paysettler"
) -> int:
    """Recria raw.settlements a partir do CSV, todas as colunas em VARCHAR. Retorna a contagem.

    Levanta SettlementFileError quando o arquivo não tem linhas de dados.
    """
    con.execute(SQL_PATH.read_text(encoding="utf-8"), [processor, run_id, str(path)])
    count = con.execute("SELECT count(*) FROM raw.settlements").fetchone()[0]
    if count == 0:
        raise SettlementFileError(f"settlement file is empty: {path}")
    return count
