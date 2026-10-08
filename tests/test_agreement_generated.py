"""Concordância linha a linha entre a categoria recalculada e o gabarito do gerador.

O gerador grava em `reconciliation_results` a categoria que atribuiu a cada transação. O
pipeline recalcula a categoria só a partir dos CSVs e das transações; as duas têm de bater
para 100% das transações, com dados limpos e sujos. Os REVERSED órfãos plantados não têm
linha no gabarito e ficam fora da comparação.
"""

import pytest
from conftest import days_without_csv, generate_dataset, run_backfill

from recon import config
from recon.db import connect

ROWS = 2_000

# Uma categoria final por transação: a da linha com lado processador (MATCHED, MISMATCHED,
# UNRECONCILED_PROCESSOR); UNRECONCILED_INTERNAL só se a transação nunca foi liquidada.
# (Interna na janela sem CSV em D vira UNRECONCILED_INTERNAL até a liquidação chegar.)
AGREEMENT_SQL = """
WITH ours AS (
    SELECT transaction_id,
           coalesce(
               max(category) FILTER (WHERE category <> 'UNRECONCILED_INTERNAL'),
               'UNRECONCILED_INTERNAL'
           ) AS category
    FROM marts.fct_reconciliation
    GROUP BY transaction_id
),
expected AS (
    SELECT transaction_id, category FROM raw.reconciliation_results WHERE "Op" = 'I'
)
SELECT e.category, coalesce(o.category, '<ausente>'), count(*)
FROM expected e LEFT JOIN ours o USING (transaction_id)
GROUP BY 1, 2
ORDER BY 1, 2
"""


@pytest.mark.parametrize("clean", [True, False], ids=["clean", "dirty"])
def test_recalculated_category_agrees_with_generator_ground_truth(tmp_path, clean):
    source = generate_dataset(tmp_path / "src", ROWS, days=10, merchants=50, seed=7, clean=clean)
    data_path = tmp_path / "data"
    expected_exit = 1 if days_without_csv(source) else 0  # dias sem CSV são block por design
    assert run_backfill(source, data_path) == expected_exit

    con = connect(data_path / config.WAREHOUSE_FILE)
    try:
        pairs = con.execute(AGREEMENT_SQL).fetchall()
    finally:
        con.close()

    divergences = [(exp, got, n) for exp, got, n in pairs if exp != got]
    assert sum(n for _, _, n in pairs) == ROWS
    assert divergences == []
