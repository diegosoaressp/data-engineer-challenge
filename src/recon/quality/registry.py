from recon.quality.checks import Check

# Lista declarativa. Os checks restantes entram na etapa 12.
CHECKS: tuple[Check, ...] = (
    Check(
        name="settlement_amount_unparseable",
        layer="staging",
        severity="block",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE amount IS NULL",
    ),
    Check(
        name="settlement_identical_duplicates",
        layer="staging",
        severity="alert",
        sql="SELECT transaction_id FROM staging.stg_settlements WHERE dup_count > 1",
    ),
)
