-- Volume interno: estado atual de cada transação não deletada, direto do staging.
-- Substituição completa no publish (sem escopo por data).
CREATE OR REPLACE TEMP TABLE fct_internal_volume_daily_stage AS
SELECT
    transaction_id,
    merchant_id,
    currency,
    amount,
    status,
    CAST(created_at AT TIME ZONE 'UTC' AS DATE) AS created_date,
    created_at
FROM staging.stg_transactions
WHERE NOT is_deleted;
