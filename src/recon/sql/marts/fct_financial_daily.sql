-- Liquidações (CFO) a partir do stage do fato: só linhas com lado processor, por data de
-- liquidação, em moeda original. O volume interno vem de fct_internal_transactions.
CREATE OR REPLACE TEMP TABLE fct_financial_daily_stage AS
SELECT
    reference_date,
    processor,
    settled_date,
    coalesce(currency, 'UNKNOWN') AS currency,
    coalesce(merchant_id, 'UNKNOWN') AS merchant_id,
    CAST(coalesce(sum(processor_amount) FILTER (WHERE settlement_status = 'SETTLED'), 0)
        AS DECIMAL(18, 2)) AS settled_amount,
    CAST(coalesce(sum(processor_amount) FILTER (WHERE settlement_status = 'REVERSED'), 0)
        AS DECIMAL(18, 2)) AS reversed_amount,
    CAST(coalesce(sum(processor_amount) FILTER (WHERE settlement_status = 'SETTLED'), 0)
        - coalesce(sum(processor_amount) FILTER (WHERE settlement_status = 'REVERSED'), 0)
        AS DECIMAL(18, 2)) AS net_amount,
    count(*) FILTER (WHERE settlement_status = 'SETTLED') AS settled_count,
    count(*) FILTER (WHERE settlement_status = 'REVERSED') AS reversed_count
FROM fct_reconciliation_stage
WHERE settlement_status IS NOT NULL
GROUP BY reference_date, processor, settled_date, coalesce(currency, 'UNKNOWN'),
         coalesce(merchant_id, 'UNKNOWN');
