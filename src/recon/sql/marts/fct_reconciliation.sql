-- Reconciliação recalculada para uma reference_date. Monta o fato em tabela temporária
-- (stage); a publicação nos marts (DELETE + INSERT) é feita depois dos checks block.
-- Placeholders: {reference_date} {processor} {tolerance} {window_days}.
-- Lado interno: todas as não deletadas (o matching não depende da janela). A janela
-- (created_at UTC em [D - window_days, D]) vira in_window e define o escopo de
-- UNRECONCILED_INTERNAL: interna fora da janela sem par no CSV não entra no fato.
CREATE OR REPLACE TEMP TABLE fct_reconciliation_stage AS
WITH internal AS (
    SELECT
        lower(trim(transaction_id)) AS transaction_id,
        merchant_id, upper(trim(currency)) AS currency, amount, status,
        CAST(created_at AT TIME ZONE 'UTC' AS DATE) AS created_date,
        created_at,
        CAST(created_at AT TIME ZONE 'UTC' AS DATE) BETWEEN
            DATE '{reference_date}' - {window_days} AND DATE '{reference_date}' AS in_window
    FROM staging.stg_transactions
    WHERE NOT is_deleted
),
settled AS (
    SELECT * REPLACE (lower(trim(transaction_id)) AS transaction_id)
    FROM staging.stg_settlements WHERE processor = '{processor}'
),
-- categoria calculada pelo serviço para os runs desta data (último resultado por transação)
source AS (
    SELECT lower(trim(res.transaction_id)) AS transaction_id, res.category
    FROM staging.stg_reconciliation_results res
    JOIN staging.stg_reconciliation_runs run ON run.id = res.run_id
    WHERE run.reference_date = DATE '{reference_date}' AND res.last_op <> 'D'
    QUALIFY row_number() OVER (PARTITION BY lower(trim(res.transaction_id)) ORDER BY res.id DESC) = 1
),
joined AS (
    SELECT
        coalesce(s.transaction_id, i.transaction_id) AS transaction_id,
        coalesce(i.merchant_id, s.merchant_id) AS merchant_id,
        coalesce(s.currency, i.currency) AS currency,
        i.currency AS internal_currency,
        i.amount AS internal_amount,
        s.amount AS processor_amount,
        s.status AS settlement_status,
        s.has_reversal,
        i.status AS internal_status,
        s.settled_at,
        i.created_at,
        i.created_date,
        i.in_window,
        (s.currency IS DISTINCT FROM i.currency) AS currency_differs,
        (s.transaction_id IS NULL) AS internal_only,
        (i.transaction_id IS NULL) AS processor_only
    FROM settled s
    FULL OUTER JOIN internal i ON i.transaction_id = s.transaction_id
)
SELECT
    DATE '{reference_date}' AS reference_date,
    '{processor}' AS processor,
    j.transaction_id,
    j.merchant_id,
    j.currency,
    j.internal_currency,
    j.internal_amount,
    j.processor_amount,
    -- sem diferença entre moedas distintas nem com valor ausente
    CASE WHEN NOT j.currency_differs
        THEN abs(j.internal_amount - j.processor_amount) END AS difference,
    CASE WHEN NOT j.currency_differs
        THEN j.processor_amount - j.internal_amount END AS signed_difference,
    -- CASE explícito, sem ELSE: valor ausente deixa category NULL (barrado pelo NOT NULL
    -- do mart e pelos checks block), nunca cai em MISMATCHED
    CASE
        WHEN j.processor_only THEN 'UNRECONCILED_PROCESSOR'
        WHEN j.internal_only THEN 'UNRECONCILED_INTERNAL'
        WHEN j.currency_differs THEN 'MISMATCHED'
        WHEN j.internal_amount IS NULL OR j.processor_amount IS NULL THEN NULL
        WHEN abs(j.internal_amount - j.processor_amount) <= {tolerance} THEN 'MATCHED'
        WHEN abs(j.internal_amount - j.processor_amount) > {tolerance} THEN 'MISMATCHED'
    END AS category,
    CASE
        WHEN j.processor_only OR j.internal_only THEN NULL
        WHEN j.currency_differs THEN 'CURRENCY'
        WHEN abs(j.internal_amount - j.processor_amount) > {tolerance} THEN 'AMOUNT'
    END AS mismatch_reason,
    j.settlement_status,
    j.has_reversal,
    j.internal_status,
    -- por data, coerente com a janela; FALSE quando não há os dois lados
    coalesce(
        (CAST(j.settled_at AT TIME ZONE 'UTC' AS DATE) - j.created_date) > {window_days}, false
    ) AS is_late_settlement,
    CAST(j.settled_at AT TIME ZONE 'UTC' AS DATE) AS settled_date,
    j.created_date,
    j.in_window,
    src.category AS source_category
FROM joined j
LEFT JOIN source src ON src.transaction_id = j.transaction_id
-- interna fora da janela sem par no CSV está fora de escopo
WHERE NOT (j.internal_only AND NOT j.in_window);
