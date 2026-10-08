-- Agregado diário de Ops a partir do stage do fato (mesma fonte, totais batem por construção).
CREATE OR REPLACE TEMP TABLE fct_reconciliation_daily_stage AS
WITH base AS (
    SELECT
        reference_date,
        processor,
        coalesce(currency, 'UNKNOWN') AS currency,
        count(*) AS total_count,
        count(*) FILTER (WHERE category = 'MATCHED') AS matched_count,
        count(*) FILTER (WHERE category = 'MISMATCHED') AS mismatched_count,
        count(*) FILTER (WHERE category = 'UNRECONCILED_PROCESSOR') AS unreconciled_processor_count,
        count(*) FILTER (WHERE category = 'UNRECONCILED_INTERNAL') AS unreconciled_internal_count,
        coalesce(sum(coalesce(processor_amount, internal_amount))
            FILTER (WHERE category = 'MATCHED'), 0) AS matched_amount,
        coalesce(sum(coalesce(processor_amount, internal_amount))
            FILTER (WHERE category = 'MISMATCHED'), 0) AS mismatched_amount,
        coalesce(sum(coalesce(processor_amount, internal_amount))
            FILTER (WHERE category = 'UNRECONCILED_PROCESSOR'), 0) AS unreconciled_processor_amount,
        coalesce(sum(coalesce(processor_amount, internal_amount))
            FILTER (WHERE category = 'UNRECONCILED_INTERNAL'), 0) AS unreconciled_internal_amount,
        coalesce(sum(difference) FILTER (WHERE category = 'MISMATCHED'), 0)
            AS mismatched_difference_amount,
        count(*) FILTER (WHERE settlement_status IS NOT NULL) AS settlement_count,
        count(*) FILTER (WHERE settlement_status = 'REVERSED') AS reversed_count,
        count(*) FILTER (WHERE source_category IS NOT NULL) AS service_compared_count,
        count(*) FILTER (
            WHERE source_category IS NOT NULL AND source_category <> category
        ) AS service_divergence_count
    FROM fct_reconciliation_stage
    GROUP BY reference_date, processor, coalesce(currency, 'UNKNOWN')
)
SELECT
    reference_date,
    processor,
    currency,
    total_count,
    matched_count,
    mismatched_count,
    unreconciled_processor_count,
    unreconciled_internal_count,
    CAST(matched_amount AS DECIMAL(18, 2)) AS matched_amount,
    CAST(mismatched_amount AS DECIMAL(18, 2)) AS mismatched_amount,
    CAST(unreconciled_processor_amount AS DECIMAL(18, 2)) AS unreconciled_processor_amount,
    CAST(unreconciled_internal_amount AS DECIMAL(18, 2)) AS unreconciled_internal_amount,
    CAST(mismatched_difference_amount AS DECIMAL(18, 2)) AS mismatched_difference_amount,
    -- MISMATCHED / (MATCHED + MISMATCHED): só os pares efetivamente comparados; NULL sem pares
    CAST(CAST(mismatched_count AS DOUBLE) / nullif(matched_count + mismatched_count, 0)
        AS DECIMAL(9, 6)) AS mismatch_rate,
    settlement_count,
    reversed_count,
    CASE WHEN settlement_count > 0
        THEN CAST(CAST(reversed_count AS DOUBLE) / settlement_count AS DECIMAL(9, 6)) END
        AS reversed_rate,
    service_compared_count,
    service_divergence_count
FROM base;
