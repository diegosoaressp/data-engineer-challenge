-- Estado atual por result id. has_orphan_run: run_id sem run correspondente.
CREATE OR REPLACE TABLE staging.stg_reconciliation_results AS
SELECT
    r.id,
    r.run_id,
    r.transaction_id,
    upper(trim(r.merchant_id)) AS merchant_id,
    r.category,
    CAST(r.internal_amount AS DECIMAL(18, 2)) AS internal_amount,
    CAST(r.processor_amount AS DECIMAL(18, 2)) AS processor_amount,
    CAST(r.difference AS DECIMAL(18, 2)) AS difference,
    CAST(r.created_at AS TIMESTAMPTZ) AS created_at,
    CAST(r._timestamp AS TIMESTAMPTZ) AS cdc_timestamp,
    r.Op AS last_op,
    NOT EXISTS (
        SELECT 1 FROM staging.stg_reconciliation_runs s WHERE s.id = r.run_id
    ) AS has_orphan_run,
    r._source_file,
    r._pipeline_run_id
FROM raw.reconciliation_results r
QUALIFY row_number() OVER (PARTITION BY r.id ORDER BY CAST(r._timestamp AS TIMESTAMPTZ) DESC) = 1;
