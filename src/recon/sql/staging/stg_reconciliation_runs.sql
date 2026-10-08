-- Estado atual por run id (I -> U).
CREATE OR REPLACE TABLE staging.stg_reconciliation_runs AS
SELECT
    id,
    CAST(reference_date AS DATE) AS reference_date,
    file_name,
    status,
    total_transactions,
    CAST(started_at AS TIMESTAMPTZ) AS started_at,
    CAST(completed_at AS TIMESTAMPTZ) AS completed_at,
    CAST(created_at AS TIMESTAMPTZ) AS created_at,
    CAST(_timestamp AS TIMESTAMPTZ) AS cdc_timestamp,
    Op AS last_op,
    _source_file,
    _pipeline_run_id
FROM raw.reconciliation_runs
QUALIFY row_number() OVER (PARTITION BY id ORDER BY CAST(_timestamp AS TIMESTAMPTZ) DESC) = 1;
