-- Runs do serviço de reconciliação da reference_date: total declarado x linhas do arquivo
-- (raw.settlements só contém o CSV de D) x resultados gravados.
-- Placeholders: {reference_date}.
CREATE OR REPLACE TEMP TABLE fct_reconciliation_run_stage AS
WITH results AS (
    SELECT run_id, count(*) AS result_rows
    FROM staging.stg_reconciliation_results
    WHERE last_op <> 'D'
    GROUP BY run_id
),
run_rows AS (
    SELECT
        run.id AS run_id,
        run.file_name,
        run.status,
        run.started_at,
        run.completed_at,
        TRY_CAST(run.total_transactions AS BIGINT) AS declared_total,
        (SELECT count(*) FROM raw.settlements) AS file_rows,
        coalesce(res.result_rows, 0) AS result_rows
    FROM staging.stg_reconciliation_runs run
    LEFT JOIN results res ON res.run_id = run.id
    WHERE run.reference_date = DATE '{reference_date}'
)
SELECT
    DATE '{reference_date}' AS reference_date,
    run_id,
    file_name,
    status,
    started_at,
    completed_at,
    CAST(date_diff('second', started_at, completed_at) AS BIGINT) AS duration_seconds,
    declared_total,
    CAST(file_rows AS BIGINT) AS file_rows,
    CAST(result_rows AS BIGINT) AS result_rows,
    declared_total = file_rows AS declared_matches_file,
    result_rows > 0 AS has_results
FROM run_rows;
