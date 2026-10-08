-- Estado atual por transaction_id: último registro por _timestamp, desempate por id e,
-- em empate total, pela ordem lógica do ciclo de vida (D > U > I): o delete vem por último.
-- Op = 'D' no último registro => is_deleted (fica fora do matching).
CREATE OR REPLACE TABLE staging.stg_transactions AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY transaction_id ORDER BY CAST(_timestamp AS TIMESTAMPTZ) DESC, id DESC,
            CASE Op WHEN 'D' THEN 3 WHEN 'U' THEN 2 ELSE 1 END DESC
        ) AS _rn,
        -- mais de 1 => replay de CDC (alerta, não bloqueia)
        count(*) FILTER (WHERE Op = 'I') OVER (PARTITION BY transaction_id) AS insert_count
    FROM raw.transactions
)
SELECT
    id,
    transaction_id,
    upper(trim(merchant_id)) AS merchant_id,
    CAST(amount AS DECIMAL(18, 2)) AS amount,
    currency,
    status,
    description,
    payment_method,
    CAST(created_at AS TIMESTAMPTZ) AS created_at,
    CAST(updated_at AS TIMESTAMPTZ) AS updated_at,
    CAST(_timestamp AS TIMESTAMPTZ) AS cdc_timestamp,
    Op AS last_op,
    (Op = 'D') AS is_deleted,
    insert_count,
    _source_file,
    _pipeline_run_id
FROM ranked
WHERE _rn = 1;
