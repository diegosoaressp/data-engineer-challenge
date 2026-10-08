-- SCD2: uma linha por versão do merchant. valid_to é exclusivo; NULL na versão corrente.
-- Versões idênticas repetidas (mesmo merchant_id + _timestamp) viram uma só.
CREATE OR REPLACE TABLE staging.stg_merchants_history AS
WITH versions AS (
    SELECT *, CAST(_timestamp AS TIMESTAMPTZ) AS valid_from
    FROM raw.enterprise_company
    QUALIFY row_number() OVER (PARTITION BY merchant_id, valid_from ORDER BY id DESC) = 1
)
SELECT
    upper(trim(merchant_id)) AS merchant_id,
    legal_name,
    trade_name,
    document,
    primary_cnae,
    valid_from,
    lead(valid_from) OVER (PARTITION BY merchant_id ORDER BY valid_from) AS valid_to,
    lead(valid_from) OVER (PARTITION BY merchant_id ORDER BY valid_from) IS NULL AS is_current,
    Op AS op
FROM versions;
