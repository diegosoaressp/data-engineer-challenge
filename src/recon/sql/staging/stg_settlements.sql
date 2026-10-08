-- Uma linha por (processor, transaction_id), tipada e limpa.
-- Valor: tira prefixo R$ e espaços; havendo vírgula, ponto = milhar e vírgula = decimal.
-- settled_at sem fuso é UTC. Valor/data não interpretáveis viram NULL (checagem block depois).
-- Linhas idênticas colapsam em dup_count; versões distintas: fica a última por settled_at,
-- com version_count e has_reversal.
CREATE OR REPLACE TABLE staging.stg_settlements AS
WITH cleaned AS (
    SELECT
        processor,
        transaction_id,
        merchant_id AS merchant_id_raw,
        upper(trim(merchant_id)) AS merchant_id,
        upper(trim(currency)) AS currency,
        upper(trim(status)) AS status,
        processor_reference,
        regexp_replace(trim(amount), '^R\$\s*', '') AS amount_txt,
        trim(amount) AS amount_raw,
        TRY_CAST(
            CASE WHEN contains(regexp_replace(trim(amount), '^R\$\s*', ''), ',')
                THEN replace(replace(regexp_replace(trim(amount), '^R\$\s*', ''), '.', ''), ',', '.')
                ELSE regexp_replace(trim(amount), '^R\$\s*', '')
            END AS DECIMAL(28, 8)
        ) AS amount_precise,
        trim(settled_at) AS settled_txt,
        _source_file,
        _pipeline_run_id
    FROM raw.settlements
),
parsed AS (
    SELECT
        processor, transaction_id, merchant_id, currency, status, processor_reference,
        _source_file, _pipeline_run_id,
        amount_precise,
        CAST(amount_precise AS DECIMAL(18, 2)) AS amount,
        -- true: o valor normalizado tinha mais de 2 casas e foi arredondado (ex.: "1.234")
        amount_precise <> CAST(amount_precise AS DECIMAL(18, 2)) AS amount_precision_loss,
        TRY_CAST(
            CASE WHEN regexp_matches(settled_txt, '(Z|[+-][0-9]{2}:?[0-9]{2})$')
                THEN settled_txt
                ELSE settled_txt || 'Z'
            END AS TIMESTAMPTZ
        ) AS settled_at,
        amount_raw <> amount_txt OR contains(amount_txt, ',') AS amount_was_br_format,
        NOT regexp_matches(settled_txt, '(Z|[+-][0-9]{2}:?[0-9]{2})$') AS settled_at_no_tz,
        merchant_id_raw <> upper(merchant_id_raw) AS merchant_was_lowercase
    FROM cleaned
),
versions AS (
    -- colapsa linhas idênticas (mesmo conteúdo normalizado) em uma versão
    SELECT
        processor, transaction_id, merchant_id, currency, status, processor_reference,
        amount, settled_at,
        count(*) AS dup_count,
        bool_or(amount_precision_loss) AS amount_precision_loss,
        bool_or(amount_was_br_format) AS amount_was_br_format,
        bool_or(settled_at_no_tz) AS settled_at_no_tz,
        bool_or(merchant_was_lowercase) AS merchant_was_lowercase,
        any_value(_source_file) AS _source_file,
        any_value(_pipeline_run_id) AS _pipeline_run_id
    FROM parsed
    GROUP BY processor, transaction_id, merchant_id, currency, status, processor_reference,
             amount, settled_at
),
ranked AS (
    SELECT
        *,
        count(*) OVER (PARTITION BY processor, transaction_id) AS version_count,
        bool_or(status = 'REVERSED') OVER (PARTITION BY processor, transaction_id) AS has_reversal,
        row_number() OVER (
            PARTITION BY processor, transaction_id
            ORDER BY settled_at DESC NULLS LAST, (status = 'REVERSED') DESC,
                     processor_reference DESC NULLS LAST, amount DESC NULLS LAST
        ) AS _rn
    FROM versions
)
SELECT
    * EXCLUDE (_rn),
    CASE status WHEN 'REVERSED' THEN -amount ELSE amount END AS signed_amount
FROM ranked
WHERE _rn = 1;
