-- Dimensão de merchant (SCD2) a partir do staging. Substituição completa no publish.
CREATE OR REPLACE TEMP TABLE dim_merchant_stage AS
SELECT merchant_id, legal_name, trade_name, document, primary_cnae,
       valid_from, valid_to, is_current, op
FROM staging.stg_merchants_history;
