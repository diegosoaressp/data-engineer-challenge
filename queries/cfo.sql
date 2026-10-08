-- CFO: volume financeiro. Valores em moeda original (sem câmbio): nunca somar moedas.
-- Cada consulta começa com "-- name: <id>"; tests/test_queries.py roda todas.

-- name: cfo_net_settled_by_day
-- Pergunta: quanto foi liquidado, revertido e qual o líquido por dia de liquidação e moeda?
SELECT settled_date, currency,
       sum(settled_amount)  AS settled,
       sum(reversed_amount) AS reversed,
       sum(net_amount)      AS net,
       sum(settled_count)   AS settled_count,
       sum(reversed_count)  AS reversed_count
FROM marts.fct_financial_daily
GROUP BY settled_date, currency
ORDER BY settled_date DESC, currency;

-- name: cfo_monthly_reversal_rate
-- Pergunta: qual a taxa de reversão (em valor) por mês e moeda?
SELECT date_trunc('month', settled_date) AS month, currency,
       sum(settled_amount)  AS settled,
       sum(reversed_amount) AS reversed,
       round(sum(reversed_amount) / nullif(sum(settled_amount), 0), 4) AS reversal_rate
FROM marts.fct_financial_daily
GROUP BY ALL
ORDER BY month DESC, currency;

-- name: cfo_top_merchants_by_net_volume
-- Pergunta: quais os 10 maiores merchants por volume líquido liquidado? (nome vigente em dim_merchant)
SELECT f.currency, f.merchant_id, m.trade_name,
       sum(f.net_amount) AS net
FROM marts.fct_financial_daily f
LEFT JOIN marts.dim_merchant m ON m.merchant_id = f.merchant_id AND m.is_current
GROUP BY f.currency, f.merchant_id, m.trade_name
ORDER BY net DESC
LIMIT 10;

-- name: cfo_internal_volume_by_created_date
-- Pergunta: qual o volume interno por data de criação, moeda e status?
-- Fonte: marts.fct_internal_transactions (uma linha por transação; não usa fct_reconciliation,
-- que repete a transação em várias datas).
SELECT created_date, currency, status,
       count(*)    AS transactions,
       sum(amount) AS amount
FROM marts.fct_internal_transactions
GROUP BY created_date, currency, status
ORDER BY created_date DESC, currency, status;

-- name: cfo_amount_at_risk_by_date
-- Pergunta: quanto valor ficou fora de MATCHED em uma data, por categoria e moeda?
-- PARÂMETROS: altere ref_date no CTE abaixo.
WITH params AS (SELECT DATE '2025-03-16' AS ref_date)
SELECT category, currency,
       count(*) AS transactions,
       sum(coalesce(processor_amount, internal_amount)) AS amount
FROM marts.fct_reconciliation
WHERE reference_date = (SELECT ref_date FROM params)
  AND category <> 'MATCHED'
GROUP BY category, currency
ORDER BY category, currency;

-- name: cfo_internal_vs_settled_by_currency
-- Pergunta: o total interno COMPLETED é compatível com o líquido liquidado, por moeda?
SELECT i.currency, i.internal_completed, s.net_settled
FROM (SELECT currency, sum(amount) AS internal_completed
      FROM marts.fct_internal_transactions
      WHERE status = 'COMPLETED' GROUP BY currency) i
LEFT JOIN (SELECT currency, sum(net_amount) AS net_settled
           FROM marts.fct_financial_daily GROUP BY currency) s USING (currency)
ORDER BY i.currency;
