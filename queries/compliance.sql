-- Compliance: trilha de auditoria e histórico.
-- Cada consulta começa com "-- name: <id>"; tests/test_queries.py roda todas.

-- name: compliance_snapshots_per_run
-- Pergunta: quantas linhas de auditoria cada execução gravou? (tabela só acrescentada)
SELECT pipeline_run_id, reference_date, min(audited_at) AS audited_at, count(*) AS rows
FROM marts.audit_reconciliation_history
GROUP BY pipeline_run_id, reference_date
ORDER BY audited_at DESC;

-- name: compliance_transaction_history
-- Pergunta: como a categoria de uma transação mudou ao longo das execuções?
-- PARÂMETROS: altere transaction_id no CTE abaixo.
WITH params AS (SELECT '00000000-0000-0000-0000-000000000000' AS transaction_id)
SELECT h.transaction_id, h.reference_date, h.pipeline_run_id, h.audited_at,
       h.category, h.internal_amount, h.processor_amount, h.difference
FROM marts.audit_reconciliation_history h, params
WHERE h.transaction_id = params.transaction_id
ORDER BY h.audited_at, h.reference_date;

-- name: compliance_as_of
-- Pergunta: qual era o resultado da reconciliação de uma data em um instante passado?
-- Último snapshot de cada transação com audited_at <= as_of.
-- PARÂMETROS: altere ref_date e as_of no CTE abaixo.
-- as_of: troque por um instante passado para ver o relatório como estava naquele momento
-- (audited_at é tempo de sistema, momento da publicação).
WITH params AS (
    SELECT DATE '2025-03-16' AS ref_date, current_timestamp AS as_of
)
SELECT h.reference_date, h.processor, h.transaction_id, h.category,
       h.internal_amount, h.processor_amount, h.difference,
       h.pipeline_run_id, h.audited_at
FROM marts.audit_reconciliation_history h, params
WHERE h.reference_date = params.ref_date
  AND h.audited_at <= params.as_of
QUALIFY row_number() OVER (
    PARTITION BY h.reference_date, h.processor, h.transaction_id
    ORDER BY h.audited_at DESC, h.pipeline_run_id DESC
) = 1
ORDER BY h.transaction_id;

-- name: compliance_category_changes_between_runs
-- Pergunta: quais reprocessamentos de uma data mudaram a categoria de uma transação?
WITH ordered AS (
    SELECT reference_date, processor, transaction_id, pipeline_run_id, audited_at, category,
           lag(category) OVER w AS previous_category
    FROM marts.audit_reconciliation_history
    WINDOW w AS (PARTITION BY reference_date, processor, transaction_id ORDER BY audited_at)
)
SELECT reference_date, processor, transaction_id, previous_category, category, pipeline_run_id
FROM ordered
WHERE previous_category IS NOT NULL AND previous_category <> category
ORDER BY reference_date, transaction_id;

-- name: compliance_merchant_history
-- Pergunta: quais merchants mudaram de cadastro e como era cada versão (SCD2)?
SELECT merchant_id, trade_name, legal_name, primary_cnae, valid_from, valid_to, is_current, op
FROM marts.dim_merchant
WHERE merchant_id IN (SELECT merchant_id FROM marts.dim_merchant
                      GROUP BY merchant_id HAVING count(*) > 1)
ORDER BY merchant_id, valid_from;

-- name: compliance_reversals_and_late_settlements
-- Pergunta: quais liquidações foram revertidas, tardias ou tiveram reversão no CSV?
SELECT reference_date, processor, transaction_id, merchant_id, settlement_status,
       has_reversal, is_late_settlement, settled_date, created_date, processor_amount
FROM marts.fct_reconciliation
WHERE has_reversal OR is_late_settlement OR settlement_status = 'REVERSED'
ORDER BY reference_date DESC, transaction_id;

-- name: compliance_pipeline_runs_per_date
-- Pergunta: quantas vezes cada data foi processada, com que resultado e com qual versão do código?
SELECT reference_date, status, count(*) AS runs, min(started_at) AS first_run,
       max(started_at) AS last_run, max(git_sha) AS git_sha
FROM ops.pipeline_runs
GROUP BY reference_date, status
ORDER BY reference_date DESC, status;
