-- Operações: saúde diária da reconciliação.
-- Cada consulta começa com "-- name: <id>"; tests/test_queries.py roda todas.

-- name: ops_daily_health
-- Pergunta: como foi a reconciliação nos últimos N dias, por processor, e qual a taxa de MISMATCHED?
-- A taxa é recalculada a partir das contagens, pois a de fct_reconciliation_daily é por moeda
-- e não se soma.
-- PARÂMETROS: altere ref_date (último dia) e days (tamanho da janela) no CTE abaixo.
WITH params AS (SELECT DATE '2025-03-16' AS ref_date, 14 AS days)
SELECT
    reference_date,
    processor,
    sum(total_count)                                   AS total,
    sum(matched_count)                                 AS matched,
    sum(mismatched_count)                              AS mismatched,
    sum(unreconciled_processor_count)                  AS unreconciled_processor,
    sum(unreconciled_internal_count)                   AS unreconciled_internal,
    round(sum(mismatched_count)
          / nullif(sum(matched_count) + sum(mismatched_count), 0), 4) AS mismatch_rate
FROM marts.fct_reconciliation_daily, params
WHERE reference_date BETWEEN params.ref_date - params.days AND params.ref_date
GROUP BY reference_date, processor
ORDER BY reference_date DESC, processor;

-- name: ops_days_above_mismatch_threshold
-- Pergunta: em quais dias a taxa de MISMATCHED passou de 5% (limite do alerta mismatch_rate_high)?
SELECT reference_date, processor,
       sum(mismatched_count) AS mismatched,
       sum(matched_count) + sum(mismatched_count) AS compared,
       round(sum(mismatched_count)
             / nullif(sum(matched_count) + sum(mismatched_count), 0), 4) AS mismatch_rate
FROM marts.fct_reconciliation_daily
GROUP BY reference_date, processor
HAVING sum(mismatched_count)
       / nullif(sum(matched_count) + sum(mismatched_count), 0) > 0.05
ORDER BY mismatch_rate DESC;

-- name: ops_failed_pipeline_runs
-- Pergunta: quais execuções do pipeline falharam e por quê? (o mart da data fica como estava)
SELECT reference_date, run_id, started_at, error
FROM ops.pipeline_runs
WHERE status = 'FAILED'
ORDER BY started_at DESC;

-- name: ops_open_alerts_latest_run
-- Pergunta: quais alertas de qualidade estão abertos na última execução de cada data?
WITH latest AS (
    SELECT reference_date, run_id,
           row_number() OVER (PARTITION BY reference_date ORDER BY started_at DESC) AS rn
    FROM ops.pipeline_runs
)
SELECT d.reference_date, d.check_name, d.severity, d.observed_value, d.threshold
FROM ops.dq_results d
JOIN latest l ON l.run_id = d.run_id AND l.rn = 1
WHERE NOT d.passed
ORDER BY d.reference_date DESC, d.severity, d.check_name;

-- name: ops_service_divergence
-- Pergunta: onde a categoria recalculada diverge da registrada pelo serviço de reconciliação?
SELECT reference_date, processor,
       sum(service_compared_count)   AS compared,
       sum(service_divergence_count) AS divergent
FROM marts.fct_reconciliation_daily
GROUP BY reference_date, processor
HAVING sum(service_divergence_count) > 0
ORDER BY reference_date DESC;

-- name: ops_service_runs_not_matching_file
-- Pergunta: quais runs do serviço falharam, não têm resultados ou declaram total diferente do arquivo?
SELECT reference_date, run_id, file_name, status, declared_total, file_rows, result_rows
FROM marts.fct_reconciliation_run
WHERE status <> 'COMPLETED' OR NOT has_results OR declared_matches_file IS NOT TRUE
ORDER BY reference_date DESC;
