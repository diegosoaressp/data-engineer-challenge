-- Compliance: snapshot de cada execução, SÓ ACRESCENTADO (nunca DELETE/UPDATE). Cada run grava
-- as linhas de fct_reconciliation que publicou, na mesma transação do publish: run que falha
-- ou sofre rollback não deixa snapshot. Reprocessar a mesma reference_date gera novo
-- pipeline_run_id, preservando o histórico de como o resultado mudou.
CREATE TABLE IF NOT EXISTS marts.audit_reconciliation_history (
    pipeline_run_id      VARCHAR NOT NULL,
    reference_date       DATE NOT NULL,
    transaction_id       VARCHAR NOT NULL,
    processor            VARCHAR NOT NULL,
    audited_at           TIMESTAMPTZ NOT NULL,
    merchant_id          VARCHAR,
    currency             VARCHAR,
    internal_currency    VARCHAR,
    internal_amount      DECIMAL(18, 2),
    processor_amount     DECIMAL(18, 2),
    difference           DECIMAL(18, 2),
    signed_difference    DECIMAL(18, 2),
    category             VARCHAR NOT NULL,
    mismatch_reason      VARCHAR,
    settlement_status    VARCHAR,
    has_reversal         BOOLEAN,
    internal_status      VARCHAR,
    is_late_settlement   BOOLEAN,
    settled_date         DATE,
    created_date         DATE,
    in_window            BOOLEAN,
    source_category      VARCHAR,
    PRIMARY KEY (pipeline_run_id, reference_date, processor, transaction_id)
);
