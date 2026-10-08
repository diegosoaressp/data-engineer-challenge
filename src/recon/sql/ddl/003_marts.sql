CREATE TABLE IF NOT EXISTS marts.fct_reconciliation (
    reference_date       DATE NOT NULL,
    processor            VARCHAR NOT NULL,
    transaction_id       VARCHAR NOT NULL,
    merchant_id          VARCHAR,
    currency             VARCHAR,
    internal_currency    VARCHAR,
    internal_amount      DECIMAL(18, 2),
    processor_amount     DECIMAL(18, 2),
    difference           DECIMAL(18, 2),
    signed_difference    DECIMAL(18, 2),
    category             VARCHAR NOT NULL CHECK (category IN (
        'MATCHED', 'MISMATCHED', 'UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')),
    mismatch_reason      VARCHAR CHECK (mismatch_reason IN ('AMOUNT', 'CURRENCY')),
    settlement_status    VARCHAR,
    has_reversal         BOOLEAN,
    internal_status      VARCHAR,
    is_late_settlement   BOOLEAN,
    settled_date         DATE,
    created_date         DATE,
    in_window            BOOLEAN,
    source_category      VARCHAR,
    PRIMARY KEY (reference_date, processor, transaction_id)
);
