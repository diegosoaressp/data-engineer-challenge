-- Dimensão (recriada a cada run a partir do staging, SCD2).
CREATE TABLE IF NOT EXISTS marts.dim_merchant (
    merchant_id   VARCHAR NOT NULL,
    legal_name    VARCHAR,
    trade_name    VARCHAR,
    document      VARCHAR,
    primary_cnae  VARCHAR,
    valid_from    TIMESTAMPTZ NOT NULL,
    valid_to      TIMESTAMPTZ,
    is_current    BOOLEAN NOT NULL,
    op            VARCHAR,
    PRIMARY KEY (merchant_id, valid_from)
);

-- Ops: saúde diária. Grão com currency porque não há câmbio: valores de moedas diferentes
-- não se somam. Contagens e taxas são aditivas entre moedas (recalcular a taxa a partir
-- das contagens). Alertas do run ficam em ops.dq_results.
CREATE TABLE IF NOT EXISTS marts.fct_reconciliation_daily (
    reference_date                  DATE NOT NULL,
    processor                       VARCHAR NOT NULL,
    currency                        VARCHAR NOT NULL,
    total_count                     BIGINT NOT NULL,
    matched_count                   BIGINT NOT NULL,
    mismatched_count                BIGINT NOT NULL,
    unreconciled_processor_count    BIGINT NOT NULL,
    unreconciled_internal_count     BIGINT NOT NULL,
    -- valor em jogo: processor_amount quando existe, senão internal_amount
    matched_amount                  DECIMAL(18, 2) NOT NULL,
    mismatched_amount               DECIMAL(18, 2) NOT NULL,
    unreconciled_processor_amount   DECIMAL(18, 2) NOT NULL,
    unreconciled_internal_amount    DECIMAL(18, 2) NOT NULL,
    mismatched_difference_amount    DECIMAL(18, 2) NOT NULL,
    mismatch_rate                   DECIMAL(9, 6) NOT NULL,
    settlement_count                BIGINT NOT NULL,
    reversed_count                  BIGINT NOT NULL,
    reversed_rate                   DECIMAL(9, 6),
    service_compared_count          BIGINT NOT NULL,
    service_divergence_count        BIGINT NOT NULL,
    PRIMARY KEY (reference_date, processor, currency)
);

-- Ops / Compliance: um run do serviço por linha, escopo por reference_date.
CREATE TABLE IF NOT EXISTS marts.fct_reconciliation_run (
    reference_date          DATE NOT NULL,
    run_id                  BIGINT NOT NULL,
    file_name               VARCHAR,
    status                  VARCHAR,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    duration_seconds        BIGINT,
    declared_total          BIGINT,
    file_rows               BIGINT NOT NULL,
    result_rows             BIGINT NOT NULL,
    declared_matches_file   BOOLEAN,
    has_results             BOOLEAN NOT NULL,
    PRIMARY KEY (reference_date, run_id)
);

-- CFO: liquidações (SETTLED/REVERSED) por data de liquidação, em moeda original (sem câmbio).
-- Escopo por reference_date e processor: cada liquidação vem de um único CSV, sem repetição
-- entre datas. O volume interno fica em fct_internal_volume_daily.
CREATE TABLE IF NOT EXISTS marts.fct_financial_daily (
    reference_date    DATE NOT NULL,
    processor         VARCHAR NOT NULL,
    settled_date      DATE NOT NULL,
    currency          VARCHAR NOT NULL,
    merchant_id       VARCHAR NOT NULL,
    settled_amount    DECIMAL(18, 2) NOT NULL,
    reversed_amount   DECIMAL(18, 2) NOT NULL,
    net_amount        DECIMAL(18, 2) NOT NULL,
    settled_count     BIGINT NOT NULL,
    reversed_count    BIGINT NOT NULL,
    PRIMARY KEY (reference_date, processor, settled_date, currency, merchant_id)
);

-- CFO: volume interno, uma linha por transação não deletada (estado atual do staging).
-- Reconstrução total a cada run: não depende de reference_date nem da janela, então a mesma
-- transação nunca é contada duas vezes. Agregar por created_date/currency/status nas queries.
CREATE TABLE IF NOT EXISTS marts.fct_internal_volume_daily (
    transaction_id  VARCHAR PRIMARY KEY,
    merchant_id     VARCHAR,
    currency        VARCHAR,
    amount          DECIMAL(18, 2),
    status          VARCHAR,
    created_date    DATE,
    created_at      TIMESTAMPTZ
);
