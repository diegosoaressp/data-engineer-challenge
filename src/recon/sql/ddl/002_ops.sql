CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    reference_date  DATE NOT NULL,
    status          VARCHAR NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    duration_ms     BIGINT,
    rows_by_stage   JSON,
    settlement_file VARCHAR,
    git_sha         VARCHAR,
    error           VARCHAR
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id          VARCHAR NOT NULL,
    reference_date  DATE NOT NULL,
    check_name      VARCHAR NOT NULL,
    layer           VARCHAR,
    severity        VARCHAR NOT NULL CHECK (severity IN ('block', 'alert')),
    passed          BOOLEAN NOT NULL,
    observed_value  DOUBLE,
    threshold       DOUBLE,
    sample_keys     VARCHAR,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
