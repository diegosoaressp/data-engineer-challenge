# CLAUDE.md

## What this repo is

A Senior Data Engineer technical challenge (README is in Portuguese). The repo ships only the scaffolding: sample data, a synthetic data generator and its tests. The deliverable to build is:

1. **Analytical tables** (facts/dimensions) for three consumers: Operations (daily reconciliation health), CFO (financial volume), Compliance (audit trail/history), plus example queries answering the questions each model supports.
2. **A pipeline** that ingests the sources and feeds those tables, meeting: correctness vs. a stated data contract, **idempotency per `reference_date`** (re-runs and mid-run failures must not duplicate/corrupt), observability, data-quality checks (block vs. alert), and automated tests.
3. **Architecture write-ups** (production design, troubleshooting scenario, scaling to ~5M txn/day).

Mandatory stack: Python 3.12+, **DuckDB** for processing, local filesystem storage, must run via `docker compose up`. Final README must fill the "Entrega" sections (how to run, assumptions, architecture, extensibility for a second settlement processor, design limits, production differences, AI tools used). Prefer complete and clean over feature-rich.

Full pipeline design and implementation order: `docs/architecture.md`.

## Commands

The container (`pipeline` service) idles with `tail -f /dev/null`; work is run via `docker compose exec`. Make targets wrap this:

```bash
make up              # docker compose up -d --build
make shell           # bash inside the container
make test            # pytest tests/ -v (inside container)
make generate        # 10k rows -> /tmp/generated (inside container)
make generate-large  # 1M rows -> /tmp/generated-large
make down / make clean
```

Locally (uv, `uv.lock` present):

```bash
uv sync --extra dev
uv run pytest                                                   # all tests
uv run pytest tests/test_generate_sample_data.py::test_seed_is_deterministic   # single test
uv run python scripts/generate_sample_data.py --rows 10000 --days 30 --out /tmp/gen
```

Lint runs in Docker (Windows Smart App Control blocks the local `ruff.exe`):

```bash
docker compose exec pipeline ruff check .
docker compose exec pipeline ruff format .
```

Note: `docker-compose.yml` only mounts `docs/sample-data` (read-only), `scripts/` and `tests/`. Any new source package (e.g. `src/`) or output directory must be added as a volume or the image rebuilt (`make up`) for changes to appear in the container. Files copied into the image (e.g. `pyproject.toml`) only update after a rebuild. `DATA_PATH`, `LOG_LEVEL`, `ENVIRONMENT` env vars are set there (see `.env.example`).

## Source data — key facts

Inputs live in `docs/sample-data/` (~1k internal txns, ~500 settlements). Read `docs/domain-glossary.md` for business rules.

- **Parquet files are Debezium-style CDC extracts**, not table snapshots: every row has `Op` (`I`/`U`/...) and `_timestamp`. Current state must be derived by taking the latest version per key ordered by `_timestamp`. E.g. `reconciliation_runs` has an `I` row (`IN_PROGRESS`) and a `U` row (`COMPLETED`) per run.
- **Schema drift**: `transactions_batch_1.parquet` uses the base schema; `transactions_batch_2.parquet` adds a `payment_method` column (chronological 70/30 split). Use `union_by_name` style reads.
- **PaySettler CSV**: `settlement_paysettler.csv` is the latest day only; the generator also writes per-day files to `<out>/paysettler/settlement_<YYYY-MM-DD>.csv`, matching `reconciliation_runs.file_name`.
- **Planted dirty data** (generator `--dirty`, on by default; `--clean` disables), see `DIRTY_RATES` in `scripts/generate_sample_data.py`: duplicate `transaction_id` from CDC replay, null `merchant_id`, comma-decimal amounts in CSV, `settled_at` missing `Z` suffix, lowercased `merchant_id` in CSV, orphan `REVERSED` rows with no internal pair, late settlements outside the 7-day window.
- Monetary columns are `float64` in the parquet files; cast to DECIMAL when modeling.

## Domain rules (from glossary)

- Match processor ↔ internal on `transaction_id` (UUID).
- Tolerance: |difference| ≤ R$0.01 is `MATCHED`; above is `MISMATCHED`. Other categories: `UNRECONCILED_PROCESSOR` (CSV only), `UNRECONCILED_INTERNAL` (internal only).
- Temporal scope: internal transactions with `created_at` in `[reference_date - 7 days, reference_date]` inclusive, UTC.
- `reference_date` = business day the CSV represents; `started_at`/`completed_at` = system time.
- CSV `status`: `SETTLED` or `REVERSED` (chargeback/refund).

## Tests

`tests/test_generate_sample_data.py` are smoke tests that run the generator as a subprocess into a tmp dir and assert the data contract (files, schema drift, CDC I/U pairs, category mix, dirty patterns, seed determinism). If you change the generator, keep these passing; pipeline tests should be added alongside.

## Conventions
- Code lives in `src/recon/`; add `./src:/app/src` volume to docker-compose.
- Layers: `raw` (CDC as-is) → `staging` (latest per key, typed, cleaned) → `marts` (facts/dims). Single DuckDB file at `$DATA_PATH/warehouse.duckdb`.
- Money: DECIMAL(18,2) from staging onward. Never compare floats.
- Timestamps: UTC everywhere; normalize `settled_at` without `Z`.
- Idempotency: writes scoped by `reference_date` via DELETE+INSERT in a single transaction. Never blind append.
- Data quality: each check declared `block` or `alert`; `block` aborts before writing marts.
- Logging: structured, always including `reference_date` and `run_id`.

## Guardrails
- Never modify `docs/sample-data/` or `scripts/generate_sample_data.py` unless asked.
- Never read or print `.env`.
- New logic requires a test. Done = `uv run pytest` and `docker compose exec pipeline ruff check .` passing.
- After changing `pyproject.toml` or `Dockerfile`, rebuild with `docker compose up -d --build`.
- Multi-file changes: propose a plan first, wait for approval.
- Run `ruff format` only on files you created or changed in the current task, never on the whole repo.