# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A bank statement analyzer (BSA) pipeline: PDF bank statements in a Unity Catalog volume are parsed,
classified, validated, and aggregated into a Gold feature table. Runs as a Databricks Workflow on
**serverless compute** — this constrains some code patterns (see Gotchas below). All files are
Databricks notebooks (`.py` with `# COMMAND ----------` cell markers), not plain scripts.

## Pipeline stages and execution order

Each notebook is a separate task in a Databricks Job. Every task except the last has
`run_if: All succeeded` on its upstream task; `05_sync_pipeline_status` has `run_if: All done`, so it
always runs and records both success and failure outcomes for the whole run.

1. **`pdf_extraction.py`** — Auto Loader (`cloudFiles`, `trigger(availableNow=True)`) streams new PDFs
   from `RAW_VOLUME_PATH`, extracts text/tables via `pdfplumber`, insert-only merges into Bronze
   (`bsa_statement_extraction_raw`).
2. **`bronze_backfill.py`** — batch reprocess of statements already in Bronze that are eligible for
   re-extraction (new `STAGE_LOGIC_VERSIONS` or a failed attempt under the retry cap). Exists because
   Auto Loader's checkpoint never re-hands-back a file once consumed, so retries can't go through the
   streaming path — this notebook re-reads `source_path` directly instead.
3. **`transaction_classification.py`** — parses transactions (table-based or regex fallback) from
   Bronze rows routed to CPU, classifies `txn_type`/direction, writes to Silver
   (`bsa_classified_transactions`).
4. **`validation_checksum.py`** — reconciles running balance vs. prior balance ± debit/credit per
   statement, tags each `PASSED` / `PARTIAL` / `FAILED` / `UNVERIFIED`.
5. **`merge_feature_engineering.py`** — aggregates statements with validation status `PASSED` or
   `PARTIAL` into the Gold table `bsa_account_features`.
6. **`05_sync_pipeline_status.py`** — calls the Databricks Jobs API
   (`/api/2.1/jobs/runs/get`) to pull per-task result_state/duration and merges them into
   `bsa_pipeline_log`, keyed by `run_id`.

## `00_config.py` — shared module, `%run` at the top of every notebook

Not a standalone stage — holds config, table name constants, and the retry/versioning framework every
stage depends on:

- **`STAGE_LOGIC_VERSIONS`** — bump a stage's version here after a real logic fix. Any statement
  logged under an older version becomes automatically eligible for reprocessing at that stage — no
  manual backfill script needed.
- **`RETRY_CAP`** (default 3) — a statement that fails a stage this many times flips to
  `NEEDS_REVIEW` and stops being retried automatically. A `STAGE_LOGIC_VERSIONS` bump overrides this
  and brings it back into eligibility regardless of retry history.
- **`get_eligible_statements(stage, candidates_df)`** — the single source of truth every stage queries
  to decide what to (re)process, reading `bsa_pipeline_log`.
- **`log_pipeline_stage(spark, stage, results_df)`** — upserts status/duration/error/attempt_count/
  logic_version for a stage via a Delta MERGE.
- **`invalidate_downstream(stage, statement_hashes_df)`** — one-hop cascade: after a stage rewrites
  real output for some statements, nulls out the *next* stage's status for them so that stage's own
  eligibility check naturally picks them back up. No-op for the last stage.
- **`extract_pdfs()` / `EXTRACTION_SCHEMA`** — shared `pdfplumber`-based extraction logic used by both
  `pdf_extraction.py` and `bronze_backfill.py` so they never drift apart.
- `pdfplumber` is deliberately imported inside `extract_pdfs()`, not at module level, so notebooks that
  never touch a PDF (classification, validation, merge, sync) don't need it installed.

## Gotchas

- **No `.cache()`/`.persist()` on serverless.** Every stage materializes its `mapInPandas` output to a
  `*_staging` Delta table (`TBL_*_STAGING`) first, then reads it back — this guarantees the expensive
  per-row Python logic (PDF parsing, classification, validation) runs exactly once per batch, not once
  per downstream action.
- **`ensure_table_columns(table_fqn, ddl_columns)`** must be called for every table whose schema might
  grow — `CREATE TABLE IF NOT EXISTS` is a no-op once a table exists, so DDL text edits alone never
  reach a live table.
- Adding a new task to the Databricks Job requires updating `TASK_KEY_TO_PREFIX` in
  `05_sync_pipeline_status.py` to match the task_key exactly as typed in the Workflow UI.
- **No automated test suite or formal validation process.** Changes are typically run interactively in
  a Databricks notebook against a dev catalog/schema before being trusted.
- This is not a git-native project layout — no subfolders, no package manifest; each `.py` file is a
  full Databricks notebook meant to be run via `%run` chaining or as Job tasks, not imported as a
  Python module.
