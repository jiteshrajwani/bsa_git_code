import uuid as _uuid

dbutils.widgets.text('run_id',"","Databricks Job run_id (auto-generated if blank)")
_run_id_param = dbutils.widgets.get('run_id').strip()
RUN_ID = _run_id_param if _run_id_param else f"interactive-{_uuid.uuid4().hex[:12] }"
BATCH_ID = RUN_ID

dbutils.widgets.text('job_start_time_ms',"","Job Run Start Time in ms since epcoh (auto from job)")
_job_start_param = dbutils.widgets.get("job_start_time_ms").strip()
JOB_START_TIME_MS = int(_job_start_param) if _job_start_param.isdigit() else None

dbutils.widgets.text("catalog", "edp_bfil_prod", "Unity Catalog name")
dbutils.widgets.text("bronze_schema","analytics_team","Bronze schema")
dbutils.widgets.text("silver_schema","analytics_team","Silver schema")
dbutils.widgets.text("gold_schema","analytics_team","Gold schema")
dbutils.widgets.text("raw_volumne_path","/Volumes/edp_bfil_prod/analytics_team/bsa_raw_statements/incoming","Raw PDF volumne path")

dbutils.widgets.text("chekpoint_volumne_path","/Volumes/edp_bfil_prod/analytics_team/bsa_raw_statements/_checkpoints","Auto Loader checkpoint path")

CATALOG = dbutils.widgets.get('catalog')
BRONZE_SCHEMA = dbutils.widgets.get('bronze_schema')
SILVER_SCHEMA = dbutils.widgets.get('silver_schema') 
GOLD_SCHEMA = dbutils.widgets.get('gold_schema')
RAW_VOLUME_PATH = dbutils.widgets.get('raw_volumne_path')
CHECKPOINT_VOLUME_PATH = dbutils.widgets.get("chekpoint_volumne_path")

# Fully qualified table names used across all stage notebooks
TBL_BRONZE_EXTRACTION = f"{CATALOG}.{BRONZE_SCHEMA}.bsa_statement_extraction_raw"
TBL_SILVER_TRANSACTIONS = f"{CATALOG}.{SILVER_SCHEMA}.bsa_classified_transactions"
TBL_SILVER_VALIDATED = f"{CATALOG}.{SILVER_SCHEMA}.bsa_validated_transactions"
TBL_GOLD_ACCOUNT_FEATURES = f"{CATALOG}.{GOLD_SCHEMA}.bsa_account_features"
TBL_BRONZE_STAGING = f"{CATALOG}.{BRONZE_SCHEMA}.bsa_extraction_staging"
TBL_SILVER_STAGING = f"{CATALOG}.{SILVER_SCHEMA}.bsa_classified_staging"
TBL_SILVER_VALIDATED_STAGING = f"{CATALOG}.{SILVER_SCHEMA}.bsa_validation_staging"
TBL_GOLD_STAGING = f"{CATALOG}.{GOLD_SCHEMA}.bsa_features_staging"


EXTRACTION_CONFIDENCE_THRESHOLD = 0.60

BANK_KEYWORDS = {
    "ICICI" : ['icici bank','icicibank.com'],
    "HDFC" : ['hdfc bank','hdfcbank.com'],
    "SBI" : ['state bank of india','sbi.co.in'],
    "AXIS" : ['axis bank','aixsbank.com'],
    "KOTAK" : ['kotak mahindra','kotak.com'],
    "INDUSIND" : ['indusind bank','indusind.com'],
    "PNB" : ['punjab national bank'],
    "BOB" : ['bank of baroda']
}

DATE_PATTERNS = [
    r"\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}",
    r"\d{1,2}[-/.\s][A-Za-z]{3,9}[-/.\s]\d{2,4}",
    r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}"
]

TRANSACTION_TYPE_RULES = [
    ('UPI',     r"\bupi[/\-]"),
    ('NEFT',     r"\bneft[/\-]"),
    ('RTGS',     r"\brtgs[/\-]"),
    ('IMPS',     r"\bimps[/\-]"),
    ('FUND_TRANSFER',     r"\btrf\b"),
    ('ATM_WITHDRAWL',     r"\batm[\s/\-]?(wdl|withdrawl|cash|cwd|nwd)\b"),
    ('POS_PURCHASE',     r"\b(pos|pur|purchase)[\s/\-]"),
    ('CHEQUE',     r"\bch(q|eque)[\s/\-]|\bclg\b|\bbrn[\-/]clg\b"),
    ('ATM_WITHDRAWL',     r"\batm[\s/\-]?(wdl|withdrawl|cash|cwd|nwd)\b"),
    ('POS_PURCHASE',     r"\b(pos|pur|purchase)[\s/\-]"),
    ('CHEQUE',     r"\bch(q|eque)[\s/\-]|\bclg\b|\bbrn[\-/]clg\b"),
    ('SALARY_CREDIT',     r"\bsal(ary)?[\s/\-]?cr\b"),
    ('INTEREST',     r"\bint(erest)?\s?(cr|paid|earned|coll)\b"),
    ('BANK_CHARGES',     r"\b(charges|chrg|fee|pentalty|amb|sms\s?chg)\b"),
    ('REVERSAL',     r"\brev(ersal)?\b|\breturn(ed)?\b"),
    ('EMI',     r"\bemi\b|\bloan\s?(inst|disb)\b"),
    ("CASH_DEPOSIT",       r"\bcdm\b|\bcash\s?dep(osit)?\b|\bbna\b" )    ,
    ("ECS_NACH",  r"\becs\b|\bnach\b|\bmandade\b"),
    ("BILL_PAYMENT", r"\bbil[l]?[/\-]|\bbillpay\b|\becom\b "),
    ("TAX_GST",  r"\bgst\b|\btds\b" ),
    ("MOBILE_BANKING",  r"\bmob(ile)?[\s/\-]?bk?g?\b|\bib[\s/\-]?transfer\b" ),
    ('SWEEP', r"\bsweep\b|\bmod\b" )
]

from delta.tables import DeltaTable
from pyspark.sql.functions import lit as _lit, current_timestamp as _current_timestamp
from pyspark.sql import functions as F
from pyspark.sql import Row


# ============================================================
# RETRY / REPROCESSING POLICY
# ============================================================
# Bump a stage's number here whenever you ship a real logic fix for that
# stage. Every statement logged under an older version automatically
# becomes eligible for reprocessing at that stage again -- no manual hash
# deletion, no separate backfill script to remember to run.
STAGE_LOGIC_VERSIONS = {
    "pdf_extraction": 1,
    "classification": 5,
    "validation": 1,
    "merge": 1,
}

# A statement stops being auto-retried after this many FAILED attempts at a
# given stage and flips to NEEDS_REVIEW instead -- still visible and
# queryable, just no longer silently burning compute every run for
# something that isn't going to fix itself. A STAGE_LOGIC_VERSIONS bump
# brings it back into eligibility regardless of this cap, since that
# represents an actual fix, not a blind retry.
RETRY_CAP = 3

_STAGE_ORDER = ["pdf_extraction", "classification", "validation", "merge"]


TBL_PIPELINE_LOG = f"{CATALOG}.{SILVER_SCHEMA}.bsa_pipeline_log"

_STAGE_COLUMNS = {
    "pdf_extraction": ("pdf_extraction_status", "pdf_extraction_duration_sec", "pdf_extraction_error"),
    "classification": ("classification_status", "classification_duration_sec", "classification_error"),
    "validation":     ("validation_status", "validation_duration_sec", "validation_error"),
    "merge":          ("merge_status", "merge_duration_sec", "merge_error"),
}

def _attempt_col(stage: str) -> str:
    return f"{stage}_attempt_count"

def _version_col(stage: str) -> str:
    return f"{stage}_logic_version"

_ALL_LOG_COLUMNS = ["statement_hash", "source_path", "run_id"]
for _stage in _STAGE_ORDER:
    _ALL_LOG_COLUMNS += [
        _STAGE_COLUMNS[_stage][0], _STAGE_COLUMNS[_stage][1], _STAGE_COLUMNS[_stage][2],
        _attempt_col(_stage), _version_col(_stage),
    ]
_ALL_LOG_COLUMNS += ["updated_at"]


def ensure_table_columns(table_fqn: str, ddl_columns: dict):
    """
    Adds any column in ddl_columns (name -> SQL type string) that's missing
    from the LIVE table. CREATE TABLE IF NOT EXISTS is a no-op once a table
    already exists, so editing the DDL text alone never reaches a table
    that's already live -- this closes that gap for good. Safe to call on
    every run; does nothing once columns are in sync.
    """
    existing = set(spark.table(table_fqn).columns)
    missing = {c: t for c, t in ddl_columns.items() if c not in existing}
    if missing:
        cols_sql = ", ".join(f"{c} {t}" for c, t in missing.items())
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS ({cols_sql})")
        print(f"[schema sync] Added to {table_fqn}: {list(missing.keys())}")


_pipeline_log_ddl_cols = "\n".join(
    f"        {_STAGE_COLUMNS[s][0]} STRING,\n"
    f"        {_STAGE_COLUMNS[s][1]} DOUBLE,\n"
    f"        {_STAGE_COLUMNS[s][2]} STRING,\n"
    f"        {_attempt_col(s)} INT,\n"
    f"        {_version_col(s)} INT,"
    for s in _STAGE_ORDER
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_PIPELINE_LOG} (
        statement_hash STRING,
        source_path STRING,
        run_id STRING,
{_pipeline_log_ddl_cols}
        updated_at TIMESTAMP
    ) USING DELTA
""")

_pipeline_log_expected_cols = {}
for _stage in _STAGE_ORDER:
    _pipeline_log_expected_cols[_STAGE_COLUMNS[_stage][0]] = "STRING"
    _pipeline_log_expected_cols[_STAGE_COLUMNS[_stage][1]] = "DOUBLE"
    _pipeline_log_expected_cols[_STAGE_COLUMNS[_stage][2]] = "STRING"
    _pipeline_log_expected_cols[_attempt_col(_stage)] = "INT"
    _pipeline_log_expected_cols[_version_col(_stage)] = "INT"
ensure_table_columns(TBL_PIPELINE_LOG, _pipeline_log_expected_cols)


def log_pipeline_stage(spark_session, stage: str, results_df):
    """
    results_df needs columns: statement_hash, status, duration_sec, error
    (source_path optional -- only extraction has it naturally).

    Upserts this stage's status/duration/error, increments this stage's
    attempt_count relative to whatever's already logged, stamps this
    stage's current STAGE_LOGIC_VERSIONS value, and auto-flips a repeatedly
    FAILED statement to NEEDS_REVIEW once it hits RETRY_CAP so it stops
    being silently retried forever.
    """
    status_col, duration_col, error_col = _STAGE_COLUMNS[stage]
    attempt_col = _attempt_col(stage)
    version_col = _version_col(stage)
    current_version = STAGE_LOGIC_VERSIONS[stage]
    
    results_df = results_df.dropDuplicates(['statement_hash'])

    prior_attempts = (
        spark_session.table(TBL_PIPELINE_LOG)
        .select("statement_hash", F.col(attempt_col).alias("_prior_attempts"))
    )

    staged = (
        results_df
        .join(prior_attempts, on="statement_hash", how="left")
        .withColumn("_attempts_now", F.coalesce(F.col("_prior_attempts"), F.lit(0)) + F.lit(1))
        .withColumn(
            status_col,
            F.when(
                (F.col("status") == "FAILED") & (F.col("_attempts_now") >= F.lit(RETRY_CAP)),
                F.lit("NEEDS_REVIEW"),
            ).otherwise(F.col("status")),
        )
        .withColumn(attempt_col, F.col("_attempts_now"))
        .withColumn(version_col, F.lit(current_version))
        .withColumnRenamed("duration_sec", duration_col)
        .withColumnRenamed("error", error_col)
        .drop("status", "_prior_attempts", "_attempts_now")
        .withColumn("run_id", _lit(RUN_ID))
        .withColumn("updated_at", _current_timestamp())
    )

    target = DeltaTable.forName(spark_session, TBL_PIPELINE_LOG)
    update_set = {
        status_col: f"source.{status_col}",
        duration_col: f"source.{duration_col}",
        error_col: f"source.{error_col}",
        attempt_col: f"source.{attempt_col}",
        version_col: f"source.{version_col}",
        "run_id": "source.run_id",
        "updated_at": "source.updated_at",
    }
    if "source_path" in staged.columns:
        update_set["source_path"] = "source.source_path"
    insert_values = {c: (f"source.{c}" if c in staged.columns else "NULL") for c in _ALL_LOG_COLUMNS}

    (
        target.alias("target")
        .merge(staged.alias("source"), "target.statement_hash = source.statement_hash")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )


def get_eligible_statements(stage: str, candidates_df, key_col: str = "statement_hash"):
    """
    Filters candidates_df down to rows eligible for `stage`, using
    bsa_pipeline_log as the single source of truth for what's already been
    done. A statement is eligible if it's:
      - never been attempted at this stage, OR
      - FAILED at this stage with attempt_count still under RETRY_CAP, OR
      - logged under an older logic_version than the stage's current one
        (this also brings NEEDS_REVIEW statements back in, deliberately --
        a real fix deserves a fresh shot regardless of retry history).
    """
    status_col, _, _ = _STAGE_COLUMNS[stage]
    attempt_col = _attempt_col(stage)
    version_col = _version_col(stage)
    current_version = STAGE_LOGIC_VERSIONS[stage]

    log_slice = spark.table(TBL_PIPELINE_LOG).select(
        F.col("statement_hash").alias("_log_hash"),
        F.col(status_col).alias("_status"),
        F.col(attempt_col).alias("_attempts"),
        F.col(version_col).alias("_version"),
    )

    joined = candidates_df.join(
        log_slice, candidates_df[key_col] == log_slice["_log_hash"], "left"
    )

    eligible_mask = (
        F.col("_status").isNull()
        | ((F.col("_status") == "FAILED") & (F.col("_attempts") < F.lit(RETRY_CAP)))
        # NULL logic_version means this row predates version tracking
        # entirely (e.g. processed before this column existed, or before a
        # later stage's tracking column was added). NULL < current_version
        # evaluates to NULL in Spark, not True, and .filter() drops NULL
        # rows same as False -- so without the coalesce, these statements
        # would be silently frozen at whatever old logic produced them,
        # forever, regardless of how many times STAGE_LOGIC_VERSIONS gets
        # bumped. Coalescing to -1 guarantees they're always older than any
        # real version (which starts at 1) and get one fresh pass under
        # current code, then behave normally from then on.
        | (F.coalesce(F.col("_version"), F.lit(-1)) < F.lit(current_version))
    )

    return joined.filter(eligible_mask).select(candidates_df["*"])


def invalidate_downstream(stage: str, statement_hashes_df):
    """
    Call this after a stage (re)writes real output for a batch of
    statements. Resets the NEXT stage's status/error/attempt_count to NULL
    for those statement_hashes, so that next stage's own eligibility check
    naturally picks them back up on its next run. One hop only, by design --
    each stage cascades to the one directly after it, so a Bronze fix
    ripples through Classification, then Validation, then Gold, one stage
    at a time, with no timestamp comparisons anywhere.
    No-op for the last stage in _STAGE_ORDER, and a no-op if the hash list
    is empty.
    """
    idx = _STAGE_ORDER.index(stage)
    if idx == len(_STAGE_ORDER) - 1:
        return
    next_stage = _STAGE_ORDER[idx + 1]
    status_col, _, error_col = _STAGE_COLUMNS[next_stage]
    attempt_col = _attempt_col(next_stage)

    hashes = statement_hashes_df.select("statement_hash").distinct()
    if hashes.take(1) == []:
        return

    target = DeltaTable.forName(spark, TBL_PIPELINE_LOG)
    (
        target.alias("target")
        .merge(hashes.alias("source"), "target.statement_hash = source.statement_hash")
        .whenMatchedUpdate(set={status_col: "NULL", error_col: "NULL", attempt_col: "NULL"})
        .execute()
    )


# ============================================================
# SHARED PDF EXTRACTION LOGIC
# ============================================================
# Lives here, not in pdf_extraction, so the streaming notebook and the
# bronze_backfill batch notebook both run the exact same parsing code --
# no risk of the two copies drifting apart over time.
import io
import time
import json
import hashlib
from typing import Iterator
import pandas as pd
from pyspark.sql.types import (
    StringType, StructField, StructType, IntegerType, DoubleType
)
# pdfplumber is deliberately NOT imported here at module level. Every
# notebook in the pipeline runs %run ./00_config, so a module-level import
# here would make pdfplumber a hard dependency for every stage, including
# ones that never touch a PDF (classification, validation, merge, sync).
# It's imported instead inside extract_pdfs() below, the only place it's
# actually used -- so only pdf_extraction and bronze_backfill, the two
# notebooks that call extract_pdfs, need it installed at all.


def _identify_bank(text_lower: str) -> str:
    for bank, keywords in BANK_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return bank
    return "UNKNOWN"


def reset_statement_for_stages(stage: str, statement_hash: str):
    status_col,_,error_col = _STAGE_COLUMNS[stage]
    attempt_col = _attempt_col(stage)
    version_col = _version_col(stage)
    spark.sql(f"""
              UPDATE {TBL_PIPELINE_LOG}
              SET {status_col} = null,
              {error_col} = null,
              {attempt_col} = null,{version_col} = null
              where statement_hash = '{statement_hash}'
              """)

def _compute_confidence(raw_text: str, num_pages: int, table_row_count: int) -> float:
    if not raw_text or num_pages == 0:
        return 0.0

    import re
    text_len = len(raw_text.strip())
    avg_chars_per_page = text_len / max(num_pages, 1)
    density_score = min(avg_chars_per_page / 800.0, 1.0)

    has_date = any(re.search(p, raw_text) for p in DATE_PATTERNS)
    date_score = 1.0 if has_date else 0.0

    printable = sum(1 for c in raw_text if c.isprintable())
    garble_ratio = 1 - (printable / max(text_len, 1))
    garble_score = max(0.0, 1.0 - garble_ratio * 5)

    table_score = 1.0 if table_row_count > 0 else 0.3

    return round((density_score * 0.35 + date_score * 0.25 + garble_score * 0.2 + table_score * 0.2), 3)


def extract_pdfs(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """mapInPandas worker: extract text+tables from each PDF's binary content"""
    import pdfplumber  # imported here, not at module level -- see note above
    for batch in iterator:
        out_rows = []
        for _, row in batch.iterrows():
            path = row['path']
            content = row['content']
            statement_hash = hashlib.sha256(content).hexdigest()
            t0 = time.time()

            raw_text = ""
            tables_json = '[]'
            num_pages = 0
            table_row_count = 0
            extraction_error = None

            try:
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    num_pages = len(pdf.pages)
                    page_texts = []
                    all_tables = []
                    for page in pdf.pages:
                        page_texts.append(page.extract_text() or "")
                        page_tables = page.extract_tables() or []
                        all_tables.extend(page_tables)
                    raw_text = "\n".join(page_texts)
                    table_row_count = sum(len(t) for t in all_tables)
                    tables_json = json.dumps(all_tables[:50])

            except Exception as e:
                extraction_error = str(e)[:500]

            extraction_duration_sec = round(time.time() - t0, 3)
            confidence = _compute_confidence(raw_text, num_pages, table_row_count)
            bank_format = _identify_bank(raw_text.lower()) if raw_text else "UNKNOWN"
            route = "CPU" if confidence >= EXTRACTION_CONFIDENCE_THRESHOLD else "GPU_FALLBACK"

            out_rows.append({
                "statement_hash": statement_hash,
                "source_path": path,
                "bank_format": bank_format,
                "num_pages": num_pages,
                "raw_text": raw_text,
                "tables_json": tables_json,
                "table_row_count": table_row_count,
                "extraction_confidence": confidence,
                'route': route,
                "extraction_error": extraction_error,
                "extraction_duration_sec": extraction_duration_sec,
            })
        yield pd.DataFrame(out_rows)


EXTRACTION_SCHEMA = StructType([
    StructField('statement_hash', StringType()),
    StructField('source_path', StringType()),
    StructField('bank_format', StringType()),
    StructField('num_pages', IntegerType()),
    StructField('raw_text', StringType()),
    StructField('tables_json', StringType()),
    StructField('table_row_count', IntegerType()),
    StructField('extraction_confidence', DoubleType()),
    StructField('route', StringType()),
    StructField('extraction_error', StringType()),
    StructField('extraction_duration_sec', DoubleType()),
])


print(f"Config loaded. Catalog={CATALOG} Bronze={TBL_BRONZE_EXTRACTION} "
      f"Silver(classified)={TBL_SILVER_TRANSACTIONS} Silver(validated)={TBL_SILVER_VALIDATED} "
      f"Gold={TBL_GOLD_ACCOUNT_FEATURES} "
      f"Staging = {TBL_BRONZE_STAGING} "
      f"RetryCap={RETRY_CAP} Versions={STAGE_LOGIC_VERSIONS}")
