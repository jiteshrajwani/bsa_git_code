%run ./00_config

# IMPORTING ALL THE LIBRARIES
from typing import Iterator 
import time
import pandas as pd 
from pyspark.sql.functions import col,current_timestamp 
from pyspark.sql.types import StringType,StructField,StructType,DoubleType,IntegerType,BooleanType
from delta.tables import DeltaTable

# RECONCILATION SETUP
RECONCILIATION_TOLERANCE = 0.05 

VALIDATION_SCHEMA = StructType([
    StructField('statement_hash',StringType()),
    StructField('total_lines',IntegerType()),
    StructField('reconciled_lines',IntegerType()),
    StructField('mismatch_count',IntegerType()),
    StructField('validation_status',StringType()),
    StructField('validation_notes',StringType()),
    StructField('pipeline_status',StringType()),
    StructField('pipeline_duration_sec',DoubleType()),
    StructField('pipeline_error',StringType())
])


def validate_statement_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch in iterator:
        results = []
        for statement_hash, group in batch.groupby("statement_hash"):
            t0 = time.time()
            try:
                group = group.sort_values("line_no")
                total_lines = len(group)
                mismatches = 0
                reconciled = 0
                prev_balance = None

                for _, row in group.iterrows():
                    bal = row["running_balance"]
                    debit = 0.0 if pd.isna(row["debit_amount"]) else row["debit_amount"]
                    credit = 0.0 if pd.isna(row["credit_amount"]) else row["credit_amount"]
                    if pd.isna(bal):
                        continue
                    if prev_balance is not None and (debit or credit):
                        expected_balance = prev_balance - debit + credit
                        diff = abs(expected_balance - bal)
                        if diff <= RECONCILIATION_TOLERANCE:
                            reconciled += 1
                        else:
                            mismatches += 1
                    prev_balance = bal

                # FIX: previously checked only `mismatches == 0` for PASSED,
                # which also matched statements where NOTHING was ever
                # actually reconciled (single-line statements, statements
                # with missing/NaN running_balance throughout, or ones where
                # classification silently produced zero usable
                # debit/credit values) -- those were being reported as
                # "All lines reconciled" when zero lines were ever checked.
                # UNVERIFIED separates "we checked and it's clean" from
                # "we never got the chance to check."
                if total_lines == 0:
                    status, notes = "FAILED", "No parsed transaction lines"
                elif reconciled == 0:
                    status, notes = "UNVERIFIED", "No lines had both a prior balance and a debit/credit to check"
                elif mismatches == 0:
                    status, notes = "PASSED", "All lines reconciled within tolerance"
                elif reconciled > mismatches:
                    status, notes = "PARTIAL", f"{mismatches} of {total_lines} lines did not reconcile"
                else:
                    status, notes = "FAILED", f"{mismatches} of {total_lines} lines did not reconcile"

                results.append({
                    "statement_hash": statement_hash, "total_lines": total_lines,
                    "reconciled_lines": reconciled, "mismatch_count": mismatches,
                    "validation_status": status, "validation_notes": notes,
                    "pipeline_status": "SUCCESS",
                    "pipeline_duration_sec": round(time.time() - t0, 3),
                    "pipeline_error": None,
                })
            except Exception as e:
                results.append({
                    "statement_hash": statement_hash, "total_lines": None,
                    "reconciled_lines": None, "mismatch_count": None,
                    "validation_status": None, "validation_notes": None,
                    "pipeline_status": "FAILED",
                    "pipeline_duration_sec": round(time.time() - t0, 3),
                    "pipeline_error": str(e)[:500],
                })
        yield pd.DataFrame(results, columns=[f.name for f in VALIDATION_SCHEMA.fields])


# CREATING THE TABLE
spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_SILVER_VALIDATED} (
        statement_hash string,
        total_lines int,
        reconciled_lines int,
        mismatch_count int,
        validation_status string,
        validation_notes string,
        validated_at timestamp
        ) using delta""")

ensure_table_columns(TBL_SILVER_VALIDATED, {
    "statement_hash": "STRING", "total_lines": "INT", "reconciled_lines": "INT",
    "mismatch_count": "INT", "validation_status": "STRING", "validation_notes": "STRING",
    "validated_at": "TIMESTAMP",
})

txns_to_validate_candidates = (
    spark.table(TBL_SILVER_TRANSACTIONS)
    .select('statement_hash')
    .distinct()
)

# log-table-driven eligibility -- replaces the old plain left-anti check,
# now also picks up FAILED-under-cap retries and statements whose logged
# validation logic_version is older than STAGE_LOGIC_VERSIONS['validation']
eligible_hashes = get_eligible_statements("validation", txns_to_validate_candidates)

txns_to_validate = (
    spark.table(TBL_SILVER_TRANSACTIONS)
    .select('statement_hash','line_no','debit_amount','credit_amount','running_balance')
    .join(eligible_hashes, on='statement_hash', how='inner')
    .repartition('statement_hash')
    .sortWithinPartitions('statement_hash','line_no')
)

validation_results_raw = txns_to_validate.mapInPandas(validate_statement_partition, schema=VALIDATION_SCHEMA)

# materialize once via staging -- log_rows and validation_results below both
# read back from the SAME staged table, so validate_statement_partition
# never runs twice
validation_results_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TBL_SILVER_VALIDATED_STAGING)
staged_validation_df = spark.table(TBL_SILVER_VALIDATED_STAGING)

log_rows = staged_validation_df.select(
    col("statement_hash"),
    col("pipeline_status").alias("status"),
    col("pipeline_duration_sec").alias("duration_sec"),
    col("pipeline_error").alias("error"),
)

validation_results = (
    staged_validation_df
    .drop("pipeline_status", "pipeline_duration_sec", "pipeline_error")
    .withColumn("validated_at", current_timestamp())
)

row_count = validation_results.count()

# a statement can now be RE-validated (not just newly validated), so this
# needs an update branch alongside insert, not insert-only
target_table = DeltaTable.forName(spark, TBL_SILVER_VALIDATED)
update_cols = {c: f"source.{c}" for c in validation_results.columns if c != "statement_hash"}
insert_cols = {c: f"source.{c}" for c in validation_results.columns}
(
    target_table.alias("target")
    .merge(validation_results.alias("source"), "target.statement_hash = source.statement_hash")
    .whenMatchedUpdate(set=update_cols)
    .whenNotMatchedInsert(values=insert_cols)
    .execute()
)

log_pipeline_stage(spark, "validation", log_rows)

# only statements that actually got a fresh validation result should force
# Gold to redo its work
invalidate_downstream("validation", validation_results.select("statement_hash"))

print(f"Validated {row_count} statement(s) -> {TBL_SILVER_VALIDATED}")


display(
    spark.table(TBL_SILVER_VALIDATED)
    .groupBy('validation_status')
    .count()
    .orderBy(col('count').desc())
    )
