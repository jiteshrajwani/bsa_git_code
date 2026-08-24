%run ./00_config

# IMPORTING THE LIBRARIES
from pyspark.sql import functions as F
import time
from delta.tables import DeltaTable

# CREATING THE FINAL GOLD TABLE
spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_GOLD_ACCOUNT_FEATURES} (
        statement_hash string,
        bank_format string,
        total_transactions long,
        total_inflow double,
        total_outflow double,
        avg_transaction_amount double,
        min_balance double,
        max_balance double,
        avg_balance double,
        salary_credit_count long,
        atm_withdrawl_count long,
        upi_txn_count long,
        bounce_or_reversal_count long,
        emi_txn_count long,
        distinct_txn_types long,
        computed_at Timestamp) using delta
        """)

ensure_table_columns(TBL_GOLD_ACCOUNT_FEATURES, {
    "statement_hash": "STRING", "bank_format": "STRING", "total_transactions": "LONG",
    "total_inflow": "DOUBLE", "total_outflow": "DOUBLE", "avg_transaction_amount": "DOUBLE",
    "min_balance": "DOUBLE", "max_balance": "DOUBLE", "avg_balance": "DOUBLE",
    "salary_credit_count": "LONG", "atm_withdrawl_count": "LONG", "upi_txn_count": "LONG",
    "bounce_or_reversal_count": "LONG", "emi_txn_count": "LONG", "distinct_txn_types": "LONG",
    "computed_at": "TIMESTAMP",
})

# ONLY STATEMENTS THAT PASSED VALIDATION (business-logic gate) ...
validated_ok = (
    spark.table(TBL_SILVER_VALIDATED)
    .filter(F.col('validation_status').isin('PASSED', "PARTIAL"))
    .select('statement_hash')
)

# ... AND are eligible per the retry/version policy for the merge stage
# itself (this replaces the old already_in_gold left-anti check -- it now
# also picks up FAILED-under-cap retries and statements whose logged merge
# logic_version is older than STAGE_LOGIC_VERSIONS['merge'])
new_eligible = get_eligible_statements("merge", validated_ok)

txns = (
    spark.table(TBL_SILVER_TRANSACTIONS)
    .join(new_eligible,on='statement_hash',how='inner')
)

t0 = time.time()
try:
    features_df = (
        txns.groupBy("statement_hash", "bank_format")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("credit_amount").alias("total_inflow"),
            F.sum("debit_amount").alias("total_outflow"),
            F.avg(F.coalesce("debit_amount", "credit_amount")).alias("avg_transaction_amount"),
            F.min("running_balance").alias("min_balance"),
            F.max("running_balance").alias("max_balance"),
            F.avg("running_balance").alias("avg_balance"),
            F.sum(F.when(F.col("txn_type") == "SALARY_CREDIT", 1).otherwise(0)).alias("salary_credit_count"),
            F.sum(F.when(F.col("txn_type") == "ATM_WITHDRAWL", 1).otherwise(0)).alias("atm_withdrawl_count"),
            F.sum(F.when(F.col("txn_type") == "UPI", 1).otherwise(0)).alias("upi_txn_count"),
            F.sum(F.when(F.col("txn_type") == "REVERSAL", 1).otherwise(0)).alias("bounce_or_reversal_count"),
            F.sum(F.when(F.col("txn_type") == "EMI", 1).otherwise(0)).alias("emi_txn_count"),
            F.countDistinct("txn_type").alias("distinct_txn_types"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    # materialize once via staging (serverless has no .cache()/.persist())
    features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TBL_GOLD_STAGING)
    staged_features_df = spark.table(TBL_GOLD_STAGING)
    row_count = staged_features_df.count()
    duration = round(time.time() - t0, 3)

    # a statement can now be RE-merged (not just newly merged), so this
    # needs an update branch alongside insert, not insert-only
    target_table = DeltaTable.forName(spark, TBL_GOLD_ACCOUNT_FEATURES)
    update_cols = {c: f"source.{c}" for c in staged_features_df.columns if c != "statement_hash"}
    insert_cols = {c: f"source.{c}" for c in staged_features_df.columns}
    (
        target_table.alias("target")
        .merge(staged_features_df.alias("source"), "target.statement_hash = source.statement_hash")
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsert(values=insert_cols)
        .execute()
    )

    log_pipeline_stage(spark, "merge", staged_features_df.select(
        F.col("statement_hash"),
        F.lit("SUCCESS").alias("status"),
        F.lit(duration).alias("duration_sec"),
        F.lit(None).cast("string").alias("error"),
    ))
    # Gold is the last stage -- invalidate_downstream("merge", ...) would be
    # a no-op anyway, so it's intentionally omitted here
    print(f"Wrote features for {row_count} new statement(s) -> {TBL_GOLD_ACCOUNT_FEATURES}")

except Exception as e:
    duration = round(time.time() - t0, 3)
    log_pipeline_stage(spark, "merge", new_eligible.select(
        F.col("statement_hash"),
        F.lit("FAILED").alias("status"),
        F.lit(duration).alias("duration_sec"),
        F.lit(str(e)[:500]).alias("error"),
    ))
    raise  # still surface the failure to the Databricks Job UI -- logging supplements, doesn't hide it
