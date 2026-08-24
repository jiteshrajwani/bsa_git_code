%run ./00_config

from pyspark.sql import DataFrame 
from pyspark.sql.functions import col, current_timestamp, lit, when as _when
from delta.tables import DeltaTable

# Process Batch (Actual PDF EXTRACTION EXECUTE FUNCTION)
# extract_pdfs() and EXTRACTION_SCHEMA now live in 00_config, shared with
# the bronze_backfill notebook so both use the exact same parsing code.
def process_batch(batch_df: DataFrame,batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no files")
        return 
        
    extracted_df = batch_df.select('path','content').mapInPandas(extract_pdfs,schema=EXTRACTION_SCHEMA)
    extracted_df = (
        extracted_df
        .dropDuplicates(['statement_hash'])
        .withColumn('ingested_at',current_timestamp())
        .withColumn('pipeline_stages',lit('bronze_extraction'))
    )

    # materialize once via staging (serverless has no .cache()/.persist()) --
    # log_rows below reads back from staged_df, NOT extracted_df, so
    # extract_pdfs (the expensive pdfplumber parsing) never re-runs a
    # second time for the same batch
    extracted_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(TBL_BRONZE_STAGING)
    staged_df = spark.table(TBL_BRONZE_STAGING)
    row_count = staged_df.count()

    target_table = DeltaTable.forName(batch_df.sparkSession,TBL_BRONZE_EXTRACTION)
    insert_cols = {c: f"source.{c}" for c in staged_df.columns}
    (
    target_table.alias('target')
    .merge(staged_df.alias('source'),'target.statement_hash=source.statement_hash')   
    .whenNotMatchedInsert(values=insert_cols)
    .execute()
    )

    log_rows = staged_df.select(
        col('statement_hash'),
        col('source_path'),
        _when(col('extraction_error').isNull(), lit('SUCCESS')).otherwise(lit('FAILED')).alias('status'),
        col('extraction_duration_sec').alias('duration_sec'),
        col('extraction_error').alias('error')
    )
    log_pipeline_stage(spark, "pdf_extraction", log_rows)

    print(f"Batch {batch_id} merged {row_count} statement(s) into {TBL_BRONZE_EXTRACTION}")


# CREATING THE TBL_BRONZE_EXTRACTION
spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_BRONZE_EXTRACTION} (
            statement_hash string,
            source_path string,
            bank_format string,
            num_pages int,
            raw_text string,
            tables_json string,
            table_row_count int,
            extraction_confidence double ,
            route string, 
            extraction_error string,
            extraction_duration_sec double,
            ingested_at TIMESTAMP,
            pipeline_stages string
        ) using delta
        """)

ensure_table_columns(TBL_BRONZE_EXTRACTION, {
    "statement_hash": "STRING", "source_path": "STRING", "bank_format": "STRING",
    "num_pages": "INT", "raw_text": "STRING", "tables_json": "STRING",
    "table_row_count": "INT", "extraction_confidence": "DOUBLE", "route": "STRING",
    "extraction_error": "STRING", "extraction_duration_sec": "DOUBLE",
    "ingested_at": "TIMESTAMP", "pipeline_stages": "STRING",
})

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .load(RAW_VOLUME_PATH)
)

query = (
    raw_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_VOLUME_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# THIS RUN'S STATEMENTS -- scoped to what pdf_extraction actually touched
# in this job run, keyed off bsa_pipeline_log.run_id (stamped by
# log_pipeline_stage inside process_batch for every batch processed here).
# Answers "what happened to the PDF(s) I just uploaded/ran," not "what does
# the whole Bronze table look like historically."
print(f"=== pdf_extraction run summary (run_id={RUN_ID}) ===")
display(
    spark.table(TBL_PIPELINE_LOG)
    .filter(col("run_id") == RUN_ID)
    .select(
        "statement_hash", "source_path",
        "pdf_extraction_status", "pdf_extraction_duration_sec", "pdf_extraction_error",
        "pdf_extraction_attempt_count",
    )
)

# OVERALL TABLE HEALTH -- small aggregate for context, not a substitute for
# the per-run view above
print("=== bsa_statement_extraction_raw overall health (route x error presence) ===")
display(
    spark.table(TBL_BRONZE_EXTRACTION)
    .withColumn("has_error", col("extraction_error").isNotNull())
    .groupBy("route", "has_error")
    .count()
    .orderBy(col("count").desc())
)

print("=== bsa_statement_extraction_raw most recent 20 (any run) ===")
display(
    spark.table(TBL_BRONZE_EXTRACTION)
    .select("statement_hash", "bank_format", "num_pages", "extraction_confidence", "route", "extraction_error")
    .orderBy(col("ingested_at").desc())
    .limit(20)
)
