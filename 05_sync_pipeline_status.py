%run ./00_config

import requests
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# map YOUR task_key names (exactly as typed in the Workflow UI) to column
# prefixes -- defined at module level (not nested inside the else branch)
# so both the schema-safety check below and the actual sync logic use the
# exact same mapping
TASK_KEY_TO_PREFIX = {
    "pdf_extraction": "pdf_extraction",
    "bronze_backfill": "bronze_backfill",
    "transaction_classification": "classification",
    "validation_checksum": "validation",
    "merge_feature_engineering": "merge",
}

# Guarantee the live table actually has a column for every prefix above
# before anything tries to write to it. UPDATE_SCHEMA_FIELDS below is built
# dynamically from TASK_KEY_TO_PREFIX, so adding a new task_key (like
# bronze_backfill) automatically extends that schema -- but only this call
# actually gets those columns onto the live Delta table. Runs every time,
# no-ops once columns are already in sync.
_job_task_cols = {}
for _prefix in TASK_KEY_TO_PREFIX.values():
    _job_task_cols[f"{_prefix}_task_duration_sec"] = "DOUBLE"
    _job_task_cols[f"{_prefix}_task_result_state"] = "STRING"
    _job_task_cols[f"{_prefix}_task_error"] = "STRING"
_job_task_cols["job_total_duration_sec"] = "DOUBLE"
_job_task_cols["job_overall_result_state"] = "STRING"
ensure_table_columns(TBL_PIPELINE_LOG, _job_task_cols)

# Skip gracefully if this isn't a real job run (e.g. someone testing interactively)
if not RUN_ID.isdigit():
    print(f"RUN_ID='{RUN_ID}' isn't a real job run_id -- skipping status sync (interactive run).")
else:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_url = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    if not api_token:
        raise RuntimeError(
            "ctx.apiToken() returned empty -- your workspace may restrict this. "
            "Store a PAT in a secret scope and use dbutils.secrets.get(...) instead."
        )
    resp = requests.get(
        f"{api_url}/api/2.1/jobs/runs/get",
        headers={"Authorization": f"Bearer {api_token}"},
        params={"run_id": RUN_ID},
    )
    resp.raise_for_status()
    run_info = resp.json()

    updates = {
        "job_total_duration_sec": round(run_info.get("run_duration", 0) / 1000, 3),
        "job_overall_result_state": run_info.get("state", {}).get("result_state"),
    }
    for task in run_info.get("tasks", []):
        prefix = TASK_KEY_TO_PREFIX.get(task.get("task_key"))
        if not prefix:
            continue
        state = task.get("state", {})
        updates[f"{prefix}_task_duration_sec"] = round(task.get("execution_duration", 0) / 1000, 3)
        updates[f"{prefix}_task_result_state"] = state.get("result_state")
        updates[f"{prefix}_task_error"] = state.get("state_message") or None

    # Explicit schema instead of type inference -- inference fails with
    # CANNOT_DETERMINE_TYPE whenever a column's only value happens to be None
    # (e.g. state_message is empty for a successful task), since there's no
    # second row for Spark to infer a type from.
    UPDATE_SCHEMA_FIELDS = [StructField("run_id", StringType())]
    for prefix in TASK_KEY_TO_PREFIX.values():
        UPDATE_SCHEMA_FIELDS += [
            StructField(f"{prefix}_task_duration_sec", DoubleType()),
            StructField(f"{prefix}_task_result_state", StringType()),
            StructField(f"{prefix}_task_error", StringType()),
        ]
    UPDATE_SCHEMA_FIELDS += [
        StructField("job_total_duration_sec", DoubleType()),
        StructField("job_overall_result_state", StringType()),
    ]
    UPDATE_SCHEMA = StructType(UPDATE_SCHEMA_FIELDS)
    full_update_row = {"run_id": RUN_ID, **updates}
    # fill in any column the loop above didn't touch (e.g. a task_key that
    # didn't match TASK_KEY_TO_PREFIX) with None, so the row matches the
    # schema exactly -- avoids a separate "missing key" error
    full_update_row = {f.name: full_update_row.get(f.name) for f in UPDATE_SCHEMA.fields}
    update_df = spark.createDataFrame([full_update_row], schema=UPDATE_SCHEMA)
    target = DeltaTable.forName(spark, TBL_PIPELINE_LOG)
    set_expr = {c: f"source.{c}" for c in updates.keys()}
    (
        target.alias("target")
        .merge(update_df.alias("source"), "target.run_id = source.run_id")
        .whenMatchedUpdate(set=set_expr)
        .execute()
    )
    print(f"Synced task/job-level status for run_id={RUN_ID}: {run_info.get('state', {}).get('result_state')}")

display(spark.table(TBL_PIPELINE_LOG))
