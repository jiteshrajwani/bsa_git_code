"""
Bank Statement Analyzer - Databricks App backend (hybrid auth)

- File upload + table query: run as the logged-in user (on-behalf-of-user
  auth), since these need the user's own Unity Catalog permissions on the
  volume and table.
- Job trigger + status check: run as the app's own service principal,
  since running a job only needs the CAN_MANAGE_RUN grant declared in
  app.yaml's "resources" section - no catalog access required.

Flow:
  1. User uploads a PDF via the frontend -> saved as the user
  2. We snapshot the current time, then trigger the Job -> as the app
  3. Frontend polls /api/status/{run_id} -> as the app
  4. On success, we query bsa_account_features -> as the user
  5. User can also download the full bsa_classified_transactions table
     as an Excel file at any time -> as the user
"""
import pdfplumber
import hashlib
import os
import uuid
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
from openpyxl import Workbook


class SizedBytesIO(BytesIO):
    def __len__(self):
        return self.getbuffer().nbytes

# 
# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
JOB_ID = int(os.environ.get("BSA_JOB_ID", "587633475324017"))
VOLUME_PATH = os.environ.get("BSA_VOLUME_PATH", "/Volumes/edp_bfil_prod/analytics_team/bsa_raw_statements/incoming")
TABLE_NAME = os.environ.get("BSA_TABLE_NAME", "edp_bfil_prod.analytics_team.bsa_account_features")
TRANSACTIONS_TABLE = os.environ.get("BSA_TRANSACTIONS_TABLE", "edp_bfil_prod.analytics_team.bsa_classified_transactions")
WAREHOUSE_ID = os.environ.get("BSA_WAREHOUSE_ID", "7ce0374386ff9e43")
# ---------------------------------------------------------------------------

BANK_STATEMENT_KEYWORDS = [
    'account_number','account_no','a/c no','a/c number',
    'ifsc','ifsc code','micr','micr code',
    'statement of account','account statement','bank statement',
    'opening balance','closing balance','balance b/f','balance c/f','balance',
    'withdrawl','deposit','debit','credit',
    'transaction_date','value_date','narration','particulars',
    'customer id','cif no','cif number','crn',
    'savings account','current account','cheque','chq no',
    'neft','rtgs','imps','upi'
]
BANK_STATEMENT_MIN_MATCHES = 2

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")

app = FastAPI()

# App-level client, using the app's own service principal (from
# DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET injected automatically).
# Only used for actions granted via app.yaml's "resources" block.
app_client = WorkspaceClient()

# RUN_SNAPSHOTS = {}


def get_user_client(request: Request) -> WorkspaceClient:
    """
    Build a WorkspaceClient authenticated as the logged-in user, using the
    access token Databricks Apps forwards when on-behalf-of-user
    authorization is enabled (scopes: sql, files).
    """
    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        raise HTTPException(
            401,
            "Missing forwarded user token. Make sure 'sql' and 'files' "
            "scopes are added under User authorization for this app, and "
            "that you've reloaded the app since they were added.",
        )
    return WorkspaceClient(host=DATABRICKS_HOST, token=user_token, auth_type="pat")


def _looks_like_bank_statement(contents: bytes) -> bool:
    """Cheap first page only check, meant to instantly reject wrong 
    uploads before they even touch the volumne or trigger a job"""

    try:
        with pdfplumber.open(BytesIO(contents)) as pdf:
            pages = pdf.pages[:2]
            text = "\n".join((p.extract_text() or "") for p in pages).lower()
    except Exception:
        return False # unreadable/corrupted/password-protected pdf
    
    matches = sum(1 for kw in BANK_STATEMENT_KEYWORDS if kw.lower() in text)
    return matches>=BANK_STATEMENT_MIN_MATCHES

@app.post("/api/upload")
async def upload_pdf(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Please upload a PDF file")

    w_user = get_user_client(request)  # upload as the user (needs volume access)

    unique_name = f"{uuid.uuid4().hex}_{file.filename}"
    dest_path = f"{VOLUME_PATH}/{unique_name}"

    contents = await file.read()
    if not _looks_like_bank_statement(contents):
        raise HTTPException(
            422,
            "This doenst look like a bank statement. Please upload a valid bank statement PDF")
    
    statement_hash = hashlib.sha256(contents).hexdigest() # same hash the pipeline computes
    w_user.files.upload(dest_path, SizedBytesIO(contents), overwrite=True)

    # snapshot_time = datetime.now(timezone.utc).isoformat()

    # Trigger the job as the app itself (only needs CAN_MANAGE_RUN, granted
    # via the "resources" block in app.yaml - no catalog access needed).
    run = app_client.jobs.run_now(job_id=JOB_ID)
    run_id = run.run_id
    # RUN_SNAPSHOTS[run_id] = snapshot_time
    # RUN_SNAPSHOTS[run_id] = {'snapshot_time':snapshot_time, "statement_hash":statement_hash}
    return {'run_id':run_id,"file_saved_as": unique_name,"statement_hash":statement_hash}
    # return {"run_id": run_id, "file_saved_as": unique_name}


@app.get("/api/status/{run_id}")
def get_status(run_id: int):
    run = app_client.jobs.get_run(run_id=run_id)
    state = run.state

    life_cycle_state = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
    result_state = state.result_state.value if state and state.result_state else "UNKNOWN"
    done = life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
    success = result_state == "SUCCESS"

    return {
        "run_id": run_id,
        "life_cycle_state": life_cycle_state,
        "result_state": result_state,
        "done": done,
        "success": success,
    }


@app.get("/api/result/{statement_hash}")
def get_result(request: Request, statement_hash:str):
    w_user = get_user_client(request)  # query as the user (needs table access)


    # snapshot_time = RUN_SNAPSHOTS.get(run_id)
    # run_data = RUN_SNAPSHOTS.get(run_id)
    # if run_data is None:
    #     raise HTTPException(404, "Unknown run_id")

    # query = f"""
    #     SELECT *
    #     FROM {TABLE_NAME}
    #     WHERE computed_at > TIMESTAMP('{snapshot_time}')
    #     ORDER BY computed_at DESC
    # """

    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE statement_hash = '{statement_hash}'
    """


    result = w_user.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )

    if result.result is None or result.result.data_array is None:
        return {"columns": [], "rows": []}

    columns = [c.name for c in result.manifest.schema.columns]
    rows = result.result.data_array

    return {"columns": columns, "rows": rows}


@app.get("/api/download-transactions/{statement_hash}")
def download_transactions(request: Request,statement_hash:str):
    """
    Streams the entire bsa_classified_transactions table as an .xlsx file.

    Note: execute_statement returns a bounded result set (roughly up to
    ~100k rows / a few MB by default). If this table grows much larger than
    that over time, this endpoint will need to switch to chunked fetching
    (result.next_chunk_index) or an EXTERNAL_LINKS disposition instead of
    reading it in one call.
    """
    w_user = get_user_client(request)  # query as the user (needs table access)
    # run_data = RUN_SNAPSHOTS.get(run_id) 
    # if run_data is None:
    #     raise HTTPException(404,"Unknown run_id")
    
    query = f"""
    SELECT * FROM {TRANSACTIONS_TABLE}
    where statement_hash = '{statement_hash}'
    order by line_no
    """

    # query = f"SELECT * FROM {TRANSACTIONS_TABLE}"

    result = w_user.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )

    if result.result is None or result.result.data_array is None:
        raise HTTPException(404, "No data found in bsa_classified_transactions")

    columns = [c.name for c in result.manifest.schema.columns]
    rows = result.result.data_array

    wb = Workbook()
    ws = wb.active
    ws.title = "Transactions"
    ws.append(columns)
    for row in rows:
        ws.append(row)

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=bsa_classified_transactions.xlsx"
        },
    )


STATIC_DIR = Path(__file__).resolve().parent / "static"
if not STATIC_DIR.exists():
    raise RuntimeError(
        f"Expected static directory at {STATIC_DIR} but it was not found. "
        f"Check that 'static/index.html' sits next to app.py in your workspace folder."
    )
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
