%run ./00_config

# Importing all the libraries
import time
import re 
import json
from typing import Iterator,List,Dict
import pandas as pd 
from pyspark.sql.functions import col,current_timestamp,lit
from pyspark.sql.types import (StringType,StructField,StructType,IntegerType,DoubleType)
from delta.tables import DeltaTable

# SOME FIXED REGEX PATTERNS

_DATE_RE = re.compile("|".join(f"({p})" for p in DATE_PATTERNS ) )
_AMOUNT_RE = re.compile(r"[\d,]+\.\d{2}")
_PAGE_NUMBER_RE = re.compile(r'^\s*page\s*\d+\s*$', re.IGNORECASE)
_REPEATED_HEADER_RE = re.compile(r'\bdate\b.*\bparticulars\b.*\bbalance\b', re.IGNORECASE)


def _classify_transaction_type(narration: str) -> tuple[str,float]:
    narration_lower = narration.lower()
    for txn_type,pattern in TRANSACTION_TYPE_RULES:
        if re.search(pattern,narration_lower,re.IGNORECASE):
            return txn_type,0.9 
    return "OTHER",0.3


# def _infer_direction_from_narration(narration: str) ->str:
#     n = narration.upper()
#     if re.search(r"/CR[/\-]|\bCR\b|\brev(ersal)?\b",n,re.IGNORECASE) or n.startswith("BY TRANSFER") :
#         return "CREDIT"
#     if re.search(r"/DR[/\-]|\bDR\b",n) or n.startswith('TO TRANSFER'):
#         return "DEBIT"
#     return "UNKNOWN"

def _infer_direction_from_narration(narration: str) ->str:
    n = narration.upper().strip()
    # "TO X" / "BY X" is a common PSU-bank narration convention (TO
    # TRANSFER, TO TRF, TO CASH, TO SELF ... / BY TRANSFER, BY TRF, BY
    # CASH, BY SELF ...). Checked first, since it's a positional signal
    # anchored at the start of the narration, more reliable than a bare
    # CR/DR substring search. This generalizes the original TO
    # TRANSFER/BY TRANSFER special case to any "TO "/"BY " prefix, so a
    # new bank's abbreviation doesn't need its own one-off patch.
    if n.startswith("BY "):
        return "CREDIT"
    if n.startswith("TO "):
        return "DEBIT"
    if re.search(r"/CR[/\-]|\bCR\b|\brev(ersal)?\b", n, re.IGNORECASE):
        return "CREDIT"
    if re.search(r"/DR[/\-]|\bDR\b", n):
        return "DEBIT"
    return "UNKNOWN"



def _infer_direction_from_flag(flag_cell) -> str:
    if not flag_cell:
        return 'UNKNOWN'
    f = str(flag_cell).strip().lower()
    if f in ('dr', 'd', 'debit'):
        return 'DEBIT'
    if f in ('cr', 'c', 'credit'):
        return 'CREDIT'
    return 'UNKNOWN'

# MORE PATTERNS FOR CHEQUE AND SUMMARY SKIP
_CHQ_REF_RE = re.compile(r'^\s*(chq|ref|cheque|reference)[\s:.]', re.IGNORECASE)
_SUMMARY_SKIP_RE = re.compile(
    r'\b(opening balance|closing balance|total|carried forward|b/f|c/f)\b', re.IGNORECASE
)


# Matches an amount immediately followed by a bare or parenthesized Dr/Cr
# tag, e.g. "249.00 (Dr)", "1,396.60 (Cr)", "1,186.69 Cr" -- lets the block
# parser see the SAME direction signal _clean_amount_with_direction uses for
# table cells, instead of only guessing direction from narration keywords.
_AMOUNT_WITH_TAG_RE = re.compile(
    r"([\d,]+\.\d{2})\s*\(?\s*(cr|dr)\.?\)?", re.IGNORECASE
)


def _tag_directions_for_amounts(line: str, amounts: List[str]) -> Dict[str, str]:
    """For each amount string found in `line`, look up the Dr/Cr tag
    immediately following it (if any) and return {amount: 'DEBIT'/'CREDIT'}.
    Amounts with no adjacent tag are simply absent from the result."""
    directions = {}
    for amt, tag in _AMOUNT_WITH_TAG_RE.findall(line):
        directions[amt] = "DEBIT" if tag.lower() == "dr" else "CREDIT"
    # findall on _AMOUNT_RE can normalize differently (e.g. no dedup) than
    # _AMOUNT_WITH_TAG_RE -- match back onto the exact strings _AMOUNT_RE
    # returned so callers can key off those directly
    return {a: directions[a] for a in amounts if a in directions}


# FINALIZING THE BLOCK FOR TABLES
def _finalize_block(block_lines: List[str]) -> Dict:
    summary_idx = summary_date = summary_amounts = None

    # Preferred case: one line carries both date and amount(s) together
    for idx, l in enumerate(block_lines):
        dm = _DATE_RE.search(l)
        amts = _AMOUNT_RE.findall(l)
        if dm and amts:
            summary_idx, summary_date, summary_amounts = idx, dm, amts
            break

    # if summary_idx is not None:
    #     summary_line = block_lines[summary_idx]
    #     remainder = summary_line[:summary_date.start()] + " " + summary_line[summary_date.end():]
    #     for amt in summary_amounts:
    #         remainder = remainder.replace(amt, " ")
    #     inline_narration = re.sub(r"\s+"," ",remainder).strip(" -/:")
    #     other_lines = [l for i, l in enumerate(block_lines) if i != summary_idx]
    #     narration = re.sub(r"\s+", " ", " ".join([*other_lines, inline_narration])).strip()

    tag_directions = {}
    if summary_idx is not None:
        summary_line = block_lines[summary_idx]
        # capture any amount->direction tag BEFORE the amounts get stripped
        # out of the line below, same signal _clean_amount_with_direction
        # uses for table cells (e.g. "249.00 (Dr)", "1,396.60 Cr")
        tag_directions = _tag_directions_for_amounts(summary_line, summary_amounts)
        # strip EVERY date occurence on the top line, not just the first match
        # -- some bank repeats value date/post date on the post line and, a
        # leftover second date otherwise pulls out the narration
        remainder = _DATE_RE.sub(" ",summary_line)
        for amt in summary_amounts:
            remainder = re.sub(re.escape(amt) + r"\s*\(?\s*(cr|dr)\.?\)?\s*"," ",remainder,flags=re.IGNORECASE  )
        inline_narration = re.sub(r"\s+"," ",remainder).strip(" -/:")
        other_lines = [l for i, l in enumerate(block_lines) if i != summary_idx]
        narration = re.sub(r"\s+", " ", " ".join([inline_narration,*other_lines])).strip()



    else:
        # Fallback: date sits on its own line, separate from the amounts --
        # e.g. a short cash-deposit entry where the date heads the block and
        # the deposit/balance numbers sit on their own line below. Take the
        # first date found anywhere in the block, and the line with the most
        # amount matches (preferring 2+ amounts -- more likely to be the
        # real txn-amount + balance pair, not a stray reference number).
        date_idx = date_match = None
        for idx, l in enumerate(block_lines):
            dm = _DATE_RE.search(l)
            if dm:
                date_idx, date_match = idx, dm
                break
        if date_match is None:
            return None

        best_amt_idx, best_amts = None, []
        for idx, l in enumerate(block_lines):
            amts = _AMOUNT_RE.findall(l)
            if len(amts) > len(best_amts):
                best_amt_idx, best_amts = idx, amts
        if not best_amts:
            return None

        summary_date, summary_amounts = date_match, best_amts
        other_lines = [l for i, l in enumerate(block_lines) if i not in (date_idx, best_amt_idx)]
        narration = re.sub(r"\s+", " ", " ".join(other_lines)).strip()

    balance_str = summary_amounts[-1].replace(",", "")
    txn_amount = float(summary_amounts[-2].replace(",", "")) if len(summary_amounts) > 1 else None

    txn_type, txn_confidence = _classify_transaction_type(narration)
    # The txn-amount cell's own Dr/Cr tag (when present) is a direct signal
    # from the source, and takes priority over guessing from narration --
    # same precedence _parse_table_rows gives it for table cells.
    direction = tag_directions.get(summary_amounts[-2]) if len(summary_amounts) > 1 else None
    if not direction:
        direction = _infer_direction_from_narration(narration)
    if direction == "UNKNOWN":
        CREDIT_LEANING = {"SALARY_CREDIT", "INTEREST", "REVERSAL", "CASH_DEPOSIT","REV"}
        direction = "CREDIT" if txn_type in CREDIT_LEANING else "DEBIT"

    return {
        "txn_date_raw": summary_date.group(0),
        "narration": narration,
        "debit_amount": None if direction == "CREDIT" else txn_amount,
        "credit_amount": txn_amount if direction == "CREDIT" else None,
        "running_balance": float(balance_str) if balance_str else None,
        "txn_type": txn_type,
        "classification_confidence": txn_confidence,
        "classification_method": "raw_text_block",
    }

    
# FALLBACK FOR STATEMENTS WITH NO DETECTED TABLES E.G. CANARA BANK STATEMENT
def _parse_lines(raw_text: str) -> List[Dict]:
    """Fallback for statements with no detected table. Handles both a
    single-line layout (date+narration+amounts on one line) and a multi-line
    layout (narration wraps across several lines around a date+amount
    'summary' line), delimited by Chq:/Ref: markers where present, or by the
    next summary line otherwise."""
    parsed = []
    if not raw_text:
        return parsed

    lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
    current_block, line_no = [], 0

    def _emit(block):
        nonlocal line_no
        txn = _finalize_block(block)
        if txn:
            line_no += 1
            txn["line_no"] = line_no
            parsed.append(txn)

    for line in lines:
        if _PAGE_NUMBER_RE.match(line) or _REPEATED_HEADER_RE.search(line):
            continue

        if _SUMMARY_SKIP_RE.search(line):
            current_block = []
            continue

        if _CHQ_REF_RE.match(line):
            _emit(current_block)
            current_block = []
            continue

        has_summary = bool(_DATE_RE.search(line) and _AMOUNT_RE.findall(line))
        block_already_has_summary = any(
            _DATE_RE.search(l) and _AMOUNT_RE.findall(l) for l in current_block
        )
        if has_summary and block_already_has_summary:
            # a new summary line arrived with no Chq/Ref marker in between --
            # close off the previous block before starting the new one
            _emit(current_block)
            current_block = [line]
            continue

        current_block.append(line)

    _emit(current_block)  # trailing block with no closing marker
    return parsed


# CLEANIGNG THE AMOUNT COLUMN
COLUMN_HINTS = {
    'date': ['txn_date','transaction date','tran date','posting date','value date','date'],
    'narration': ['description','narration','particulars','details','remarks','narrative'],
    'debit': ['debit','withdrawal','withdrawl','dr'],
    'credit': ['credit','deposit','cr'],
    'balance': ['balance','closing balance','running balance','opening balance'],
    'amount': ['amount','txn amount','transaction amount','amt'],
    'drcr': ['dr/cr','cr/dr','dc indicator'],
}


def _match_column(header_cell,hints):
    h = (header_cell or "").replace('\n',' ').strip().lower()
    for hint in hints:
        if len(hint)<=2:
            if re.search(rf"\b{hint}\b",h):
                return True
        elif hint in h:
            return True
    return False

def _map_table_columns(header_row):
    col_map = {}
    for idx,cell in enumerate(header_row):
        for role,hints in COLUMN_HINTS.items():
            if role not in col_map and _match_column(cell,hints):
                col_map[role]=idx 
                break
    return col_map 


_MONEY_RE = re.compile(r"^\(?[\d,]+\.\d{1,2}\)?$")
_CURRENCY_PREFIX_RE = re.compile(r"^[₹$]\s*|^(rs\.?|inr)\s*", re.IGNORECASE)
_TRAILING_LABEL_RE = re.compile(r"\s*(cr|dr)\.?$", re.IGNORECASE)
# Some banks wrap the Dr/Cr tag in its own parens instead of appending it
# bare, e.g. "249.00 (Dr)" / "1,396.60 (Cr)". Checked and stripped BEFORE
# the bare-negative-parens check below, since here the parens wrap the tag,
# not the number -- treating them as a negative-number marker would corrupt
# the value (and leave the un-strippable "(Dr)"/"(Cr)" behind, so the row
# used to just get silently dropped).
_PARENTHESIZED_LABEL_RE = re.compile(r"\s*\((cr|dr)\.?\)\s*$", re.IGNORECASE)

def _clean_amount_with_direction(cell):
    """Like _clean_amount, but also returns the DEBIT/CREDIT direction when
    the cell itself carries a Dr/Cr tag (bare trailing or parenthesized) --
    e.g. '249.00 (Dr)', '1,186.69 Cr'. Returns (value, direction), where
    direction is None when the cell carries no such tag."""
    if not cell:
        return None, None
    text = str(cell).strip().replace("\xa0", " ").replace("\u200b", "")
    if not text:
        return None, None
    text = _CURRENCY_PREFIX_RE.sub("", text).strip()

    direction = None
    paren_label = _PARENTHESIZED_LABEL_RE.search(text)
    if paren_label:
        direction = "DEBIT" if paren_label.group(1).lower() == "dr" else "CREDIT"
        text = _PARENTHESIZED_LABEL_RE.sub("", text).strip()
    else:
        trailing_label = _TRAILING_LABEL_RE.search(text)
        if trailing_label:
            direction = "DEBIT" if trailing_label.group(1).lower() == "dr" else "CREDIT"
        text = _TRAILING_LABEL_RE.sub("", text).strip()

    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()
    if not _MONEY_RE.match(text):
        return None, None
    try:
        value = float(text.replace(",", "").strip("()"))
        return (-value if negative else value), direction
    except ValueError:
        return None, None


def _clean_amount(cell):
    """Handles plain numbers as well as bank-formatted variants:
    '₹ 1,186.69', 'Rs.1,186.69', 'INR 1186.69', '1,186.69 Cr', '(100.00)',
    '249.00 (Dr)'. Direction-only callers should use
    _clean_amount_with_direction instead."""
    value, _ = _clean_amount_with_direction(cell)
    return value


# PARSING THE TABLE

def _parse_table_rows(rows: list, col_map: dict) -> List[Dict]:
    parsed = []
    for row in rows:
        if not row or all(not c or str(c).strip() == "" for c in row):
            continue
        date_idx = col_map.get('date')
        if date_idx is None or date_idx >= len(row):
            continue
        date_cell = row[date_idx]
        if not date_cell or not _DATE_RE.search(str(date_cell)):
            continue

        narration = str(row[col_map['narration']]) if col_map.get('narration', -1) < len(row) and 'narration' in col_map else ""
        debit = _clean_amount(row[col_map['debit']]) if 'debit' in col_map and col_map['debit'] < len(row) else None
        credit = _clean_amount(row[col_map['credit']]) if 'credit' in col_map and col_map['credit'] < len(row) else None
        balance = _clean_amount(row[col_map['balance']]) if 'balance' in col_map and col_map['balance'] < len(row) else None

        # Alternate layout: single Amount column + Dr/Cr flag column,
        # instead of split Debit/Credit columns -- a real, common variant.
        # Some banks fold the flag INTO the amount cell itself instead of a
        # separate column, e.g. "249.00 (Dr)" or "1,186.69 Cr" -- that tag,
        # when present, is a direct signal from the source cell and takes
        # priority over a separate drcr column or guessing from narration.
        if debit is None and credit is None and 'amount' in col_map and col_map['amount'] < len(row):
            amount, tag_direction = _clean_amount_with_direction(row[col_map['amount']])
            if amount is not None:
                direction = tag_direction or 'UNKNOWN'
                if direction == 'UNKNOWN' and 'drcr' in col_map and col_map['drcr'] < len(row):
                    direction = _infer_direction_from_flag(row[col_map['drcr']])
                if direction == 'UNKNOWN':
                    direction = _infer_direction_from_narration(narration)
                if direction == 'UNKNOWN':
                    txn_type_guess, _ = _classify_transaction_type(narration)
                    direction = "CREDIT" if txn_type_guess in {"SALARY_CREDIT","INTEREST","REVERSAL","CASH_DEPOSIT"} else "DEBIT"
                if direction == 'CREDIT':
                    credit = amount
                else:
                    debit = amount

        if balance is None or (debit is None and credit is None):
            money_cells = [c for c in row if _clean_amount(c) is not None]
            if balance is None and money_cells:
                balance = _clean_amount(money_cells[-1])
            if debit is None and credit is None and len(money_cells) > 1:
                amount = _clean_amount(money_cells[-2])
                direction = _infer_direction_from_narration(narration)
                if direction == 'UNKNOWN':
                    txn_type_guess, _ = _classify_transaction_type(narration)
                    direction = "CREDIT" if txn_type_guess in {"SALARY_CREDIT","INTEREST","REVERSAL","CASH_DEPOSIT"} else "DEBIT"
                if direction == 'CREDIT':
                    credit = amount
                else:
                    debit = amount

        txn_type, txn_confidence = _classify_transaction_type(narration)
        if txn_type == "OTHER" and credit is not None:
            txn_type = "CREDIT_OTHER"
        elif txn_type == "OTHER" and debit is not None:
            txn_type = "DEBIT_OTHER"

        parsed.append({
            "txn_date_raw": str(date_cell).strip(),
            "narration": narration.strip(),
            "debit_amount": debit,
            "credit_amount": credit,
            "running_balance": balance,
            "txn_type": txn_type,
            "classification_confidence": txn_confidence,
            "classification_method": "table_extract",
        })
    return parsed



def _table_cell_texts(table) -> List[str]:
    """Every non-empty string cell in a table, flattened -- used to test
    whether a 'table' pdfplumber returned is actually a garbled single/
    few-column blob rather than a real row/column grid."""
    texts = []
    for row in table:
        if not row:
            continue
        for cell in row:
            if cell and str(cell).strip():
                texts.append(str(cell))
    return texts


def _is_blob_table(table) -> bool:
    """Detects the case pdfplumber sometimes produces on a page that has
    extra non-tabular content above the real grid (a QR/banner block,
    an account-details block, etc.): instead of a clean row-per-transaction
    grid, the whole page comes back as ONE 'table' with only 1-2 columns,
    where each cell is a giant \\n-joined blob of many transaction lines
    squashed together. A real table (even a headerless continuation page)
    has one row per transaction; a blob table has far fewer rows than the
    number of date+amount pairs actually packed inside its cells."""
    if not table:
        return False
    max_cols = max((len(row) for row in table if row), default=0)
    if max_cols > 2:
        return False
    cell_texts = _table_cell_texts(table)
    packed_txn_lines = sum(
        1 for text in cell_texts for line in text.split("\n")
        if _DATE_RE.search(line) and _AMOUNT_RE.findall(line)
    )
    # more than one txn-looking line packed into a handful of wide cells --
    # a real (even headerless) table would already have split these into
    # separate rows instead of separate \n's inside one cell
    return packed_txn_lines > 1 and packed_txn_lines > len(table)


# A serial-number column ("S.No") glued to the front of each squashed line
# is a common convention in exactly this kind of blob (see the bank example
# that motivated this recovery path: "1 18/07/2025 X80073901 UPIAR/...").
# When present, it's a much sharper block boundary than _parse_lines' generic
# Chq/Ref/summary-line heuristics -- those assume real line-per-field
# wrapping, not one giant multi-record blob, and without this presplit they
# merge unrelated transactions (and leftover header/footer text) into a
# single narration.
_SERIAL_TXN_START_RE = re.compile(
    r"(?m)^\s*\d{1,5}\s+(?=(?:" + "|".join(DATE_PATTERNS) + r"))"
)


def _split_blob_into_records(blob_text: str) -> List[str]:
    """Split a squashed multi-transaction blob into one chunk per
    transaction, using the leading 'S.No <date>' marker as the boundary when
    present. Falls back to returning the whole blob as a single chunk (which
    _parse_lines' own block detection then handles as before) when no such
    marker is found, e.g. a blob that isn't S.No-prefixed."""
    starts = [m.start() for m in _SERIAL_TXN_START_RE.finditer(blob_text)]
    if len(starts) < 2:
        return [blob_text]
    chunks = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(blob_text)
        chunks.append(blob_text[start:end])
    return chunks


def _recover_blob_table(table) -> List[Dict]:
    """A table pdfplumber mis-detected as one/two wide columns instead of a
    real grid still has the actual transaction text -- just squashed with
    \\n's inside its cells instead of split into rows. Flatten every cell's
    text back into lines, split it into one chunk per transaction where a
    serial-number marker allows it, and hand each chunk to the same
    line-based parser used for statements with no detected table at all --
    so a page that fails table detection doesn't silently lose its
    transactions, and doesn't have unrelated records/header/footer text
    bleed into one another either."""
    lines = []
    for text in _table_cell_texts(table):
        lines.extend(text.split("\n"))
    recovered_text = "\n".join(lines)

    rows = []
    for chunk in _split_blob_into_records(recovered_text):
        rows.extend(_parse_lines(chunk))
    for r in rows:
        r["classification_method"] = "raw_text_block_recovered"
    return rows


def _parse_tables(tables_json: str) -> List[Dict]:
    parsed = []
    if not tables_json:
        return parsed
    try:
        tables = json.loads(tables_json)
    except Exception:
        return parsed

    last_good_col_map = None
    for table in tables:
        if not table:
            continue

        col_map = _map_table_columns(table[0])
        has_header = 'date' in col_map

        if has_header:
            last_good_col_map = col_map
            data_rows = table[1:]
        elif last_good_col_map is not None:
            # This table has no detectable header -- likely a continuation
            # page whose header didn't repeat, or a page-boundary artifact
            # corrupted row 0. Reuse the last known-good mapping and treat
            # every row as data; the per-row date check above filters out
            # any stray/garbage rows automatically.
            col_map = last_good_col_map
            data_rows = table
        else:
            # No header ever seen yet in this document -- most likely
            # because THIS is the very first page and it failed table
            # detection entirely (e.g. a QR/account-details banner above
            # the real grid confused pdfplumber's line detection), so
            # there's no last_good_col_map to fall back on either. Rather
            # than silently dropping this page's transactions, check
            # whether it's a garbled blob carrying real transaction text
            # squashed into a couple of wide cells, and recover it via the
            # same regex line-parser used for tableless statements.
            if _is_blob_table(table):
                parsed.extend(_recover_blob_table(table))
            continue

        parsed.extend(_parse_table_rows(data_rows, col_map))

    return parsed


def _txn_signature(txn: Dict):
    """(date_raw, rounded balance) -- stable enough to dedupe/reconcile the
    same real-world transaction seen via two different parse paths (table
    grid vs. raw-text blob), without requiring an exact narration match
    (whitespace/line-join differences make narration text an unreliable key
    across the two paths)."""
    bal = txn.get("running_balance")
    return (txn.get("txn_date_raw"), round(bal, 2) if bal is not None else None)


# PARSING THE TRANSACTION VALUE
def _parse_transactions(raw_text: str, tables_json: str, words_json: str = None) -> List[Dict]:
    rows = _parse_tables(tables_json)
    if not rows:
        rows = _parse_lines(raw_text)
    else:
        # Hardening pass: table detection can silently drop a page (see
        # _is_blob_table above for the known failure mode) without ever
        # returning zero rows overall -- so "rows is non-empty" alone isn't
        # proof nothing was lost. Cross-check against a full line-based
        # parse of raw_text and backfill any transaction whose (date,
        # balance) signature appears there but not in the table-derived
        # rows. Only additions are possible here -- nothing already in
        # `rows` is ever removed or overwritten by this pass.
        table_signatures = {_txn_signature(r) for r in rows}
        for line_txn in _parse_lines(raw_text):
            sig = _txn_signature(line_txn)
            if sig not in table_signatures and sig != (None, None):
                line_txn["classification_method"] = "raw_text_block_backfill"
                rows.append(line_txn)
                table_signatures.add(sig)
    for i, r in enumerate(rows, start=1):
        r['line_no'] = i
    return rows

# DECLARING THE SCHEMA FOR THE TABLE
CLASSIFICATION_SCHEMA = StructType([
    StructField('statement_hash',StringType()),
    StructField('bank_format',StringType()),
    StructField('line_no',IntegerType()),
    StructField('txn_date_raw',StringType()),
    StructField('narration',StringType()),
    StructField('debit_amount',DoubleType()),
    StructField('credit_amount',DoubleType()),
    StructField('running_balance',DoubleType()),
    StructField('txn_type',StringType()),
    StructField('classification_confidence',DoubleType()),
    StructField('classification_method',StringType()),
    StructField('duration_sec',DoubleType()),
    StructField('error_message',StringType())
])


# CLASSIFIYING THE PARTITION
def classify_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Emits normal transaction rows plus one sentinel status row per
    statement (txn_type='__STATUS__') carrying success/failure, duration,
    and the real caught error -- so one bad statement can't crash the whole
    partition, and its failure is fully visible downstream."""
    for batch in iterator:
        out_rows = []
        for _, row in batch.iterrows():
            statement_hash = row["statement_hash"]
            bank_format = row["bank_format"]
            t0 = time.time()
            try:
                txns = _parse_transactions(row["raw_text"], row["tables_json"])
                for txn in txns:
                    out_rows.append({
                        "statement_hash": statement_hash,
                        "bank_format": bank_format,
                        "classification_method": "regex",
                        "duration_sec": None,
                        "error_message": None,
                        **txn,
                    })
                out_rows.append({
                    "statement_hash": statement_hash, "bank_format": bank_format,
                    "line_no": -1, "txn_date_raw": None, "narration": None,
                    "debit_amount": None, "credit_amount": None, "running_balance": None,
                    "txn_type": "__STATUS__", "classification_confidence": None,
                    "classification_method": "SUCCESS" if txns else "SUCCESS_EMPTY",
                    "duration_sec": round(time.time() - t0, 3), "error_message": None,
                })
            except Exception as e:
                out_rows.append({
                    "statement_hash": statement_hash, "bank_format": bank_format,
                    "line_no": -1, "txn_date_raw": None, "narration": None,
                    "debit_amount": None, "credit_amount": None, "running_balance": None,
                    "txn_type": "__STATUS__", "classification_confidence": None,
                    "classification_method": "FAILED",
                    "duration_sec": round(time.time() - t0, 3), "error_message": str(e)[:500],
                })
        yield pd.DataFrame(out_rows, columns=[f.name for f in CLASSIFICATION_SCHEMA.fields])

# CREATING THE SILVER TABLE
spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_SILVER_TRANSACTIONS} (
            statement_hash string,
            bank_format string,
            line_no int,
            txn_date_raw string,
            narration string,
            debit_amount double,
            credit_amount double,
            running_balance double,
            txn_type string,
            classification_confidence double,
            classification_method string,
            classified_at Timestamp
        ) using delta          
        """)

ensure_table_columns(TBL_SILVER_TRANSACTIONS, {
    "statement_hash": "STRING", "bank_format": "STRING", "line_no": "INT",
    "txn_date_raw": "STRING", "narration": "STRING", "debit_amount": "DOUBLE",
    "credit_amount": "DOUBLE", "running_balance": "DOUBLE", "txn_type": "STRING",
    "classification_confidence": "DOUBLE", "classification_method": "STRING",
    "classified_at": "TIMESTAMP",
})


bronze_cpu = (
    spark.table(TBL_BRONZE_EXTRACTION)
    .filter(col('route') == "CPU")
    .select('statement_hash', 'bank_format', 'raw_text', 'tables_json')
)

# log-table-driven eligibility: picks up brand-new statements, statements
# that FAILED classification last time (under the retry cap), and
# statements whose logged classification logic_version is older than
# STAGE_LOGIC_VERSIONS['classification'] in 00_config
bronze_cpu_eligible = get_eligible_statements("classification", bronze_cpu)

# Captured NOW, before log_pipeline_stage()/invalidate_downstream() below
# write anything -- those calls update bsa_pipeline_log for exactly these
# statements, so re-evaluating bronze_cpu_eligible (a lazy DataFrame) AFTER
# they run would re-run get_eligible_statements() against the NEW state and
# find these same statements no-longer-eligible, misreporting "0
# statement(s)" in the summary print below even on a run that classified
# hundreds of lines.
eligible_count = bronze_cpu_eligible.count()

raw_output_df = bronze_cpu_eligible.mapInPandas(classify_partition, schema=CLASSIFICATION_SCHEMA)

# materialize once via staging (serverless has no .cache()/.persist()) --
# log_rows and classified_df below both read back from this SAME staged
# table, so classify_partition never runs twice for the same batch
raw_output_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TBL_SILVER_STAGING)
staged_output_df = spark.table(TBL_SILVER_STAGING)

log_rows = (
    staged_output_df.filter(col("txn_type") == "__STATUS__")
    .select(
        col("statement_hash"),
        col("classification_method").alias("status"),
        col("duration_sec"),
        col("error_message").alias("error"),
    )
)

classified_df = (
    staged_output_df.filter(col("txn_type") != "__STATUS__")
    .drop("duration_sec", "error_message")
    .withColumn("classified_at", current_timestamp())
)

row_count = classified_df.count()
touched_hashes = classified_df.select("statement_hash").distinct()

# Reprocessing a statement means its line count can legitimately change
# (a parsing fix might now produce 43 lines where it used to produce 40,
# or vice versa). A line_no-keyed MERGE can't clean up orphaned old lines
# in that second case, so instead: delete every existing line for a
# touched statement, then insert the fresh set for it. Statements not in
# this batch are left completely untouched.
target_table = DeltaTable.forName(spark, TBL_SILVER_TRANSACTIONS)
(
    target_table.alias("target")
    .merge(touched_hashes.alias("source"), "target.statement_hash = source.statement_hash")
    .whenMatchedDelete()
    .execute()
)
classified_df.write.format("delta").mode("append").saveAsTable(TBL_SILVER_TRANSACTIONS)

log_pipeline_stage(spark, "classification", log_rows)

# only statements that actually got fresh rows written should force
# validation to redo its work
invalidate_downstream("classification", touched_hashes)

print(f"Classified {row_count} transaction line(s) from {eligible_count} statement(s) -> {TBL_SILVER_TRANSACTIONS}")

# THIS RUN'S STATEMENTS -- scoped to the statement_hashes this run actually
# wrote fresh classification output for, not the whole table's history.
print(f"=== transaction_classification run summary (run_id={RUN_ID}) ===")
display(
    spark.table(TBL_SILVER_TRANSACTIONS)
    .join(touched_hashes, on="statement_hash", how="inner")
    .groupBy("statement_hash", "bank_format")
    .agg(
        F.count("*").alias("line_count"),
        F.sum(F.when(col("txn_type") == "OTHER", 1).otherwise(0)).alias("unclassified_other_count"),
    )
    .orderBy(col("statement_hash"))
)

# OVERALL TABLE HEALTH -- small aggregate for context, not a substitute for
# the per-run view above
print("=== bsa_classified_transactions overall txn_type breakdown ===")
display(
    spark.table(TBL_SILVER_TRANSACTIONS)
    .groupBy('txn_type')
    .count()
    .orderBy(col('count').desc())
)
