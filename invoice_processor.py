#!/usr/bin/env python3
"""
invoice_processor.py
--------------------
Polls a Google Drive folder (InvoicesToProcess) for new invoice PDFs,
extracts structured data via Gemini LLM, writes results to Google Sheets,
renames + moves files in Google Drive.

Setup: read SETUP.md before running.
"""

import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pdfplumber
import google.generativeai as genai
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ─────────────────────────────────────────────
#  Load environment
# ─────────────────────────────────────────────

load_dotenv()

# ─────────────────────────────────────────────
#  Configuration  ← edit .env or set env vars
# ─────────────────────────────────────────────

GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "your_key_here")
GEMINI_MODEL     = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Path to Google service account JSON key file
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# Google Sheets spreadsheet ID  (from the URL: /d/<SPREADSHEET_ID>/edit)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "YOUR_SPREADSHEET_ID_HERE")

# Google Drive folder IDs
FOLDER_TO_PROCESS = os.getenv("FOLDER_TO_PROCESS", "1YIUagn9cyzHZejCAVX6LYjaTKyO6GCa9")
FOLDER_EXTRACTED  = os.getenv("FOLDER_EXTRACTED",  "108tJWt8pb83ESdtwYSqtSZVtS11weV4F")
FOLDER_ERRORS     = os.getenv("FOLDER_ERRORS",     "1JClebDM8_cBGXsuuJlTTmSSCnrDH6fkd")
FOLDER_CLOSED     = os.getenv("FOLDER_CLOSED", "")
FOLDER_QUARANTINE = os.getenv("FOLDER_QUARANTINE", "")

# How often to check for new files (seconds)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

# Local files for idempotency and invoice/file linking
PROCESSED_STATE_FILE = Path(__file__).parent / "processed_state.json"
INVOICE_REGISTRY_FILE = Path(__file__).parent / "invoice_registry.json"

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Company-type suffixes to remove from supplier name (order: longest first)
COMPANY_SUFFIXES = [
    r"\s*,?\s*š\.\s*p\.",
    r"\s*,?\s*s\.\s*r\.\s*o\.",
    r"\s*,?\s*s\.\s*d\.",
    r"\s*,?\s*a\.\s*s\.",
]

CANONICAL_FIELDS = {
    "invoice_supplier_name",
    "invoice_id",
    "invoice_date_issued",
    "invoice_date_due",
    "invoice_price_total",
    "invoice_currency",
    "line_items",
}

RENAME_REQUIRED_FIELDS = (
    "invoice_supplier_name",
    "invoice_id",
    "invoice_date_due",
)

FULL_INVOICE_REQUIRED_FIELDS = (
    "invoice_supplier_name",
    "invoice_id",
    "invoice_date_due",
    "invoice_date_issued",
    "invoice_price_total",
    "invoice_currency",
)

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("invoice_processor")

# ─────────────────────────────────────────────
#  Google API clients
# ─────────────────────────────────────────────

def build_google_clients():
    """Build authenticated Drive and Sheets API clients from service account."""
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE, scopes=SCOPES
    )
    drive   = build("drive",   "v3", credentials=creds, cache_discovery=False)
    sheets  = build("sheets",  "v4", credentials=creds, cache_discovery=False)
    return drive, sheets

# ─────────────────────────────────────────────
#  Processed IDs persistence
# ─────────────────────────────────────────────

def _load_json_file(path: Path, default: Any):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return default


def _save_json_file(path: Path, payload: Any):
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_processed_state() -> dict[str, dict[str, str]]:
    return _load_json_file(PROCESSED_STATE_FILE, {})


def save_processed_state(state: dict[str, dict[str, str]]):
    _save_json_file(PROCESSED_STATE_FILE, state)


def load_invoice_registry() -> dict[str, dict[str, str]]:
    return _load_json_file(INVOICE_REGISTRY_FILE, {})


def save_invoice_registry(registry: dict[str, dict[str, str]]):
    _save_json_file(INVOICE_REGISTRY_FILE, registry)

# ─────────────────────────────────────────────
#  Google Drive helpers
# ─────────────────────────────────────────────

def list_pdfs_in_folder(drive, folder_id: str) -> list[dict]:
    """Return list of PDF file metadata dicts in a Drive folder."""
    query = (
        f"'{folder_id}' in parents "
        f"and mimeType='application/pdf' "
        f"and trashed=false"
    )
    results = (
        drive.files()
        .list(
            q=query,
            fields="files(id, name, webViewLink, md5Checksum, modifiedTime)",
            pageSize=100,
        )
        .execute()
    )
    return results.get("files", [])


def download_pdf(drive, file_id: str) -> bytes:
    """Download a Drive file and return its bytes."""
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def rename_and_move_file(drive, file_id: str, new_name: str,
                          dest_folder_id: str, src_folder_id: str):
    """Rename a Drive file and move it to dest_folder_id."""
    drive.files().update(
        fileId=file_id,
        body={"name": new_name},
        addParents=dest_folder_id,
        removeParents=src_folder_id,
        fields="id, name, parents",
    ).execute()


def move_file(drive, file_id: str, dest_folder_id: str, src_folder_id: str):
    """Move a Drive file without renaming."""
    drive.files().update(
        fileId=file_id,
        addParents=dest_folder_id,
        removeParents=src_folder_id,
        fields="id, parents",
    ).execute()


def is_new_or_changed(file_meta: dict, processed_state: dict[str, dict[str, str]]) -> bool:
    file_id = file_meta["id"]
    current = {
        "md5Checksum": file_meta.get("md5Checksum", ""),
        "modifiedTime": file_meta.get("modifiedTime", ""),
    }
    previous = processed_state.get(file_id)
    if not previous:
        return True
    return (
        previous.get("md5Checksum") != current["md5Checksum"]
        or previous.get("modifiedTime") != current["modifiedTime"]
    )


def update_processed_state(file_meta: dict, processed_state: dict[str, dict[str, str]]):
    processed_state[file_meta["id"]] = {
        "md5Checksum": file_meta.get("md5Checksum", ""),
        "modifiedTime": file_meta.get("modifiedTime", ""),
        "lastProcessedAt": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

# ─────────────────────────────────────────────
#  Google Sheets helpers
# ─────────────────────────────────────────────

DOCS_PROCESSED_HEADERS = [
    "processed_at",
    "status",
    "error_reason",
    "internal_number",
    "document_gdrive_link",
    "document_name_original",
    "document_name_changed",
    "document_folder_changed",
    "invoice_supplier_name",
    "invoice_id",
    "invoice_date_issued",
    "invoice_date_due",
    "invoice_price_total",
    "invoice_currency",
]

INVOICE_ITEMS_HEADERS = [
    "invoice_supplier_name",
    "invoice_id",
    "invoice_date_issued",
    "invoice_date_due",
    "invoice_price_total",
    "invoice_currency",
    "stay_product_name",
    "stay_order_id",
    "stay_client_name",
    "stay_date_start",
    "stay_date_end",
    "stay_price",
]

INVOICES_TO_CLOSE_HEADERS = [
    "invoice_id",
    "internal_number",
    "processed_at",
]


def ensure_sheet_headers(sheets, spreadsheet_id: str):
    """Create sheets and write headers if they don't exist yet."""
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_titles = {s["properties"]["title"] for s in meta.get("sheets", [])}

    requests = []
    for title in ("DocsProcessed", "InvoiceItemsList", "InvoicesToClose"):
        if title not in existing_titles:
            requests.append({
                "addSheet": {"properties": {"title": title}}
            })

    if requests:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    # Write headers only if the sheet is empty (row 1 is blank)
    def write_if_empty(sheet_name, headers):
        result = (
            sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1:A1")
            .execute()
        )
        if not result.get("values"):
            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body={"values": [headers]},
            ).execute()

    write_if_empty("DocsProcessed", DOCS_PROCESSED_HEADERS)
    write_if_empty("InvoiceItemsList", INVOICE_ITEMS_HEADERS)
    write_if_empty("InvoicesToClose", INVOICES_TO_CLOSE_HEADERS)


def append_row(sheets, spreadsheet_id: str, sheet_name: str, row: list):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def write_docs_processed(sheets, spreadsheet_id: str, data: dict,
                          original_name: str, new_name: str,
                          folder_label: str, gdrive_link: str,
                          status: str, error_reason: str = "",
                          internal_number: str = ""):
    row = [
        datetime.utcnow().isoformat(timespec="seconds") + "Z",
        status,
        error_reason,
        internal_number,
        gdrive_link,
        original_name,
        new_name,
        folder_label,
        data.get("invoice_supplier_name") or "",
        data.get("invoice_id") or "",
        data.get("invoice_date_issued") or "",
        data.get("invoice_date_due") or "",
        str(data.get("invoice_price_total") or ""),
        data.get("invoice_currency") or "",
    ]
    append_row(sheets, spreadsheet_id, "DocsProcessed", row)


def write_invoice_items(sheets, spreadsheet_id: str, data: dict):
    line_items = data.get("line_items") or []
    if not line_items:
        return

    if len(line_items) == 1:
        only_item = line_items[0]
        if only_item.get("stay_price") is None and data.get("invoice_price_total") is not None:
            only_item["stay_price"] = data.get("invoice_price_total")

    for item in line_items:
        row = [
            data.get("invoice_supplier_name") or "",
            data.get("invoice_id") or "",
            data.get("invoice_date_issued") or "",
            data.get("invoice_date_due") or "",
            str(data.get("invoice_price_total") or ""),
            data.get("invoice_currency") or "",
            item.get("stay_product_name") or "",
            str(item.get("stay_order_id") or ""),
            item.get("stay_client_name") or "",
            item.get("stay_date_start") or "",
            item.get("stay_date_end") or "",
            str(item.get("stay_price") or ""),
        ]
        append_row(sheets, spreadsheet_id, "InvoiceItemsList", row)


def get_pending_closures(sheets, spreadsheet_id: str) -> list[dict]:
    result = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range="InvoicesToClose!A2:C")
        .execute()
    )
    rows = result.get("values", [])
    pending = []
    for idx, row in enumerate(rows, start=2):
        invoice_id = row[0].strip() if len(row) > 0 else ""
        internal_number = row[1].strip() if len(row) > 1 else ""
        processed_at = row[2].strip() if len(row) > 2 else ""
        if invoice_id and internal_number and not processed_at:
            pending.append(
                {
                    "row": idx,
                    "invoice_id": invoice_id,
                    "internal_number": internal_number,
                }
            )
    return pending


def mark_closure_processed(sheets, spreadsheet_id: str, row_index: int):
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"InvoicesToClose!C{row_index}",
        valueInputOption="RAW",
        body={"values": [[datetime.utcnow().isoformat(timespec="seconds") + "Z"]]},
    ).execute()

# ─────────────────────────────────────────────
#  PDF text extraction
# ─────────────────────────────────────────────

def extract_pdf_text(pdf_bytes: bytes) -> str:
    pages = []
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name
    try:
        with pdfplumber.open(tmp_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                pages.append(f"--- Page {i+1} ---\n{text}")
    finally:
        os.unlink(tmp_path)
    return "\n\n".join(pages)

# ─────────────────────────────────────────────
#  Gemini extraction
# ─────────────────────────────────────────────

EXTRACTION_PROMPT = """You are an expert data extraction system. Your task is to extract specified information - invoice parameters - from the provided PDF text.

The invoice can be in Slovak, Czech, English or Hungarian language.

Input: Text extracted from a PDF document representing a commercial invoice. If it is a multipage document, there is a chance that the document contains more than one invoice.

If the invoice item list contains only 1 client = 1 item, and value of the field "stay_price" is not clear, use value of "invoice_price_total" instead.

Output Format: provide ONLY the extracted data as a valid JSON object. No markdown, no code fences, no explanation — just raw JSON.

INVOICE BASIC FIELDS

invoice_supplier_name: name of the company issuing the invoice, labeled often as "Dodávateľ" or "Dodavatel"

invoice_id: labeled as "Číslo faktúry" (e.g., INV-2023-001), usually in the upper right section

invoice_date_issued: labeled as "Dátum vystavenia" (format: DD.MM.YYYY)

invoice_date_due: labeled as "Dátum splatnosti" (format: DD.MM.YYYY)

invoice_price_total: total sum of the invoice labeled as: Celkom, Spolu, K úhrade (output as plain number, no currency symbols)

invoice_currency: one of Euro, €, CZK, Kč, HUF, Forint. Output: € for Euro/€; CZK for CZK/Kč; HUF for HUF/Forint.

INVOICE ITEM FIELDS

Each invoice item represents a hotel stay with or without extra services. May be in a table labeled "Príloha k faktúre".
If more than one service is assigned to the same client, treat it as a package (one item) and use the total package price.

stay_client_name: labeled as: Meno, Jméno, Name, Priezvisko a meno, Meno a priezvisko, Jméno a příjmení, Příjmení a jméno, Hosť, Hostia

stay_date_start: date DD.MM.YYYY, may be part of a period string "DD.MM - DD.MM.YYYY". Labeled as: Od, Dátum od

stay_date_end: date DD.MM.YYYY, may be part of a period string. Labeled as: Do, Dátum do

stay_price: total item price. Labeled as: Celkom, Spolu, Čiastka (output as plain number)

stay_order_id: 7-digit number starting with 32, e.g. 3295492. Labeled as: Objednávka, Číslo objednávky

stay_product_name: text. Labeled as: Pobyt, Balík

AFTER EXTRACTION
Put item fields in an array named line_items. Each element:
{
    "stay_client_name": string or null,
    "stay_date_start": string or null,
    "stay_date_end": string or null,
  "stay_price": number or null,
  "stay_product_name": string or null,
  "stay_order_id": number or null
}

INSTRUCTIONS & ERROR HANDLING:
- If a field is not found, set its value to null.
- All numeric values must be plain numbers (no currency symbols).
- If the document is clearly not an invoice, return an empty JSON object {}.

PDF TEXT:
{pdf_text}
"""

REPAIR_PROMPT = """Return ONLY valid JSON matching exactly this schema:
{
  "invoice_supplier_name": string|null,
  "invoice_id": string|null,
  "invoice_date_issued": "DD.MM.YYYY"|null,
  "invoice_date_due": "DD.MM.YYYY"|null,
  "invoice_price_total": number|null,
  "invoice_currency": "€"|"CZK"|"HUF"|null,
  "line_items": [
    {
      "stay_client_name": string|null,
      "stay_date_start": "DD.MM.YYYY"|null,
      "stay_date_end": "DD.MM.YYYY"|null,
      "stay_price": number|null,
      "stay_product_name": string|null,
      "stay_order_id": number|null
    }
  ]
}
If clearly not invoice, return {}.

PDF TEXT:
{pdf_text}
"""


def normalize_currency(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"€", "EUR", "EURO"}:
        return "€"
    if text in {"CZK", "KČ", "KC"}:
        return "CZK"
    if text in {"HUF", "FORINT"}:
        return "HUF"
    return None


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[^0-9,.-]", "", text)
    if text.count(",") and text.count("."):
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "")
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    elif text.count(",") and not text.count("."):
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_date(value: Any, fallback_year: int | None = None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    text = text.replace("/", ".").replace("-", ".")
    text = re.sub(r"\s+", "", text)

    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%d.%m.%Y")
        except ValueError:
            continue

    # Day and month only
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.?$", text)
    if m and fallback_year:
        try:
            candidate = datetime(
                year=fallback_year,
                month=int(m.group(2)),
                day=int(m.group(1)),
            )
            return candidate.strftime("%d.%m.%Y")
        except ValueError:
            return None
    return None


def _get_alias(data: dict, *keys: str):
    for key in keys:
        if key in data and data.get(key) not in ("", []):
            return data.get(key)
    return None


def normalize_line_item(item: dict, fallback_year: int | None) -> dict:
    stay_price = parse_number(_get_alias(item, "stay_price"))
    stay_order_raw = _get_alias(item, "stay_order_id")
    stay_order_id = None
    if stay_order_raw is not None:
        digits = re.sub(r"\D", "", str(stay_order_raw))
        if digits:
            stay_order_id = int(digits)

    return {
        "stay_client_name": _get_alias(item, "stay_client_name", "stay_clients_name"),
        "stay_date_start": normalize_date(
            _get_alias(item, "stay_date_start", "stay_start_date"),
            fallback_year,
        ),
        "stay_date_end": normalize_date(
            _get_alias(item, "stay_date_end", "stay_end_date"),
            fallback_year,
        ),
        "stay_price": stay_price,
        "stay_product_name": _get_alias(item, "stay_product_name"),
        "stay_order_id": stay_order_id,
    }


def normalize_extraction(raw_data: dict) -> dict:
    issue_date = normalize_date(
        _get_alias(raw_data, "invoice_date_issued", "invoice_date_of_issue")
    )
    due_date = normalize_date(_get_alias(raw_data, "invoice_date_due", "invoice_due_date"))

    fallback_year = None
    if due_date:
        fallback_year = datetime.strptime(due_date, "%d.%m.%Y").year
    elif issue_date:
        fallback_year = datetime.strptime(issue_date, "%d.%m.%Y").year

    line_items = raw_data.get("line_items")
    if not isinstance(line_items, list):
        line_items = []

    normalized_items = [
        normalize_line_item(item, fallback_year)
        for item in line_items
        if isinstance(item, dict)
    ]

    normalized = {
        "invoice_supplier_name": _get_alias(raw_data, "invoice_supplier_name"),
        "invoice_id": _get_alias(raw_data, "invoice_id", "invoice_document_id"),
        "invoice_date_issued": issue_date,
        "invoice_date_due": due_date,
        "invoice_price_total": parse_number(_get_alias(raw_data, "invoice_price_total")),
        "invoice_currency": normalize_currency(_get_alias(raw_data, "invoice_currency")),
        "line_items": normalized_items,
    }

    if len(normalized["line_items"]) == 1 and normalized["line_items"][0].get("stay_price") is None:
        normalized["line_items"][0]["stay_price"] = normalized.get("invoice_price_total")

    return normalized


def likely_multi_invoice(pdf_text: str) -> bool:
    patterns = [
        r"(?:Číslo\s+faktúry|Čislo\s+faktúry|Cislo\s+faktury|Invoice\s*No\.?|Faktúra\s*č\.?|Faktura\s*č\.?)",
        r"\b(?:INV[-\s]?\d{2,}|\d{6,})\b",
    ]
    score = 0
    for pattern in patterns:
        score += len(re.findall(pattern, pdf_text, flags=re.IGNORECASE))
    return score >= 4


def extraction_looks_suspicious(data: dict) -> bool:
    if not data:
        return True
    if not isinstance(data, dict):
        return True
    items = data.get("line_items") or []
    if not isinstance(items, list):
        return True
    if items and all(
        not any(item.get(field) for field in (
            "stay_client_name",
            "stay_date_start",
            "stay_date_end",
            "stay_price",
            "stay_product_name",
            "stay_order_id",
        ))
        for item in items
        if isinstance(item, dict)
    ):
        return True
    return False


def missing_required_fields(data: dict, fields: tuple[str, ...]) -> list[str]:
    missing = []
    for field in fields:
        value = data.get(field)
        if value is None:
            missing.append(field)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(field)
    return missing


def parse_model_json(raw_text: str) -> dict:
    raw = (raw_text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.MULTILINE)
        raw = re.sub(r"```$", "", raw.strip())
    return json.loads(raw)


def call_gemini(pdf_text: str) -> tuple[dict, str]:
    model = genai.GenerativeModel(GEMINI_MODEL)

    first_prompt = EXTRACTION_PROMPT.replace("{pdf_text}", pdf_text)
    first_response = model.generate_content(first_prompt)
    first_raw = (first_response.text or "").strip()

    try:
        first_data = parse_model_json(first_raw)
        return first_data, ""
    except json.JSONDecodeError:
        pass

    repair_prompt = REPAIR_PROMPT.replace("{pdf_text}", pdf_text)
    second_response = model.generate_content(repair_prompt)
    second_raw = (second_response.text or "").strip()

    try:
        second_data = parse_model_json(second_raw)
        return second_data, ""
    except json.JSONDecodeError as e:
        log.error("JSON parse error after retry: %s | Raw: %s", e, second_raw[:300])
        return {}, "INVALID_JSON"

# ─────────────────────────────────────────────
#  Filename builder
# ─────────────────────────────────────────────

def clean_supplier_name(name: str) -> str:
    cleaned = name
    for pattern in COMPANY_SUFFIXES:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def parse_date_to_yyyymmdd(date_str: str) -> str | None:
    if not date_str:
        return None
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    return None


def sanitize_filename(text: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", str(text)).strip()


def build_new_filename(data: dict) -> str | None:
    """FA_<YYYYMMDD>_<supplier_cleaned>_SID_<invoice_id>.pdf"""
    supplier = data.get("invoice_supplier_name") or ""
    doc_id   = data.get("invoice_id") or ""
    due_date = data.get("invoice_date_due") or ""

    if not supplier or not doc_id or not due_date:
        return None

    yyyymmdd = parse_date_to_yyyymmdd(due_date)
    if not yyyymmdd:
        return None

    cleaned  = sanitize_filename(clean_supplier_name(supplier)).replace(" ", "-")
    safe_id  = sanitize_filename(doc_id)
    return f"FA_{yyyymmdd}_{cleaned}_SID_{safe_id}.pdf"


def has_obligatory_invoice_fields(data: dict) -> bool:
    return len(missing_required_fields(data, FULL_INVOICE_REQUIRED_FIELDS)) == 0


def build_closed_filename(current_name: str, internal_number: str) -> str:
    base = current_name[:-4] if current_name.lower().endswith(".pdf") else current_name
    if re.search(r"_INT_[^_]+$", base):
        base = re.sub(r"_INT_[^_]+$", "", base)
    return f"{base}_INT_{sanitize_filename(internal_number)}.pdf"

# ─────────────────────────────────────────────
#  Core pipeline
# ─────────────────────────────────────────────

def process_file(drive, sheets, file_meta: dict, invoice_registry: dict[str, dict[str, str]]):
    """Full pipeline for a single Drive PDF file."""
    file_id       = file_meta["id"]
    original_name = file_meta["name"]
    gdrive_link   = file_meta.get("webViewLink", "")
    log.info("Processing: %s (%s)", original_name, file_id)

    def move_with_log(dest_folder_id: str, folder_label: str, status: str,
                      error_reason: str, data: dict | None = None,
                      changed_name: str | None = None):
        move_file(drive, file_id, dest_folder_id, FOLDER_TO_PROCESS)
        write_docs_processed(
            sheets,
            SPREADSHEET_ID,
            data or {},
            original_name,
            changed_name or original_name,
            folder_label,
            gdrive_link,
            status=status,
            error_reason=error_reason,
        )
        log.warning("  → Moved to %s: %s | reason=%s", folder_label, original_name, error_reason)

    def move_to_errors(data=None, reason=""):
        move_with_log(
            dest_folder_id=FOLDER_ERRORS,
            folder_label="Invoices Errors",
            status="ERROR",
            error_reason=reason,
            data=data,
        )

    def move_to_quarantine(data=None, reason=""):
        folder_id = FOLDER_QUARANTINE or FOLDER_ERRORS
        label = "Invoices Quarantine" if FOLDER_QUARANTINE else "Invoices Errors"
        move_with_log(
            dest_folder_id=folder_id,
            folder_label=label,
            status="QUARANTINED",
            error_reason=reason,
            data=data,
        )

    # 1. Download
    try:
        pdf_bytes = download_pdf(drive, file_id)
    except Exception as e:
        log.error("Download failed: %s", e)
        move_to_errors(reason="DOWNLOAD_FAILED")
        return

    # 2. Extract text
    try:
        pdf_text = extract_pdf_text(pdf_bytes)
    except Exception as e:
        log.error("PDF read failed: %s", e)
        move_to_errors(reason="PDF_READ_FAILED")
        return

    if not pdf_text.strip():
        log.warning("No text in PDF — moving to Errors.")
        move_to_errors(reason="EMPTY_PDF_TEXT")
        return

    if likely_multi_invoice(pdf_text):
        log.warning("Possible multi-invoice document — moving to Quarantine.")
        move_to_quarantine(reason="MULTI_INVOICE_UNSUPPORTED")
        return

    # 3. Gemini extraction
    try:
        raw_data, gemini_error = call_gemini(pdf_text)
    except Exception as e:
        log.error("Gemini call failed: %s", e)
        move_to_errors(reason="GEMINI_CALL_FAILED")
        return

    if gemini_error:
        move_to_quarantine(reason=gemini_error)
        return

    if not raw_data:
        log.warning("Not an invoice — moving to Errors.")
        move_to_errors(reason="NOT_AN_INVOICE")
        return

    data = normalize_extraction(raw_data)

    if extraction_looks_suspicious(data):
        log.warning("Suspicious extraction output — moving to Quarantine.")
        move_to_quarantine(data=data, reason="SUSPICIOUS_EXTRACTION")
        return

    if all(data.get(k) is None for k in ("invoice_supplier_name", "invoice_id", "invoice_date_due")):
        log.warning("Not an invoice — moving to Errors.")
        move_to_errors(data=data, reason="NOT_AN_INVOICE")
        return

    # 4. Build new filename (checks rename-obligatory fields)
    missing_for_rename = missing_required_fields(data, RENAME_REQUIRED_FIELDS)
    if missing_for_rename:
        reason = f"MISSING_RENAME_FIELDS:{','.join(missing_for_rename)}"
        log.warning("Missing rename-obligatory fields — moving to Errors.")
        move_to_errors(data=data, reason=reason)
        return

    new_filename = build_new_filename(data)
    if not new_filename:
        log.warning("Missing rename-obligatory fields — moving to Errors.")
        move_to_errors(data=data, reason="INVALID_RENAME_FIELDS")
        return

    # 5. Check full invoice data
    has_full_data = has_obligatory_invoice_fields(data)
    missing_full_fields = missing_required_fields(data, FULL_INVOICE_REQUIRED_FIELDS)

    # 6. Rename + move to Extracted
    try:
        rename_and_move_file(drive, file_id, new_filename,
                             FOLDER_EXTRACTED, FOLDER_TO_PROCESS)
        log.info("  → Renamed & moved: %s", new_filename)
    except Exception as e:
        log.error("Drive move failed: %s", e)
        move_to_errors(data=data, reason="DRIVE_RENAME_MOVE_FAILED")
        return

    invoice_id = str(data.get("invoice_id") or "").strip()
    if invoice_id:
        invoice_registry[invoice_id] = {
            "file_id": file_id,
            "name": new_filename,
            "folder": "extracted",
            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }

    # 7. Write DocsProcessed
    write_docs_processed(
        sheets, SPREADSHEET_ID, data,
        original_name, new_filename, "Invoices Extracted", gdrive_link,
        status="EXTRACTED",
        error_reason="",
    )

    # 8. Write InvoiceItemsList (only if full data present)
    if has_full_data:
        write_invoice_items(sheets, SPREADSHEET_ID, data)
        log.info("  → %d line item(s) written to InvoiceItemsList.",
                 len(data.get("line_items") or []))
    else:
        log.warning(
            "  → Missing full invoice fields — InvoiceItemsList skipped: %s",
            ",".join(missing_full_fields),
        )

    log.info("  Done: %s", original_name)


def process_invoice_closures(drive, sheets, invoice_registry: dict[str, dict[str, str]]):
    if not FOLDER_CLOSED:
        return

    try:
        pending = get_pending_closures(sheets, SPREADSHEET_ID)
    except Exception as e:
        log.error("Failed to read InvoicesToClose sheet: %s", e)
        return

    if not pending:
        return

    log.info("Found %d pending closure item(s).", len(pending))
    for close_item in pending:
        invoice_id = close_item["invoice_id"]
        internal_number = close_item["internal_number"]
        row_index = close_item["row"]

        reg_item = invoice_registry.get(invoice_id)
        if not reg_item:
            log.warning("Closure skipped, invoice_id not found in registry: %s", invoice_id)
            continue

        file_id = reg_item.get("file_id")
        current_name = reg_item.get("name", "")
        if not file_id:
            log.warning("Closure skipped, missing file_id for invoice_id: %s", invoice_id)
            continue

        new_closed_name = build_closed_filename(current_name, internal_number)
        try:
            rename_and_move_file(
                drive,
                file_id,
                new_closed_name,
                FOLDER_CLOSED,
                FOLDER_EXTRACTED,
            )
        except Exception as e:
            log.error("Closure move failed for invoice_id=%s: %s", invoice_id, e)
            continue

        reg_item["name"] = new_closed_name
        reg_item["folder"] = "closed"
        reg_item["internal_number"] = internal_number
        reg_item["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        write_docs_processed(
            sheets,
            SPREADSHEET_ID,
            {
                "invoice_id": invoice_id,
                "invoice_supplier_name": "",
                "invoice_date_issued": "",
                "invoice_date_due": "",
                "invoice_price_total": "",
                "invoice_currency": "",
            },
            original_name=current_name,
            new_name=new_closed_name,
            folder_label="Invoices Closed",
            gdrive_link="",
            status="CLOSED",
            error_reason="",
            internal_number=internal_number,
        )

        try:
            mark_closure_processed(sheets, SPREADSHEET_ID, row_index)
        except Exception as e:
            log.error("Failed to mark closure row %s: %s", row_index, e)
            continue

        log.info("Closed invoice moved: %s -> %s", current_name, new_closed_name)

# ─────────────────────────────────────────────
#  Main loop
# ─────────────────────────────────────────────

def main():
    # Preflight checks
    if GEMINI_API_KEY in ("YOUR_API_KEY_HERE", "YOUR_GEMINI_API_KEY", "your_key_here", ""):
        log.error("GEMINI_API_KEY not set. Copy .env.example to .env and add your key.")
        return
    if SPREADSHEET_ID in ("YOUR_SPREADSHEET_ID_HERE", "YOUR_SPREADSHEET_ID", ""):
        log.error("SPREADSHEET_ID not set in .env.")
        return
    if not Path(GOOGLE_CREDENTIALS_FILE).exists():
        log.error("credentials.json not found. See SETUP.md.")
        return

    genai.configure(api_key=GEMINI_API_KEY)

    try:
        drive, sheets = build_google_clients()
    except Exception as e:
        log.error("Failed to build Google API clients: %s", e)
        return

    # Ensure spreadsheet has correct sheets + headers
    try:
        ensure_sheet_headers(sheets, SPREADSHEET_ID)
    except Exception as e:
        log.error("Spreadsheet setup failed: %s", e)
        return

    processed_state = load_processed_state()
    invoice_registry = load_invoice_registry()

    log.info("Invoice Processor started.")
    log.info("  Polling folder : %s  (every %ds)", FOLDER_TO_PROCESS, POLL_INTERVAL)
    log.info("  Spreadsheet   : %s", SPREADSHEET_ID)

    while True:
        try:
            files = list_pdfs_in_folder(drive, FOLDER_TO_PROCESS)
            new_files = [f for f in files if is_new_or_changed(f, processed_state)]

            if new_files:
                log.info("Found %d new PDF(s).", len(new_files))
                for file_meta in new_files:
                    try:
                        process_file(drive, sheets, file_meta, invoice_registry)
                    except Exception as e:
                        log.error("Unhandled error for '%s': %s",
                                  file_meta.get("name"), e)
                    finally:
                        update_processed_state(file_meta, processed_state)
                        save_processed_state(processed_state)
                        save_invoice_registry(invoice_registry)
            else:
                log.debug("No new files. Sleeping %ds…", POLL_INTERVAL)

            process_invoice_closures(drive, sheets, invoice_registry)
            save_invoice_registry(invoice_registry)

        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
