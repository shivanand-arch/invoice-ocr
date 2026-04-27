import streamlit as st
import anthropic
import base64
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
import io
import csv
import zipfile
import pandas as pd
from pypdf import PdfReader, PdfWriter
import gspread
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import Flow

# ─── Page Config ───
st.set_page_config(
    page_title="Invoice OCR - Exotel",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Paths ───
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
VENDOR_MAPPINGS_FILE = DATA_DIR / "vendor_mappings.json"
ACCOUNT_MAPPINGS_FILE = DATA_DIR / "account_mappings.json"
PROCESSED_INVOICES_FILE = DATA_DIR / "processed_invoices.json"
CREDIT_OVERRIDES_FILE = DATA_DIR / "credit_overrides.json"
MASTER_SHEET_FILE = DATA_DIR / "master_sheet_mappings.json"

# ─── Large PDF Handling Config ───
# Invoice summary data is on the first/last few pages; CDR pages are skipped
MAX_PAGES_FOR_CLAUDE = 25  # Max pages to send to Claude in one request
FRONT_PAGES = 10           # First N pages to always include
BACK_PAGES = 5             # Last N pages to always include

# ─── CSS ───
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    .stApp { font-family: 'Inter', sans-serif; }
    .top-bar {
        background: white;
        padding: 0.8rem 1.5rem;
        border-bottom: 2px solid #f0f0f0;
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1rem;
    }
    .top-bar-title {
        font-size: 1.4rem;
        font-weight: 700;
        background: linear-gradient(90deg, #1a1a2e, #16213e, #0f3460);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .status-badge {
        background: #e8f5e9;
        color: #2e7d32;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="top-bar">
    <span class="top-bar-title">📄 Invoice OCR & Auto-Posting System</span>
    <span class="status-badge">● Online — Sonnet</span>
</div>
""", unsafe_allow_html=True)

# ─── Google OAuth (exotel.com only) ───
ALLOWED_DOMAIN = "exotel.com"

_user = st.user if hasattr(st, "user") else getattr(st, "experimental_user", None)
_is_logged_in = getattr(_user, "is_logged_in", False) if _user else False
user_email = (getattr(_user, "email", None) or "") if _user else ""

if not _is_logged_in:
    st.markdown("### Please sign in with your Exotel Google account")
    st.login()
    st.stop()

if not user_email.endswith(f"@{ALLOWED_DOMAIN}"):
    st.error(f"Access restricted to @{ALLOWED_DOMAIN} accounts. You are signed in as {user_email}.")
    st.logout()
    st.stop()

st.caption(f"Signed in as **{user_email}**")
if st.sidebar.button("Sign out"):
    st.logout()

# ─── Tracker Column Definitions ───
# (OAuth callback handler is defined later; called once UI is ready below.)

TRACKER_COLUMNS = [
    "SNO", "Date", "PI NO", "Tracker No", "Cmp", "Cogs/Noncogs", "Vendor no.",
    "Product", "Circle", "Description", "Account No", "InvoiceNo", "Repeat Number",
    "Received dt", "Due Date", "Inv dt", "Start Date", "End dt", "Invoice Value",
    "GST/BC", "Previous Balance", "Paid date", "Payment ID", "Remarks",
    "SACK use /", "Sack Value use /", "AER",
    "Difference b/w Invoice Value & Previous Outstanding",
    "Intercomapny Billing", "Vendor submit date", "Invoice Upload", "Ageing",
    "Submit SLA", "Upload SLA", "Payment SLA", "count of SLA",
    "AC_update", "Final Approval", "Booking Status", "Exotel", "Veeno",
]

# Columns shown in the editable table (subset that matters)
DISPLAY_COLUMNS = [
    "SNO", "Date", "Cmp", "Cogs/Noncogs", "Vendor no.",
    "Product", "Circle", "Description", "Account No", "InvoiceNo",
    "Repeat Number", "Received dt", "Due Date", "Inv dt", "Start Date", "End dt",
    "Invoice Value", "GST/BC", "Previous Balance", "Remarks",
]


# ─── Data Helpers ───
def load_json(path, default=None):
    if default is None:
        default = {}
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def get_vendor_mappings():
    return load_json(VENDOR_MAPPINGS_FILE)


def save_vendor_mappings(mappings):
    save_json(VENDOR_MAPPINGS_FILE, mappings)


def get_account_mappings():
    return load_json(ACCOUNT_MAPPINGS_FILE)


def save_account_mappings(mappings):
    save_json(ACCOUNT_MAPPINGS_FILE, mappings)


def get_processed_invoices():
    return load_json(PROCESSED_INVOICES_FILE, default=[])


def save_processed_invoices(invoices):
    save_json(PROCESSED_INVOICES_FILE, invoices)


def get_credit_overrides():
    return load_json(CREDIT_OVERRIDES_FILE)


def save_credit_overrides(overrides):
    save_json(CREDIT_OVERRIDES_FILE, overrides)


def get_master_mappings():
    return load_json(MASTER_SHEET_FILE)


def save_master_mappings(mappings):
    save_json(MASTER_SHEET_FILE, mappings)


# ─── Google Sheets Integration (per-user OAuth) ───
# Each Biz Ops user authorizes once per session; rows are written to the sheet
# using their own Google account (which already has access to the sheet).
GSHEET_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
]


def _get_oauth_flow(code_verifier: str | None = None):
    """Build the google-auth-oauthlib Flow object from secrets.

    If `code_verifier` is provided (callback path), reuse it so PKCE
    completes correctly. Otherwise the flow will autogenerate one when
    `authorization_url()` is called.
    """
    client_id = st.secrets.get("GSHEET_OAUTH_CLIENT_ID", "")
    client_secret = st.secrets.get("GSHEET_OAUTH_CLIENT_SECRET", "")
    redirect_uri = st.secrets.get("GSHEET_OAUTH_REDIRECT_URI", "http://localhost:8501/")

    if not client_id or not client_secret:
        return None

    client_config = {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect_uri],
        }
    }
    flow = Flow.from_client_config(client_config, scopes=GSHEET_OAUTH_SCOPES)
    flow.redirect_uri = redirect_uri
    # PKCE: either reuse the verifier persisted from get_oauth_url(), or let
    # the flow autogenerate a fresh one on authorization_url().
    if code_verifier:
        flow.code_verifier = code_verifier
    else:
        flow.autogenerate_code_verifier = True
    return flow


def handle_oauth_callback():
    """If we returned from Google OAuth with ?code=..., exchange for a token
    and store it in session_state. Called once per page load."""
    if st.session_state.get("gsheet_credentials"):
        return  # already have token

    code = st.query_params.get("code")
    if not code:
        return

    # Restore PKCE verifier saved in get_oauth_url() — needed because each
    # Streamlit rerun creates a fresh Flow instance.
    code_verifier = st.session_state.get("oauth_code_verifier")
    if not code_verifier:
        # Stale ?code= without a matching verifier (e.g. browser back/forward
        # or a callback for the st.login() flow). Clear the param and bail
        # silently — the user can click Authorize again.
        st.query_params.clear()
        return

    flow = _get_oauth_flow(code_verifier=code_verifier)
    if not flow:
        return

    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        st.session_state.gsheet_credentials = {
            "token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": creds.scopes,
        }
        # Clear one-shot OAuth state so we don't reuse it
        st.session_state.pop("oauth_code_verifier", None)
        st.session_state.pop("oauth_state", None)
        # Clear ?code= from URL to avoid re-running on refresh
        st.query_params.clear()
    except Exception as e:
        st.error(f"OAuth callback failed: {e}")


def get_oauth_url():
    """Build the Google OAuth consent URL the user should be sent to.

    Persists the PKCE code_verifier and state in session_state so the
    callback (which runs in a fresh Streamlit rerun with a fresh Flow
    instance) can complete the token exchange.
    """
    flow = _get_oauth_flow()
    if not flow:
        return None
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    st.session_state["oauth_code_verifier"] = flow.code_verifier
    st.session_state["oauth_state"] = state
    return auth_url


def push_rows_to_tracker(tracker_df):
    """Append tracker rows to the BC Tracker Google Sheet using the
    signed-in Biz Ops user's OAuth token.
    Returns (success_count, error_message)."""
    creds_dict = st.session_state.get("gsheet_credentials")
    if not creds_dict:
        return 0, "Not authorized for Sheets access. Click 'Authorize Sheets Access' first."

    spreadsheet_id = st.secrets.get("GSHEET_SPREADSHEET_ID", "")
    if not spreadsheet_id:
        return 0, "GSHEET_SPREADSHEET_ID not configured in secrets.toml."

    worksheet_name = st.secrets.get("GSHEET_WORKSHEET_NAME", "BC Tracker")
    worksheet_gid = st.secrets.get("GSHEET_WORKSHEET_GID", None)

    creds = UserCredentials(**creds_dict)
    try:
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)
    except Exception as e:
        return 0, f"Cannot open sheet: {type(e).__name__}: {str(e)[:200]}"

    # Find worksheet by name, falling back to gid, then first tab
    try:
        ws = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        if worksheet_gid is not None:
            try:
                ws = spreadsheet.get_worksheet_by_id(int(worksheet_gid))
            except Exception:
                ws = spreadsheet.get_worksheet(0)
        else:
            ws = spreadsheet.get_worksheet(0)

    # Read sheet headers to map columns by name
    sheet_headers = ws.row_values(1)
    if not sheet_headers:
        return 0, "BC Tracker sheet has no header row. Add headers matching the tracker columns."

    # Build rows in sheet column order
    rows_to_append = []
    for _, row in tracker_df.iterrows():
        sheet_row = []
        for header in sheet_headers:
            value = ""
            if header in row.index:
                value = "" if pd.isna(row[header]) else str(row[header])
            sheet_row.append(value)
        rows_to_append.append(sheet_row)

    if not rows_to_append:
        return 0, "No rows to push."

    # Convert column count to A1-style column letter (handles AA, AB, …)
    def col_letter(n):
        result = ""
        while n > 0:
            n -= 1
            result = chr(65 + n % 26) + result
            n //= 26
        return result

    end_col = col_letter(len(sheet_headers))
    next_row = len(ws.get_all_values()) + 1

    try:
        ws.update(
            f"A{next_row}:{end_col}{next_row + len(rows_to_append) - 1}",
            rows_to_append,
            value_input_option="USER_ENTERED",
        )
    except gspread.exceptions.APIError as e:
        return 0, f"Sheets API error: {str(e)[:200]}"

    return len(rows_to_append), None


# ─── Duplicate Check ───
def check_duplicate(vendor_name, invoice_no, invoice_date, invoice_value):
    processed = get_processed_invoices()
    for inv in processed:
        if (inv.get("vendor_name", "").strip().lower() == vendor_name.strip().lower()
                and str(inv.get("invoice_no", "")).strip() == str(invoice_no).strip()
                and str(inv.get("invoice_date", "")).strip() == str(invoice_date).strip()
                and str(inv.get("invoice_value", "")).strip() == str(invoice_value).strip()):
            return True
    return False


# ─── Due Date Logic ───
STANDARD_CREDIT_DAYS = {
    "Veeno": 30,
    "Exotel": 45,
    "Drishti": 45,
}


def calculate_due_date(invoice_date_str, entity, vendor_name=None, account_no=None, due_date_on_invoice=None):
    """Calculate due date based on entity rules:
    - Exotel: 45 days from invoice date
    - Veeno: 30 days from invoice date
    - International: use the due date printed on the invoice as-is
    """
    # For International entities, use the due date from the invoice directly
    if entity and "international" in entity.lower():
        if due_date_on_invoice and due_date_on_invoice.lower() not in ("", "pay immediate", "null", "none"):
            # Try to parse and reformat to standard DD-Mon-YY
            for fmt in ("%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%d-%b-%Y"):
                try:
                    parsed = datetime.strptime(due_date_on_invoice, fmt)
                    return parsed.strftime("%d-%b-%y")
                except (ValueError, TypeError):
                    continue
            # If no format matched, return the raw value
            return due_date_on_invoice
        # Fallback: if no due date on invoice, return empty
        return ""

    try:
        inv_date = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except (ValueError, TypeError):
        try:
            inv_date = datetime.strptime(invoice_date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            return ""

    overrides = get_credit_overrides()
    credit_days = None

    if account_no and str(account_no) in overrides.get("account", {}):
        credit_days = overrides["account"][str(account_no)]

    if credit_days is None and vendor_name:
        vendor_key = vendor_name.strip().lower()
        if vendor_key in overrides.get("vendor", {}):
            credit_days = overrides["vendor"][vendor_key]

    if credit_days is None:
        for key, days in STANDARD_CREDIT_DAYS.items():
            if key.lower() in entity.lower():
                credit_days = days
                break

    if credit_days is None:
        credit_days = 45

    due_date = inv_date + timedelta(days=credit_days)
    return due_date.strftime("%d-%b-%y")


# ─── Description Generator ───
def generate_description(product_short, circle, entity, inv_date, start_date, end_date):
    """SIP_Karnataka_Exotel_Mar-26_01-Mar-26_31-Mar-26"""
    def fmt_month(date_str):
        if not date_str:
            return ""
        try:
            dt = datetime.strptime(date_str, "%d-%b-%y")
            return dt.strftime("%b-%y")
        except (ValueError, TypeError):
            return date_str

    parts = [
        product_short or "N/A",
        circle or "N/A",
        entity or "N/A",
        fmt_month(inv_date),
        start_date or "",
        end_date or "",
    ]
    return "_".join(p for p in parts if p)


# ─── Format Helpers ───
def fmt_currency(value):
    try:
        v = float(value)
        if v == 0:
            return ""
        return f"{v:,.2f}"
    except (ValueError, TypeError):
        return ""


def derive_product_short(full_product):
    """Fallback: derive short product name from full name."""
    if not full_product:
        return ""
    fp = full_product.lower()
    if "sip" in fp:
        return "SIP"
    if "pri" in fp or "isdn" in fp:
        return "PRI"
    if "ill" in fp or "leased" in fp:
        return "ILL"
    if "mpls" in fp or "vpn" in fp:
        return "MPLS"
    if "toll" in fp:
        return "TF"
    if "did" in fp:
        return "DID"
    if "cloud" in fp:
        return "Cloud"
    if "broadband" in fp:
        return "Broadband"
    return full_product.split()[0] if full_product else ""


# ─── Master Sheet Parser ───
def parse_master_sheet(df):
    """Parse master sheet DataFrame: Column K (index 10) = Account No, Column H (index 7) = Product.
    Returns dict mapping account_no -> {product, product_short}."""
    mappings = {}
    if len(df.columns) < 11:
        return mappings

    # Column H (index 7) = Product, Column K (index 10) = Account No
    account_col = df.columns[10]  # Column K
    product_col = df.columns[7]   # Column H

    for _, row in df.iterrows():
        acct = str(row[account_col]).strip()
        product = str(row[product_col]).strip()
        if not acct or acct in ("nan", "", "None"):
            continue
        if not product or product in ("nan", "", "None"):
            continue

        product_short = derive_product_short(product)
        mappings[acct] = {
            "product": product,
            "product_short": product_short,
        }

    return mappings


# ─── Large PDF Trimming ───
def trim_pdf_for_extraction(pdf_bytes):
    """For large PDFs (500+ pages), extract only the first and last few pages.
    Invoice summary data (account, amounts, dates) is always on these pages.
    The middle pages are typically CDR (Call Detail Records) which aren't needed.

    Returns: (trimmed_pdf_bytes, total_page_count, was_trimmed)
    """
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        total_pages = len(reader.pages)

        if total_pages <= MAX_PAGES_FOR_CLAUDE:
            return pdf_bytes, total_pages, False

        # Extract first N and last N pages
        writer = PdfWriter()
        pages_to_include = set()

        # First pages (invoice header, account details, billing summary)
        for i in range(min(FRONT_PAGES, total_pages)):
            pages_to_include.add(i)

        # Last pages (totals, payment summary, due date)
        for i in range(max(0, total_pages - BACK_PAGES), total_pages):
            pages_to_include.add(i)

        for page_idx in sorted(pages_to_include):
            writer.add_page(reader.pages[page_idx])

        output = io.BytesIO()
        writer.write(output)
        output.seek(0)
        trimmed_bytes = output.read()

        return trimmed_bytes, total_pages, True

    except Exception:
        # If PDF parsing fails, return original and let Claude try
        return pdf_bytes, 0, False


# ─── Claude Extraction ───
def extract_invoice_data(pdf_bytes, filename):
    client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])

    # Trim large PDFs to avoid token limits
    trimmed_bytes, total_pages, was_trimmed = trim_pdf_for_extraction(pdf_bytes)

    if was_trimmed:
        st.info(
            f"📋 `{filename}`: {total_pages} pages detected — sending first {FRONT_PAGES} + last {BACK_PAGES} pages "
            f"(skipping {total_pages - FRONT_PAGES - BACK_PAGES} CDR pages)"
        )

    pdf_b64 = base64.standard_b64encode(trimmed_bytes).decode("utf-8")

    vendor_mappings = get_vendor_mappings()

    # Keep mapping context minimal to save tokens — only vendor mappings
    # Account→Product mapping is applied post-extraction from master sheet
    mapping_context = ""
    if vendor_mappings:
        mapping_context += "\n\nKnown vendor-to-entity mappings:\n"
        for vendor, info in list(vendor_mappings.items())[:20]:
            mapping_context += f"- {vendor} → Entity: {info.get('entity', 'unknown')}\n"

    trimmed_note = ""
    if was_trimmed:
        trimmed_note = (
            f"\n\nNOTE: This PDF originally has {total_pages} pages. Only the first {FRONT_PAGES} and last {BACK_PAGES} "
            f"pages are provided (the middle pages contain CDR/call detail records). "
            f"Extract all invoice summary data from these pages."
        )

    extraction_prompt = f"""You are an expert invoice data extraction system for Exotel's Finance/AP team.
Extract ALL the following fields from this Tata Teleservices invoice PDF. Be precise with numbers and dates.
{mapping_context}{trimmed_note}

Return a JSON object with exactly these fields:
{{
  "entity": "<EXOTEL TECHCOM PRIVATE LIMITED → Exotel, VEENO COMMUNICATIONS PRIVATE LIMITED → Veeno, DRISHTI-SOFT SOLUTIONS PRIVATE LIMITED → Drishti, EXOTEL TECHCOM PTE LTD → Exotel International. This is the CUSTOMER name on the invoice.>",
  "vendor_name": "<The service provider / biller name in UPPERCASE, e.g. TATA TELESERVICES (MAHARASHTRA) LTD or TATA TELESERVICES LIMITED or BHARTI AIRTEL LIMITED or DU (EMIRATES INTEGRATED TELECOMMUNICATIONS)>",
  "customer_gstin": "<Customer's GST number>",
  "account_no": "<Account No from Bill Details>",
  "invoice_no": "<Bill/Invoice No>",
  "invoice_date": "<Bill Date in DD-Mon-YY format, e.g. 03-Mar-26>",
  "header_bill_period": "<The billing period shown in the header/top section of the invoice, in DD-Mon-YY to DD-Mon-YY format>",
  "bill_period_start": "<Actual Start of Bill Period from the charges summary/detail section in DD-Mon-YY>",
  "bill_period_end": "<Actual End of Bill Period from the charges summary/detail section in DD-Mon-YY>",
  "period_mismatch": <true/false — set to true if the header billing period differs from the actual charges billing period>,
  "period_mismatch_detail": "<Describe the mismatch if any, e.g. 'Header says 01-Jan-26 to 31-Jan-26 but charges are for 15-Dec-25 to 14-Jan-26'. null if no mismatch>",
  "due_date_on_invoice": "<Due Date as printed on the invoice, or 'Pay Immediate' if so stated>",
  "currency": "<INR, AED, USD, etc.>",
  "rental_charges": <number>,
  "usage_charges": <number>,
  "subtotal_without_tax": <number>,
  "one_time_charges": <number>,
  "gst_amount": <number, 0 for international invoices with no GST/VAT>,
  "total_current_charges": <number, Total Current Charges including tax>,
  "previous_balance": <number, Previous Outstanding/Balance amount from the invoice. IMPORTANT: Look carefully — if the invoice shows 'Previous Balance' or 'Previous Outstanding' with a value, report it. If there is also a 'Payment Received/Adjusted' line, report that separately in last_payment. If no previous balance line exists, use 0.>,
  "last_payment": <number, Payment received/adjusted amount, if shown on the invoice>,
  "amount_due": <number, the final Amount Due / Amount Payable shown on the invoice>,
  "invoice_value": <number, Total Current Charges (this period's charges including tax, EXCLUDING previous balance)>,
  "invoice_value_without_tax": <number, net value before GST/tax>,
  "product": "<Full service/product name, e.g. SIP Trunk Channel Line Int>",
  "product_short": "<SHORT abbreviation: SIP, PRI, ILL, MPLS, TF, DID, Cloud, Broadband, VPNOL>",
  "circuit_ids": "<comma-separated Tata Tele Numbers / Circuit IDs>",
  "circle": "<State/Region from Installation/Place of Supply: Maharashtra, Rajasthan, Gujarat, Karnataka, Dubai, etc.>",
  "invoice_category": "<Recurring or One-time>",
  "is_international": <true/false — true if the invoice is from an international vendor or has no GST>,
  "notes": "<any additional relevant information, warnings, or anomalies detected>"
}}

IMPORTANT:
- For monetary values, return plain numbers (no commas, no Rs. prefix). Use 0 if not applicable.
- If a field is not found, use null.
- The entity is the CUSTOMER (who is being billed), NOT the vendor.
- For 'product_short', use: SIP for SIP Trunk, PRI for PRI/ISDN, ILL for Internet Leased Line, etc.
- For 'circle', derive from the state in Installation/Place of Supply.
- PERIOD CHECK: Carefully compare the billing period in the invoice header/top section with the actual period in the charges/summary section. Flag any mismatch.
- PREVIOUS BALANCE: Report the exact 'Previous Balance/Outstanding' figure from the invoice. If the invoice shows payment adjustments, report them in last_payment separately. If there is NO previous balance line on the invoice, use 0 — do NOT invent one.
- INTERNATIONAL INVOICES: For invoices with no GST (international vendors like DU, Etisalat, etc.), gst_amount should be 0 and invoice_value_without_tax should equal total_current_charges.
- Return ONLY the JSON object, no other text."""

    response = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": pdf_b64,
                        },
                        "cache_control": {"type": "ephemeral"},
                    },
                    {"type": "text", "text": extraction_prompt},
                ],
            }
        ],
    )

    response_text = response.content[0].text.strip()
    if response_text.startswith("```"):
        response_text = re.sub(r"^```(?:json)?\s*", "", response_text)
        response_text = re.sub(r"\s*```$", "", response_text)

    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        st.error(f"Failed to parse extraction for {filename}")
        return None

    data["_source_file"] = filename
    data["_total_pages"] = total_pages
    data["_was_trimmed"] = was_trimmed
    return data


def invoice_to_tracker_row(sno, data):
    """Convert extracted invoice data into a Trello Tracker row dict."""
    entity = data.get("entity", "")
    product_short = data.get("product_short") or derive_product_short(data.get("product", ""))
    circle = data.get("circle", "")
    inv_date = data.get("invoice_date", "")
    start_date = data.get("bill_period_start", "")
    end_date = data.get("bill_period_end", "")
    vendor_name = data.get("vendor_name", "")
    account_no = str(data.get("account_no", ""))
    invoice_no = str(data.get("invoice_no", ""))
    due_date_on_invoice = data.get("due_date_on_invoice", "")

    # Master sheet mapping: override product based on account number
    master_mappings = get_master_mappings()
    if account_no and account_no in master_mappings:
        master_entry = master_mappings[account_no]
        # Product from master sheet takes priority
        product_short = master_entry.get("product_short", product_short)

    # Due date: Exotel=45d, Veeno=30d, International=invoice due date
    due_date = calculate_due_date(inv_date, entity, vendor_name, account_no, due_date_on_invoice)

    # Description
    desc = generate_description(product_short, circle, entity, inv_date, start_date, end_date)

    # Values
    is_international = data.get("is_international", False)
    inv_value = data.get("invoice_value") or data.get("total_current_charges") or 0
    prev_bal = data.get("previous_balance") or 0

    # GST/BC: For international invoices, GST = Invoice Value (no tax applied)
    if is_international:
        gst_bc = inv_value
    else:
        gst_bc = data.get("invoice_value_without_tax") or data.get("subtotal_without_tax") or 0

    # Received date = today
    received_dt = datetime.now().strftime("%d-%b-%y")

    # Duplicate check
    is_dup = check_duplicate(vendor_name, invoice_no, inv_date, str(inv_value))
    repeat = "DUP" if is_dup else "1"

    # Cogs
    account_mappings = get_account_mappings()
    cogs = "Cogs"
    if account_no in account_mappings:
        cogs = account_mappings[account_no].get("cogs", "Cogs")

    # Period mismatch warning
    remarks_parts = []
    if data.get("period_mismatch"):
        mismatch_detail = data.get("period_mismatch_detail", "Header vs actual billing period mismatch")
        remarks_parts.append(f"PERIOD MISMATCH: {mismatch_detail}")

    # Previous balance anomaly: flag if previous_balance is reported but seems wrong
    if prev_bal and float(prev_bal) != 0:
        last_payment = data.get("last_payment") or 0
        if float(last_payment) != 0:
            remarks_parts.append(f"Prev bal: {fmt_currency(prev_bal)}, Payment adjusted: {fmt_currency(last_payment)}")

    extra_notes = data.get("notes", "") or ""
    if extra_notes:
        remarks_parts.append(extra_notes)

    row = {}
    for col in TRACKER_COLUMNS:
        row[col] = ""

    row["SNO"] = sno
    row["Date"] = received_dt
    row["Cmp"] = entity
    row["Cogs/Noncogs"] = cogs
    row["Vendor no."] = vendor_name
    row["Product"] = product_short
    row["Circle"] = circle
    row["Description"] = desc
    row["Account No"] = account_no
    row["InvoiceNo"] = invoice_no
    row["Repeat Number"] = repeat
    row["Received dt"] = received_dt
    row["Due Date"] = due_date
    row["Inv dt"] = inv_date
    row["Start Date"] = start_date
    row["End dt"] = end_date
    row["Invoice Value"] = fmt_currency(inv_value)
    row["GST/BC"] = fmt_currency(gst_bc)
    row["Previous Balance"] = fmt_currency(prev_bal) if prev_bal else ""
    row["Remarks"] = " | ".join(remarks_parts) if remarks_parts else ""
    row["Exotel"] = fmt_currency(inv_value) if "exotel" in entity.lower() else ""
    row["Veeno"] = fmt_currency(inv_value) if "veeno" in entity.lower() else ""

    return row


# ─── Session State ───
if "tracker_df" not in st.session_state:
    st.session_state.tracker_df = None
if "raw_extractions" not in st.session_state:
    st.session_state.raw_extractions = []

# Handle Google OAuth callback (?code=... in URL after consent screen)
handle_oauth_callback()

# ─── STEP 1: Upload ───
st.markdown("### Upload Invoices")
st.caption("Upload invoice PDFs or a ZIP file. Data will be extracted and shown as an editable tracker sheet.")

uploaded_files = st.file_uploader(
    "Drop invoice PDFs or a ZIP file here",
    type=["pdf", "zip"],
    accept_multiple_files=True,
    key="invoice_upload",
)

if uploaded_files:
    # Expand ZIPs
    pdf_files = []
    zip_count = 0
    for file in uploaded_files:
        if file.name.lower().endswith(".zip"):
            zip_count += 1
            file.seek(0)
            raw = file.read()
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    count_in_zip = 0
                    for name in zf.namelist():
                        if "__MACOSX" in name or name.endswith("/"):
                            continue
                        if name.lower().endswith(".pdf"):
                            pdf_bytes = zf.read(name)
                            short_name = os.path.basename(name)
                            pdf_files.append((short_name, pdf_bytes))
                            count_in_zip += 1
                    if count_in_zip == 0:
                        st.warning(f"ZIP `{file.name}` contains no PDF files.")
            except zipfile.BadZipFile:
                st.error(f"`{file.name}` is not a valid ZIP file.")
        else:
            file.seek(0)
            pdf_files.append((file.name, file.read()))

    if zip_count:
        st.info(f"{len(uploaded_files)} file(s) uploaded ({zip_count} ZIP) → **{len(pdf_files)} PDF(s)** to process")
    else:
        st.info(f"{len(pdf_files)} PDF file(s) selected")

    if not pdf_files:
        st.warning("No PDF files to process.")

    if pdf_files and st.button("Extract All Invoices", type="primary", use_container_width=True):
        tracker_rows = []
        raw_extractions = []
        progress = st.progress(0, text="Starting extraction...")

        for i, (filename, pdf_bytes) in enumerate(pdf_files):
            progress.progress(
                i / len(pdf_files),
                text=f"Extracting {filename} ({i + 1}/{len(pdf_files)})...",
            )
            try:
                data = extract_invoice_data(pdf_bytes, filename)
                if data:
                    # Apply learned mappings
                    vendor_mappings = get_vendor_mappings()
                    account_mappings = get_account_mappings()
                    master_mappings = get_master_mappings()
                    vendor_key = (data.get("vendor_name") or "").strip().lower()
                    acct_key = str(data.get("account_no") or "")

                    if vendor_key in vendor_mappings and not data.get("entity"):
                        data["entity"] = vendor_mappings[vendor_key].get("entity", "")

                    # Master sheet mapping takes priority for product
                    if acct_key in master_mappings:
                        master_entry = master_mappings[acct_key]
                        data["product_short"] = master_entry.get("product_short", data.get("product_short", ""))
                    elif acct_key in account_mappings:
                        if not data.get("product_short"):
                            data["product_short"] = account_mappings[acct_key].get("product_short", "")

                    if not data.get("product_short"):
                        data["product_short"] = derive_product_short(data.get("product", ""))

                    raw_extractions.append(data)
                    row = invoice_to_tracker_row(len(tracker_rows) + 1, data)
                    tracker_rows.append(row)
            except Exception as e:
                st.error(f"Error extracting {filename}: {e}")

        progress.progress(1.0, text="Extraction complete!")

        if tracker_rows:
            st.session_state.tracker_df = pd.DataFrame(tracker_rows, columns=TRACKER_COLUMNS)
            st.session_state.raw_extractions = raw_extractions
            st.success(f"Extracted **{len(tracker_rows)}** invoice(s) from {len(pdf_files)} PDF(s).")
        else:
            st.error("No invoices could be extracted.")

# ─── STEP 2: Editable Tracker Table ───
if st.session_state.tracker_df is not None:
    st.divider()
    st.markdown("### Invoice Tracker")
    st.caption("Edit any cell directly. Then export to CSV or save to history.")

    # Check for duplicates
    dup_count = (st.session_state.tracker_df["Repeat Number"] == "DUP").sum()
    if dup_count > 0:
        st.warning(f"{dup_count} duplicate invoice(s) detected (marked as DUP in Repeat Number column).")

    # Check for period mismatches
    mismatch_rows = st.session_state.tracker_df[
        st.session_state.tracker_df["Remarks"].str.contains("PERIOD MISMATCH", case=False, na=False)
    ]
    if len(mismatch_rows) > 0:
        st.warning(f"{len(mismatch_rows)} invoice(s) have billing period mismatches (header vs actual). Check Remarks column.")

    # Editable data editor
    edited_df = st.data_editor(
        st.session_state.tracker_df[DISPLAY_COLUMNS],
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="tracker_editor",
    )

    # Sync edits back
    for col in DISPLAY_COLUMNS:
        if col in edited_df.columns:
            st.session_state.tracker_df[col] = edited_df[col]

    # Export buttons
    st.divider()
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        # CSV Export — full tracker format
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=TRACKER_COLUMNS)
        writer.writeheader()
        for _, row in st.session_state.tracker_df.iterrows():
            writer.writerow(row.to_dict())
        csv_data = output.getvalue()

        st.download_button(
            "Download Tracker CSV",
            csv_data,
            file_name=f"Trello_Tracker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
        )

    with col2:
        # Excel export
        excel_buffer = io.BytesIO()
        st.session_state.tracker_df.to_excel(excel_buffer, index=False, sheet_name="BC Tracker")
        excel_buffer.seek(0)

        st.download_button(
            "Download Tracker Excel",
            excel_buffer.getvalue(),
            file_name=f"Trello_Tracker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    with col3:
        is_authorized = bool(st.session_state.get("gsheet_credentials"))
        if not is_authorized:
            auth_url = get_oauth_url()
            if auth_url:
                st.link_button(
                    "Authorize Sheets Access",
                    auth_url,
                    use_container_width=True,
                    help="One-time per session: grants this app permission to write to Google Sheets as you.",
                )
            else:
                st.button(
                    "Push to BC Tracker",
                    disabled=True,
                    use_container_width=True,
                    help="OAuth client not configured in secrets.toml.",
                )
        else:
            if st.button("Push to BC Tracker", type="primary", use_container_width=True):
                with st.spinner("Pushing to Google Sheet..."):
                    count, error = push_rows_to_tracker(st.session_state.tracker_df)
                    if error:
                        st.error(f"Failed: {error}")
                    else:
                        st.success(f"Pushed **{count}** row(s) to BC Tracker sheet!")

    with col4:
        if st.button("Save to History", use_container_width=True):
            # Save raw extractions and learn mappings
            processed = get_processed_invoices()
            for data in st.session_state.raw_extractions:
                vendor_name = data.get("vendor_name", "")
                entity = data.get("entity", "")
                account_no = str(data.get("account_no", ""))
                product_short = data.get("product_short", "")

                # Learn vendor mapping
                v_key = vendor_name.strip().lower()
                if v_key:
                    vm = get_vendor_mappings()
                    if v_key not in vm:
                        vm[v_key] = {"entity": entity, "vendor_name": vendor_name, "learned_at": datetime.now().isoformat()}
                        save_vendor_mappings(vm)

                # Learn account mapping
                if account_no:
                    am = get_account_mappings()
                    am[account_no] = {
                        "product": data.get("product", ""),
                        "product_short": product_short,
                        "cogs": "Cogs",
                        "entity": entity,
                        "circle": data.get("circle", ""),
                        "learned_at": datetime.now().isoformat(),
                    }
                    save_account_mappings(am)

                processed.append(data)

            save_processed_invoices(processed)
            st.success(f"Saved {len(st.session_state.raw_extractions)} invoice(s) to history. Vendor & account mappings updated.")

# ─── Sidebar: Settings ───
with st.sidebar:
    st.markdown("### Settings")

    with st.expander("Master Sheet (Account → Product Mapping)"):
        master = get_master_mappings()
        st.metric("Mapped Accounts", len(master))

        st.caption("Upload the master sheet (CSV or Excel) to map Account No (Col K) → Product (Col H)")
        master_file = st.file_uploader(
            "Upload master sheet",
            type=["csv", "xlsx", "xls"],
            key="master_upload",
        )
        if master_file:
            try:
                if master_file.name.lower().endswith(".csv"):
                    master_df = pd.read_csv(master_file)
                else:
                    master_df = pd.read_excel(master_file)

                new_mappings = parse_master_sheet(master_df)
                if new_mappings:
                    save_master_mappings(new_mappings)
                    st.success(f"Loaded **{len(new_mappings)}** account-to-product mappings from master sheet.")
                    st.rerun()
                else:
                    st.warning("No valid Account No → Product mappings found. Ensure Column K has Account No and Column H has Product.")
            except Exception as e:
                st.error(f"Error parsing master sheet: {e}")

        if master:
            st.markdown("**Account → Product mappings:**")
            for acct, info in list(master.items())[:20]:
                st.text(f"  {acct} → {info.get('product_short', info.get('product', '?'))}")
            if len(master) > 20:
                st.caption(f"... and {len(master) - 20} more")

        if master and st.button("Clear Master Mappings"):
            save_master_mappings({})
            st.success("Master mappings cleared.")
            st.rerun()

    with st.expander("Credit Period Overrides"):
        st.caption("Standard: Veeno=30d, Exotel/Drishti=45d, International=invoice due date")
        overrides = get_credit_overrides()
        if "vendor" not in overrides:
            overrides["vendor"] = {}
        if "account" not in overrides:
            overrides["account"] = {}

        ov_vendor = st.text_input("Vendor name", key="ov_vendor")
        ov_days = st.number_input("Credit days", min_value=1, max_value=365, value=30, key="ov_days")
        if st.button("Add Override"):
            if ov_vendor:
                overrides["vendor"][ov_vendor.lower().strip()] = ov_days
                save_credit_overrides(overrides)
                st.success(f"{ov_vendor} → {ov_days} days")
                st.rerun()

        if overrides.get("vendor"):
            st.markdown("**Active overrides:**")
            for v, d in overrides["vendor"].items():
                st.text(f"  {v}: {d} days")

    with st.expander("Learned Mappings"):
        vm = get_vendor_mappings()
        am = get_account_mappings()
        st.metric("Known Vendors", len(vm))
        st.metric("Known Accounts", len(am))
        if vm:
            st.markdown("**Vendors:**")
            for k, v in vm.items():
                st.text(f"  {v.get('vendor_name', k)} → {v.get('entity', '?')}")
        if am:
            st.markdown("**Accounts:**")
            for k, v in am.items():
                st.text(f"  {k} → {v.get('product_short', '?')} ({v.get('entity', '?')})")
        if st.button("Clear All Mappings"):
            save_vendor_mappings({})
            save_account_mappings({})
            st.success("Cleared.")
            st.rerun()

    with st.expander("Processing History"):
        processed = get_processed_invoices()
        st.metric("Total Processed", len(processed))
        if st.button("Clear History"):
            save_processed_invoices([])
            st.success("History cleared.")
            st.rerun()
