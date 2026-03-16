import streamlit as st
import anthropic
import base64
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import io
import csv
import zipfile
import tempfile

# ─── Page Config ───
st.set_page_config(
    page_title="Invoice OCR - Exotel",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Paths ───
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
VENDOR_MAPPINGS_FILE = DATA_DIR / "vendor_mappings.json"
ACCOUNT_MAPPINGS_FILE = DATA_DIR / "account_mappings.json"
PROCESSED_INVOICES_FILE = DATA_DIR / "processed_invoices.json"
CREDIT_OVERRIDES_FILE = DATA_DIR / "credit_overrides.json"
EDIT_LOG_FILE = DATA_DIR / "edit_log.json"

# ─── CSS ───
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .stApp { font-family: 'Inter', sans-serif; }

    /* Top bar */
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

    /* Cards */
    .field-card {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 0.75rem;
        margin-bottom: 0.5rem;
    }
    .field-label {
        font-size: 0.75rem;
        color: #6c757d;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .field-value {
        font-size: 1rem;
        font-weight: 500;
        color: #212529;
    }

    /* Alerts */
    .alert-duplicate {
        background: #ffebee;
        border-left: 4px solid #c62828;
        padding: 0.75rem 1rem;
        border-radius: 4px;
        margin: 0.5rem 0;
    }
    .alert-warning {
        background: #fff3e0;
        border-left: 4px solid #e65100;
        padding: 0.75rem 1rem;
        border-radius: 4px;
        margin: 0.5rem 0;
    }
    .alert-success {
        background: #e8f5e9;
        border-left: 4px solid #2e7d32;
        padding: 0.75rem 1rem;
        border-radius: 4px;
        margin: 0.5rem 0;
    }

    /* Hide Streamlit chrome */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ─── Top Bar ───
st.markdown("""
<div class="top-bar">
    <span class="top-bar-title">📄 Invoice OCR & Auto-Posting System</span>
    <span class="status-badge">● Online — Sonnet 4.6</span>
</div>
""", unsafe_allow_html=True)


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


def get_edit_log():
    return load_json(EDIT_LOG_FILE, default=[])


def save_edit_log(log):
    save_json(EDIT_LOG_FILE, log)


def log_edit(invoice_no, field, old_value, new_value, user="system"):
    log = get_edit_log()
    log.append({
        "timestamp": datetime.now().isoformat(),
        "invoice_no": invoice_no,
        "field": field,
        "old_value": str(old_value),
        "new_value": str(new_value),
        "user": user,
    })
    save_edit_log(log)


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
    "International": 20,
}


def calculate_due_date(invoice_date_str, entity, vendor_name=None, account_no=None):
    """Calculate due date based on entity standard + any vendor/account overrides."""
    try:
        inv_date = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except (ValueError, TypeError):
        try:
            inv_date = datetime.strptime(invoice_date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            return "Unable to calculate"

    # Check overrides
    overrides = get_credit_overrides()
    credit_days = None

    # Account-level override takes priority
    if account_no and str(account_no) in overrides.get("account", {}):
        credit_days = overrides["account"][str(account_no)]

    # Then vendor-level
    if credit_days is None and vendor_name:
        vendor_key = vendor_name.strip().lower()
        if vendor_key in overrides.get("vendor", {}):
            credit_days = overrides["vendor"][vendor_key]

    # Fall back to entity standard
    if credit_days is None:
        for key, days in STANDARD_CREDIT_DAYS.items():
            if key.lower() in entity.lower():
                credit_days = days
                break

    if credit_days is None:
        credit_days = 45  # default

    due_date = inv_date + timedelta(days=credit_days)
    return due_date.strftime("%d-%b-%y")


# ─── Description Generator ───
def generate_description(product_short, circle, entity, inv_date, start_date, end_date):
    """Generate description in the tracker format: Product_Circle_Cmp_Mon-YY_DD-Mon-YY_DD-Mon-YY"""
    def fmt_month(date_str):
        """Convert date like '03-Mar-26' to 'Mar-26'"""
        if not date_str:
            return ""
        try:
            dt = datetime.strptime(date_str, "%d-%b-%y")
            return dt.strftime("%b-%y")
        except (ValueError, TypeError):
            return date_str

    def fmt_date(date_str):
        """Convert date like '03-Mar-26' to '01-Mar-26' format"""
        if not date_str:
            return ""
        return date_str

    parts = [
        product_short or "N/A",
        circle or "N/A",
        entity or "N/A",
        fmt_month(inv_date),
        fmt_date(start_date),
        fmt_date(end_date),
    ]
    return "_".join(p for p in parts if p)


# ─── Format Helpers for Export ───
def fmt_currency(value):
    """Format a number with commas and 2 decimal places, e.g. 5,310.00"""
    try:
        v = float(value)
        return f"{v:,.2f}"
    except (ValueError, TypeError):
        return ""


# ─── Claude Extraction ───
def extract_invoice_data(pdf_bytes, filename):
    """Send PDF to Claude and extract structured invoice fields."""
    client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])

    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    # Load existing mappings to provide context
    vendor_mappings = get_vendor_mappings()
    account_mappings = get_account_mappings()

    mapping_context = ""
    if vendor_mappings:
        mapping_context += "\n\nKnown vendor-to-entity mappings:\n"
        for vendor, info in vendor_mappings.items():
            mapping_context += f"- {vendor} → Entity: {info.get('entity', 'unknown')}\n"
    if account_mappings:
        mapping_context += "\nKnown account number mappings:\n"
        for acct, info in account_mappings.items():
            mapping_context += f"- Account {acct} → Product: {info.get('product', 'unknown')}, COGS: {info.get('cogs', 'unknown')}\n"

    extraction_prompt = f"""You are an expert invoice data extraction system for Exotel's Finance/AP team.
Extract ALL the following fields from this Tata Teleservices invoice PDF. Be precise with numbers and dates.

{mapping_context}

Return a JSON object with exactly these fields:
{{
  "entity": "<EXOTEL TECHCOM PRIVATE LIMITED or VEENO COMMUNICATIONS PRIVATE LIMITED or DRISHTI-SOFT SOLUTIONS PRIVATE LIMITED — this is the CUSTOMER name on the invoice, map it to: Exotel, Veeno, or Drishti>",
  "vendor_name": "<The service provider / biller name, e.g. Tata Teleservices (Maharashtra) Ltd or Tata Teleservices Ltd>",
  "vendor_gstin": "<Vendor's GST number from the invoice>",
  "customer_gstin": "<Customer's GST number>",
  "account_no": "<Account No from Bill Details>",
  "invoice_no": "<Bill/Invoice No>",
  "invoice_date": "<Bill Date in DD-Mon-YY format, e.g. 03-Mar-26>",
  "bill_period_start": "<Start of Bill Period in DD-Mon-YY>",
  "bill_period_end": "<End of Bill Period in DD-Mon-YY>",
  "due_date_on_invoice": "<Due Date as printed on the invoice, or 'Pay Immediate' if so stated>",
  "currency": "INR",
  "rental_charges": <number, rental charges amount without tax>,
  "usage_charges": <number, usage charges amount without tax>,
  "subtotal_without_tax": <number, SubTotal before one-time charges and tax>,
  "one_time_charges": <number>,
  "gst_amount": <number, total GST amount>,
  "cgst": <number>,
  "sgst": <number>,
  "igst": <number or 0>,
  "total_current_charges": <number, Total Current Charges including tax>,
  "previous_balance": <number, Previous Balance amount, can be negative>,
  "last_payment": <number>,
  "invoice_value": <number, the final amount due / total bill amount>,
  "invoice_value_without_tax": <number, subtotal + one-time charges, i.e. net value before GST>,
  "product": "<Full service/product name from bill details page, e.g. SIP Trunk Channel Line Int>",
  "product_short": "<Short product abbreviation derived from the service/product description. Use: SIP for any SIP Trunk related product, PRI for PRI/ISDN lines, ILL for Internet Leased Line, MPLS for MPLS/VPN, TF for Toll Free, DID for DID numbers, Cloud for Cloud Telephony, Broadband for broadband services, VPNOL for VPN over Internet. If unsure, use the first word or most recognizable abbreviation.>",
  "circuit_ids": "<comma-separated Tata Tele Numbers / Circuit IDs>",
  "hsn_code": "<HSN code>",
  "circle": "<State/Circle derived from Installation/Place of Supply address — use state name like Maharashtra, Rajasthan, Gujarat, etc.>",
  "invoice_category": "<Recurring or One-time — based on whether charges are rental/AMC vs setup/installation>",
  "plan_details": "<Bill plan name, e.g. SIP150Rent 4.5ps15 Sec NP plan>",
  "installation_address": "<Full Installation/Place of Supply address>",
  "gst_rate": "<GST rate percentage, e.g. 18%>",
  "notes": "<any additional relevant information>"
}}

IMPORTANT:
- For monetary values, return plain numbers (no commas, no Rs. prefix). Use 0 if not applicable.
- If a field is not found in the invoice, use null.
- The entity is the CUSTOMER (who is being billed), not the vendor.
- For 'circle', derive it from the state in the Installation/Place of Supply section.
- For invoice_value, use the "Amount due before due date" or "Total Current Charges" if no previous balance, or the total amount due.
- For 'product_short', extract a SHORT abbreviation (SIP, PRI, ILL, MPLS, TF, DID, Cloud, etc.) from the product/service description. This is critical for the tracker description format.
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
                    {
                        "type": "text",
                        "text": extraction_prompt,
                    },
                ],
            }
        ],
    )

    # Parse the JSON from Claude's response
    response_text = response.content[0].text.strip()
    # Strip markdown code fences if present
    if response_text.startswith("```"):
        response_text = re.sub(r"^```(?:json)?\s*", "", response_text)
        response_text = re.sub(r"\s*```$", "", response_text)

    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        st.error(f"Failed to parse Claude's response as JSON:\n{response_text[:500]}")
        return None

    data["_source_file"] = filename
    data["_extracted_at"] = datetime.now().isoformat()
    return data


# ─── Session State Init ───
if "extracted_invoices" not in st.session_state:
    st.session_state.extracted_invoices = []
if "current_invoice_idx" not in st.session_state:
    st.session_state.current_invoice_idx = 0
if "processing" not in st.session_state:
    st.session_state.processing = False

# ─── Sidebar ───
with st.sidebar:
    st.markdown("### Navigation")
    page = st.radio(
        "Go to",
        ["Upload & Extract", "Review & Edit", "Processed Invoices", "Reports", "Settings"],
        label_visibility="collapsed",
    )
    st.divider()
    processed = get_processed_invoices()
    st.metric("Processed Invoices", len(processed))
    vendor_maps = get_vendor_mappings()
    st.metric("Known Vendors", len(vendor_maps))
    acct_maps = get_account_mappings()
    st.metric("Known Accounts", len(acct_maps))

# ─── PAGE: Upload & Extract ───
if page == "Upload & Extract":
    st.markdown("### Upload Invoices")
    st.caption("Upload invoice PDFs individually, or upload a ZIP file containing multiple PDFs. Claude will extract all fields automatically.")

    uploaded_files = st.file_uploader(
        "Drop invoice PDFs or a ZIP file here",
        type=["pdf", "zip"],
        accept_multiple_files=True,
        key="invoice_upload",
    )

    if uploaded_files:
        # Expand ZIP files into individual PDFs
        pdf_files = []  # list of (filename, bytes)
        zip_count = 0
        for file in uploaded_files:
            if file.name.lower().endswith(".zip"):
                zip_count += 1
                file.seek(0)  # ensure we read from start
                raw = file.read()
                try:
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        pdf_count_in_zip = 0
                        for name in zf.namelist():
                            # Skip macOS resource fork files and directories
                            if "__MACOSX" in name or name.endswith("/"):
                                continue
                            if name.lower().endswith(".pdf"):
                                pdf_bytes = zf.read(name)
                                # Use just the filename, not the full path inside zip
                                short_name = os.path.basename(name)
                                pdf_files.append((short_name, pdf_bytes))
                                pdf_count_in_zip += 1
                        if pdf_count_in_zip == 0:
                            st.warning(f"ZIP file `{file.name}` contains no PDF files.")
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
            st.warning("No PDF files to process. Please upload PDFs directly or a ZIP containing PDFs.")

        if st.button("Extract All Invoices", type="primary", use_container_width=True):
            st.session_state.extracted_invoices = []
            progress = st.progress(0, text="Starting extraction...")

            for i, (filename, pdf_bytes) in enumerate(pdf_files):
                progress.progress(
                    (i) / len(pdf_files),
                    text=f"Extracting {filename} ({i + 1}/{len(pdf_files)})...",
                )
                try:
                    data = extract_invoice_data(pdf_bytes, filename)
                    if data:
                        # Apply learned mappings
                        vendor_mappings = get_vendor_mappings()
                        account_mappings = get_account_mappings()
                        vendor_key = (data.get("vendor_name") or "").strip().lower()
                        acct_key = str(data.get("account_no") or "")

                        # Auto-fill entity from vendor mapping if not extracted
                        if vendor_key in vendor_mappings and not data.get("entity"):
                            data["entity"] = vendor_mappings[vendor_key].get("entity", "")

                        # Auto-fill product/COGS from account mapping
                        if acct_key in account_mappings:
                            if not data.get("product"):
                                data["product"] = account_mappings[acct_key].get("product", "")
                            data["cogs_classification"] = account_mappings[acct_key].get("cogs", "COGS")
                        else:
                            data["cogs_classification"] = "COGS"  # default

                        # Ensure product_short has a value
                        if not data.get("product_short"):
                            # Fallback: derive from full product name
                            full = (data.get("product") or "").upper()
                            if "SIP" in full:
                                data["product_short"] = "SIP"
                            elif "PRI" in full or "ISDN" in full:
                                data["product_short"] = "PRI"
                            elif "ILL" in full or "INTERNET LEASED" in full:
                                data["product_short"] = "ILL"
                            elif "MPLS" in full or "VPN" in full:
                                data["product_short"] = "MPLS"
                            elif "TOLL FREE" in full or "TF" in full:
                                data["product_short"] = "TF"
                            elif "DID" in full:
                                data["product_short"] = "DID"
                            elif "CLOUD" in full:
                                data["product_short"] = "Cloud"
                            elif "BROADBAND" in full:
                                data["product_short"] = "Broadband"
                            else:
                                # Use first word as abbreviation
                                data["product_short"] = (data.get("product") or "N/A").split()[0] if data.get("product") else "N/A"

                        # Calculate due date
                        entity_short = data.get("entity", "")
                        data["calculated_due_date"] = calculate_due_date(
                            data.get("invoice_date"),
                            entity_short,
                            data.get("vendor_name"),
                            data.get("account_no"),
                        )

                        # Generate description using product_short
                        data["description"] = generate_description(
                            data.get("product_short"),
                            data.get("circle"),
                            entity_short,
                            data.get("invoice_date"),
                            data.get("bill_period_start"),
                            data.get("bill_period_end"),
                        )

                        # Check for duplicate
                        data["_is_duplicate"] = check_duplicate(
                            data.get("vendor_name", ""),
                            data.get("invoice_no", ""),
                            data.get("invoice_date", ""),
                            data.get("invoice_value", ""),
                        )

                        # Received date = today (upload date)
                        data["received_date"] = datetime.now().strftime("%d-%b-%y")

                        st.session_state.extracted_invoices.append(data)
                except Exception as e:
                    st.error(f"Error extracting {filename}: {e}")

            progress.progress(1.0, text="Extraction complete!")
            st.success(f"Extracted {len(st.session_state.extracted_invoices)} invoice(s) from {len(pdf_files)} PDF(s). Go to **Review & Edit** to verify.")
            st.session_state.current_invoice_idx = 0

    # Show extraction summary if we have results
    if st.session_state.extracted_invoices:
        st.divider()
        st.markdown("### Extraction Summary")
        summary_data = []
        for inv in st.session_state.extracted_invoices:
            dup_flag = "DUPLICATE" if inv.get("_is_duplicate") else "OK"
            summary_data.append({
                "File": inv.get("_source_file", ""),
                "Entity": inv.get("entity", ""),
                "Invoice #": inv.get("invoice_no", ""),
                "Date": inv.get("invoice_date", ""),
                "Amount": inv.get("total_current_charges", ""),
                "Status": dup_flag,
            })
        st.dataframe(summary_data, use_container_width=True, hide_index=True)


# ─── PAGE: Review & Edit ───
elif page == "Review & Edit":
    st.markdown("### Review & Edit Extracted Invoices")

    if not st.session_state.extracted_invoices:
        st.info("No invoices extracted yet. Go to **Upload & Extract** first.")
    else:
        invoices = st.session_state.extracted_invoices
        n = len(invoices)

        # Invoice selector
        col_nav1, col_nav2, col_nav3 = st.columns([1, 3, 1])
        with col_nav1:
            if st.button("← Prev", disabled=st.session_state.current_invoice_idx <= 0):
                st.session_state.current_invoice_idx -= 1
                st.rerun()
        with col_nav2:
            idx = st.session_state.current_invoice_idx
            st.markdown(f"**Invoice {idx + 1} of {n}** — `{invoices[idx].get('_source_file', '')}`")
        with col_nav3:
            if st.button("Next →", disabled=st.session_state.current_invoice_idx >= n - 1):
                st.session_state.current_invoice_idx += 1
                st.rerun()

        inv = invoices[st.session_state.current_invoice_idx]

        # Duplicate warning
        if inv.get("_is_duplicate"):
            st.markdown('<div class="alert-duplicate"><strong>⚠ DUPLICATE DETECTED</strong> — This invoice matches an already-processed invoice (same Vendor + Invoice No + Date + Value). Posting is blocked.</div>', unsafe_allow_html=True)

        # Editable form
        with st.form(key=f"invoice_form_{st.session_state.current_invoice_idx}"):
            st.markdown("#### Core Fields")
            col1, col2, col3 = st.columns(3)

            with col1:
                entity = st.selectbox("Entity (Company)", ["Exotel", "Veeno", "Drishti"],
                                      index=["Exotel", "Veeno", "Drishti"].index(inv.get("entity", "Exotel")) if inv.get("entity") in ["Exotel", "Veeno", "Drishti"] else 0)
                vendor_name = st.text_input("Vendor Name", value=inv.get("vendor_name", ""))
                invoice_no = st.text_input("Invoice No.", value=str(inv.get("invoice_no", "")))
                account_no = st.text_input("Account No.", value=str(inv.get("account_no", "")))

            with col2:
                invoice_date = st.text_input("Invoice Date", value=inv.get("invoice_date", ""))
                start_date = st.text_input("Start Date (Bill Period)", value=inv.get("bill_period_start", ""))
                end_date = st.text_input("End Date (Bill Period)", value=inv.get("bill_period_end", ""))
                received_date = st.text_input("Received Date", value=inv.get("received_date", ""))

            with col3:
                product_short = st.text_input("Product (Short: SIP, PRI, ILL...)", value=inv.get("product_short", ""))
                circle = st.text_input("Circle (State)", value=inv.get("circle", ""))
                cogs = st.selectbox("COGS / Non-COGS", ["COGS", "Non-COGS"],
                                    index=0 if inv.get("cogs_classification", "COGS") == "COGS" else 1)
                invoice_category = st.selectbox("Invoice Category", ["Recurring", "One-time"],
                                                index=0 if inv.get("invoice_category", "Recurring") == "Recurring" else 1)

            st.markdown("#### Financial Details")
            fcol1, fcol2, fcol3, fcol4 = st.columns(4)

            with fcol1:
                currency = st.selectbox("Currency", ["INR", "USD", "EUR", "GBP", "Other"],
                                        index=0)
                rental_charges = st.number_input("Rental Charges", value=float(inv.get("rental_charges") or 0), format="%.2f")
                usage_charges = st.number_input("Usage Charges", value=float(inv.get("usage_charges") or 0), format="%.2f")

            with fcol2:
                subtotal = st.number_input("Subtotal (w/o Tax)", value=float(inv.get("subtotal_without_tax") or 0), format="%.2f")
                one_time = st.number_input("One-time Charges", value=float(inv.get("one_time_charges") or 0), format="%.2f")
                gst_amount = st.number_input("GST/Tax Amount", value=float(inv.get("gst_amount") or 0), format="%.2f")

            with fcol3:
                total_charges = st.number_input("Total Current Charges", value=float(inv.get("total_current_charges") or 0), format="%.2f")
                invoice_value = st.number_input("Invoice Value (Amount Due)", value=float(inv.get("invoice_value") or 0), format="%.2f")
                previous_balance = st.number_input("Previous Balance", value=float(inv.get("previous_balance") or 0), format="%.2f")

            with fcol4:
                fx_original_value = st.text_input("Original Foreign Currency Value", value="", help="If currency is not INR, enter original value here")
                cgst = st.number_input("CGST", value=float(inv.get("cgst") or 0), format="%.2f")
                sgst = st.number_input("SGST", value=float(inv.get("sgst") or 0), format="%.2f")

            # Rental + Usage validation
            if rental_charges + usage_charges > 0 and subtotal > 0:
                expected_net = rental_charges + usage_charges
                if abs(expected_net - subtotal) > 1:
                    st.warning(f"Rental ({rental_charges:,.2f}) + Usage ({usage_charges:,.2f}) = {expected_net:,.2f} does not match Subtotal ({subtotal:,.2f})")

            # Previous balance highlight
            if previous_balance and abs(previous_balance) > total_charges and total_charges > 0:
                st.warning("Previous balance exceeds one month's charges — please review.")

            st.markdown("#### Due Date & Description")
            dcol1, dcol2 = st.columns(2)
            with dcol1:
                due_date_invoice = st.text_input("Due Date (on invoice)", value=inv.get("due_date_on_invoice", ""))
                calculated_due = calculate_due_date(invoice_date, entity, vendor_name, account_no)
                due_date_calc = st.text_input("Due Date (calculated)", value=str(calculated_due))
            with dcol2:
                desc = generate_description(product_short, circle, entity, invoice_date, start_date, end_date)
                description = st.text_area("Description", value=desc, height=80)

            st.markdown("#### Additional Info")
            acol1, acol2 = st.columns(2)
            with acol1:
                customer_gstin = st.text_input("Customer GSTIN", value=inv.get("customer_gstin", ""))
                vendor_gstin = st.text_input("Vendor GSTIN", value=inv.get("vendor_gstin", ""))
            with acol2:
                circuit_ids = st.text_input("Circuit IDs", value=inv.get("circuit_ids", ""))
                notes = st.text_area("Notes / Remarks", value=inv.get("notes", "") or "", height=80,
                                     help="For foreign currency invoices, put original amount here")

            # Submit buttons
            bcol1, bcol2, bcol3 = st.columns(3)
            with bcol1:
                save_btn = st.form_submit_button("Save Changes", use_container_width=True)
            with bcol2:
                approve_btn = st.form_submit_button("Approve & Post", type="primary", use_container_width=True)
            with bcol3:
                skip_btn = st.form_submit_button("Skip (Exception)", use_container_width=True)

        if save_btn:
            # Update the invoice in session state
            updated = {
                **inv,
                "entity": entity,
                "vendor_name": vendor_name,
                "invoice_no": invoice_no,
                "account_no": account_no,
                "invoice_date": invoice_date,
                "bill_period_start": start_date,
                "bill_period_end": end_date,
                "received_date": received_date,
                "product_short": product_short,
                "circle": circle,
                "cogs_classification": cogs,
                "invoice_category": invoice_category,
                "currency": currency,
                "rental_charges": rental_charges,
                "usage_charges": usage_charges,
                "subtotal_without_tax": subtotal,
                "one_time_charges": one_time,
                "gst_amount": gst_amount,
                "total_current_charges": total_charges,
                "invoice_value": invoice_value,
                "previous_balance": previous_balance,
                "cgst": cgst,
                "sgst": sgst,
                "due_date_on_invoice": due_date_invoice,
                "calculated_due_date": due_date_calc,
                "description": description,
                "customer_gstin": customer_gstin,
                "vendor_gstin": vendor_gstin,
                "circuit_ids": circuit_ids,
                "notes": notes,
            }
            st.session_state.extracted_invoices[st.session_state.current_invoice_idx] = updated
            st.success("Changes saved.")

        if approve_btn:
            if inv.get("_is_duplicate"):
                st.error("Cannot post — duplicate invoice detected. Resolve before posting.")
            else:
                # Validate mandatory fields
                missing = []
                if not entity:
                    missing.append("Entity")
                if not vendor_name:
                    missing.append("Vendor Name")
                if not invoice_no:
                    missing.append("Invoice No.")
                if not invoice_date:
                    missing.append("Invoice Date")
                if not invoice_value:
                    missing.append("Invoice Value")
                if not account_no:
                    missing.append("Account No.")

                if missing:
                    st.error(f"Missing mandatory fields: {', '.join(missing)}")
                else:
                    # Build final record
                    record = {
                        "entity": entity,
                        "vendor_name": vendor_name,
                        "invoice_no": invoice_no,
                        "account_no": account_no,
                        "invoice_date": invoice_date,
                        "bill_period_start": start_date,
                        "bill_period_end": end_date,
                        "received_date": received_date,
                        "product": inv.get("product", ""),
                        "product_short": product_short,
                        "circle": circle,
                        "cogs_classification": cogs,
                        "invoice_category": invoice_category,
                        "currency": currency,
                        "rental_charges": rental_charges,
                        "usage_charges": usage_charges,
                        "subtotal_without_tax": subtotal,
                        "one_time_charges": one_time,
                        "gst_amount": gst_amount,
                        "total_current_charges": total_charges,
                        "invoice_value": invoice_value,
                        "invoice_value_without_tax": subtotal + one_time,
                        "previous_balance": previous_balance,
                        "cgst": cgst,
                        "sgst": sgst,
                        "due_date_on_invoice": due_date_invoice,
                        "calculated_due_date": due_date_calc,
                        "description": description,
                        "customer_gstin": customer_gstin,
                        "vendor_gstin": vendor_gstin,
                        "circuit_ids": circuit_ids,
                        "notes": notes,
                        "posted_at": datetime.now().isoformat(),
                        "_source_file": inv.get("_source_file", ""),
                    }

                    # Save to processed
                    processed = get_processed_invoices()
                    processed.append(record)
                    save_processed_invoices(processed)

                    # Learn vendor → entity mapping
                    vendor_mappings = get_vendor_mappings()
                    v_key = vendor_name.strip().lower()
                    if v_key and v_key not in vendor_mappings:
                        vendor_mappings[v_key] = {
                            "entity": entity,
                            "vendor_name": vendor_name,
                            "learned_at": datetime.now().isoformat(),
                        }
                        save_vendor_mappings(vendor_mappings)

                    # Learn account → product/COGS mapping
                    account_mappings = get_account_mappings()
                    a_key = str(account_no).strip()
                    if a_key:
                        account_mappings[a_key] = {
                            "product": inv.get("product", ""),
                            "product_short": product_short,
                            "cogs": cogs,
                            "entity": entity,
                            "circle": circle,
                            "learned_at": datetime.now().isoformat(),
                        }
                        save_account_mappings(account_mappings)

                    # Remove from extraction queue
                    st.session_state.extracted_invoices.pop(st.session_state.current_invoice_idx)
                    if st.session_state.current_invoice_idx >= len(st.session_state.extracted_invoices):
                        st.session_state.current_invoice_idx = max(0, len(st.session_state.extracted_invoices) - 1)

                    st.success(f"Invoice {invoice_no} posted successfully! Vendor & account mappings learned.")
                    st.rerun()

        if skip_btn:
            st.warning(f"Invoice {inv.get('invoice_no', '')} sent to exception queue (skipped).")


# ─── PAGE: Processed Invoices ───
elif page == "Processed Invoices":
    st.markdown("### Processed Invoices")
    processed = get_processed_invoices()

    if not processed:
        st.info("No invoices have been processed yet.")
    else:
        # Filters
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            entity_filter = st.multiselect("Filter by Entity", ["Exotel", "Veeno", "Drishti"], default=[])
        with fcol2:
            cogs_filter = st.multiselect("Filter by COGS", ["COGS", "Non-COGS"], default=[])
        with fcol3:
            search_q = st.text_input("Search (Invoice #, Vendor...)", "")

        filtered = processed
        if entity_filter:
            filtered = [i for i in filtered if i.get("entity") in entity_filter]
        if cogs_filter:
            filtered = [i for i in filtered if i.get("cogs_classification") in cogs_filter]
        if search_q:
            q = search_q.lower()
            filtered = [i for i in filtered if q in str(i.get("invoice_no", "")).lower()
                        or q in str(i.get("vendor_name", "")).lower()
                        or q in str(i.get("account_no", "")).lower()]

        # Display table matching Trello Tracker columns
        display_data = []
        for sno, inv in enumerate(filtered, 1):
            inv_value = inv.get("invoice_value") or inv.get("total_current_charges") or ""
            gst_bc = inv.get("invoice_value_without_tax") or inv.get("subtotal_without_tax") or ""
            display_data.append({
                "SNO": sno,
                "Date": inv.get("received_date", ""),
                "PI NO": "",
                "Tracker No": "",
                "Cmp": inv.get("entity", ""),
                "Cogs/Noncogs": "Cogs" if inv.get("cogs_classification") == "COGS" else "Noncogs",
                "Vendor no.": inv.get("vendor_name", ""),
                "Product": inv.get("product_short", "") or inv.get("product", ""),
                "Circle": inv.get("circle", ""),
                "Description": inv.get("description", ""),
                "Account No": inv.get("account_no", ""),
                "InvoiceNo": inv.get("invoice_no", ""),
                "Repeat Number": 1,
                "Received dt": inv.get("received_date", ""),
                "Due Date": inv.get("calculated_due_date", ""),
                "Inv dt": inv.get("invoice_date", ""),
                "Start Date": inv.get("bill_period_start", ""),
                "End dt": inv.get("bill_period_end", ""),
                "Invoice Value": fmt_currency(inv_value) if inv_value else "",
                "GST/BC": fmt_currency(gst_bc) if gst_bc else "",
                "Previous Balance": fmt_currency(inv.get("previous_balance", "")) if inv.get("previous_balance") else "",
                "Remarks": inv.get("notes", "") or "",
            })
        st.dataframe(display_data, use_container_width=True, hide_index=True)

        # Export in Trello Tracker format
        def build_tracker_row(sno, inv):
            """Build a row matching the Trello Tracker FY 26 spreadsheet format."""
            inv_value = inv.get("invoice_value") or inv.get("total_current_charges") or ""
            gst_bc = inv.get("invoice_value_without_tax") or inv.get("subtotal_without_tax") or ""
            prev_bal = inv.get("previous_balance", "")
            diff = ""
            try:
                if inv_value and prev_bal:
                    diff = fmt_currency(float(inv_value) - float(prev_bal))
            except (ValueError, TypeError):
                pass

            # Format Exotel/Veeno column values
            exotel_val = fmt_currency(inv.get("invoice_value", "")) if inv.get("entity") == "Exotel" and inv.get("invoice_value") else ""
            veeno_val = fmt_currency(inv.get("invoice_value", "")) if inv.get("entity") == "Veeno" and inv.get("invoice_value") else ""

            return {
                "SNO": sno,
                "Date": inv.get("received_date", ""),
                "PI NO": "",
                "Tracker No": "",
                "Cmp": inv.get("entity", ""),
                "Cogs/Noncogs": "Cogs" if inv.get("cogs_classification") == "COGS" else "Noncogs",
                "Vendor no.": inv.get("vendor_name", ""),
                "Product": inv.get("product_short", "") or inv.get("product", ""),
                "Circle": inv.get("circle", ""),
                "Description": inv.get("description", ""),
                "Account No": inv.get("account_no", ""),
                "InvoiceNo": inv.get("invoice_no", ""),
                "Repeat Number": 1,
                "Received dt": inv.get("received_date", ""),
                "Due Date": inv.get("calculated_due_date", ""),
                "Inv dt": inv.get("invoice_date", ""),
                "Start Date": inv.get("bill_period_start", ""),
                "End dt": inv.get("bill_period_end", ""),
                "Invoice Value": fmt_currency(inv_value) if inv_value else "",
                "GST/BC": fmt_currency(gst_bc) if gst_bc else "",
                "Previous Balance": fmt_currency(prev_bal) if prev_bal else "",
                "Paid date": "",
                "Payment ID": "",
                "Remarks": inv.get("notes", "") or "",
                "SACK use /": "",
                "Sack Value use /": "",
                "AER": "",
                "Difference b/w Invoice Value & Previous Outstanding": diff,
                "Intercomapny Billing": "",
                "Vendor submit date": "",
                "Invoice Upload": "",
                "Ageing": "",
                "Submit SLA": "",
                "Upload SLA": "",
                "Payment SLA": "",
                "count of SLA": "",
                "AC_update": "",
                "Final Approval": "",
                "Booking Status": "",
                "Exotel": exotel_val,
                "Veeno": veeno_val,
            }

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

        if st.button("Export to Tracker CSV", type="primary", use_container_width=True):
            if filtered:
                tracker_rows = [build_tracker_row(i + 1, inv) for i, inv in enumerate(filtered)]
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=TRACKER_COLUMNS)
                writer.writeheader()
                writer.writerows(tracker_rows)
                st.download_button(
                    "Download Tracker CSV",
                    output.getvalue(),
                    file_name=f"Trello_Tracker_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )


# ─── PAGE: Reports ───
elif page == "Reports":
    st.markdown("### Reports & Analytics")
    processed = get_processed_invoices()

    if not processed:
        st.info("No data available for reports. Process some invoices first.")
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Entity Summary", "COGS Summary", "Due Date Aging", "Duplicate Check", "Vendor Credit"
        ])

        with tab1:
            st.markdown("#### Entity-wise Invoice Summary")
            entity_summary = {}
            for inv in processed:
                e = inv.get("entity", "Unknown")
                if e not in entity_summary:
                    entity_summary[e] = {"count": 0, "total_value": 0, "total_gst": 0}
                entity_summary[e]["count"] += 1
                entity_summary[e]["total_value"] += float(inv.get("total_current_charges") or 0)
                entity_summary[e]["total_gst"] += float(inv.get("gst_amount") or 0)
            rows = [{"Entity": k, "Invoice Count": v["count"],
                      "Total Value (incl. tax)": f"₹{v['total_value']:,.2f}",
                      "Total GST": f"₹{v['total_gst']:,.2f}"}
                     for k, v in entity_summary.items()]
            st.dataframe(rows, use_container_width=True, hide_index=True)

        with tab2:
            st.markdown("#### COGS vs Non-COGS Summary")
            cogs_summary = {}
            for inv in processed:
                c = inv.get("cogs_classification", "COGS")
                if c not in cogs_summary:
                    cogs_summary[c] = {"count": 0, "total_value": 0}
                cogs_summary[c]["count"] += 1
                cogs_summary[c]["total_value"] += float(inv.get("total_current_charges") or 0)
            rows = [{"Classification": k, "Invoice Count": v["count"],
                      "Total Value": f"₹{v['total_value']:,.2f}"}
                     for k, v in cogs_summary.items()]
            st.dataframe(rows, use_container_width=True, hide_index=True)

        with tab3:
            st.markdown("#### Due Date Aging Report")
            today = datetime.now()
            aging_buckets = {"Current (not due)": [], "1-30 days overdue": [],
                             "31-60 days overdue": [], "60+ days overdue": []}
            for inv in processed:
                due_str = inv.get("calculated_due_date", "")
                if due_str and due_str != "Unable to calculate":
                    try:
                        due_dt = datetime.strptime(due_str, "%d-%b-%y")
                        diff = (today - due_dt).days
                        if diff < 0:
                            aging_buckets["Current (not due)"].append(inv)
                        elif diff <= 30:
                            aging_buckets["1-30 days overdue"].append(inv)
                        elif diff <= 60:
                            aging_buckets["31-60 days overdue"].append(inv)
                        else:
                            aging_buckets["60+ days overdue"].append(inv)
                    except ValueError:
                        pass
            rows = [{"Bucket": k, "Invoice Count": len(v),
                      "Total Value": f"₹{sum(float(i.get('total_current_charges') or 0) for i in v):,.2f}"}
                     for k, v in aging_buckets.items()]
            st.dataframe(rows, use_container_width=True, hide_index=True)

        with tab4:
            st.markdown("#### Duplicate Invoice Report")
            seen = {}
            duplicates = []
            for inv in processed:
                key = f"{inv.get('vendor_name', '')}|{inv.get('invoice_no', '')}|{inv.get('invoice_date', '')}"
                if key in seen:
                    duplicates.append(inv)
                    if seen[key] not in duplicates:
                        duplicates.append(seen[key])
                else:
                    seen[key] = inv
            if duplicates:
                st.warning(f"Found {len(duplicates)} potential duplicate entries")
                dup_rows = [{"Vendor": d.get("vendor_name"), "Invoice #": d.get("invoice_no"),
                              "Date": d.get("invoice_date"), "Value": d.get("total_current_charges")}
                             for d in duplicates]
                st.dataframe(dup_rows, use_container_width=True, hide_index=True)
            else:
                st.success("No duplicates found.")

        with tab5:
            st.markdown("#### Vendor Credit Period Report")
            overrides = get_credit_overrides()
            rows = []
            for entity, days in STANDARD_CREDIT_DAYS.items():
                rows.append({"Entity/Type": entity, "Standard Credit Days": days, "Type": "Standard"})
            for vendor, days in overrides.get("vendor", {}).items():
                rows.append({"Entity/Type": vendor, "Standard Credit Days": days, "Type": "Vendor Override"})
            for acct, days in overrides.get("account", {}).items():
                rows.append({"Entity/Type": f"Account {acct}", "Standard Credit Days": days, "Type": "Account Override"})
            st.dataframe(rows, use_container_width=True, hide_index=True)


# ─── PAGE: Settings ───
elif page == "Settings":
    st.markdown("### Settings & Configuration")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Credit Period Overrides", "Vendor Mappings", "Account Mappings", "Edit Log"
    ])

    with tab1:
        st.markdown("#### Credit Period Overrides")
        st.caption("Override standard credit days at vendor or account level.")
        overrides = get_credit_overrides()
        if "vendor" not in overrides:
            overrides["vendor"] = {}
        if "account" not in overrides:
            overrides["account"] = {}

        st.markdown("**Standard Credit Periods:**")
        for entity, days in STANDARD_CREDIT_DAYS.items():
            st.text(f"  {entity}: {days} days")

        st.markdown("**Add Vendor-level Override:**")
        ov_col1, ov_col2, ov_col3 = st.columns([2, 1, 1])
        with ov_col1:
            ov_vendor = st.text_input("Vendor name (lowercase)", key="ov_vendor")
        with ov_col2:
            ov_days = st.number_input("Credit days", min_value=1, max_value=365, value=30, key="ov_days")
        with ov_col3:
            if st.button("Add Vendor Override"):
                if ov_vendor:
                    overrides["vendor"][ov_vendor.lower().strip()] = ov_days
                    save_credit_overrides(overrides)
                    st.success(f"Added: {ov_vendor} → {ov_days} days")
                    st.rerun()

        st.markdown("**Add Account-level Override:**")
        ac_col1, ac_col2, ac_col3 = st.columns([2, 1, 1])
        with ac_col1:
            ov_acct = st.text_input("Account number", key="ov_acct")
        with ac_col2:
            ov_acct_days = st.number_input("Credit days", min_value=1, max_value=365, value=45, key="ov_acct_days")
        with ac_col3:
            if st.button("Add Account Override"):
                if ov_acct:
                    overrides["account"][ov_acct.strip()] = ov_acct_days
                    save_credit_overrides(overrides)
                    st.success(f"Added: Account {ov_acct} → {ov_acct_days} days")
                    st.rerun()

        # Show existing overrides
        if overrides.get("vendor") or overrides.get("account"):
            st.markdown("**Current Overrides:**")
            override_rows = []
            for v, d in overrides.get("vendor", {}).items():
                override_rows.append({"Type": "Vendor", "Key": v, "Credit Days": d})
            for a, d in overrides.get("account", {}).items():
                override_rows.append({"Type": "Account", "Key": a, "Credit Days": d})
            st.dataframe(override_rows, use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("#### Learned Vendor Mappings")
        st.caption("These are vendor → entity mappings the system has learned from processed invoices.")
        vendor_mappings = get_vendor_mappings()
        if vendor_mappings:
            rows = [{"Vendor (key)": k, "Entity": v.get("entity", ""), "Vendor Name": v.get("vendor_name", ""),
                      "Learned": v.get("learned_at", "")}
                     for k, v in vendor_mappings.items()]
            st.dataframe(rows, use_container_width=True, hide_index=True)

            if st.button("Clear All Vendor Mappings"):
                save_vendor_mappings({})
                st.success("Vendor mappings cleared.")
                st.rerun()
        else:
            st.info("No vendor mappings learned yet. Process some invoices to build the mapping.")

    with tab3:
        st.markdown("#### Learned Account Mappings")
        st.caption("These are account → product/COGS mappings learned from processed invoices.")
        account_mappings = get_account_mappings()
        if account_mappings:
            rows = [{"Account No": k, "Product": v.get("product", ""), "Product Short": v.get("product_short", ""),
                      "COGS": v.get("cogs", ""),
                      "Entity": v.get("entity", ""), "Circle": v.get("circle", ""),
                      "Learned": v.get("learned_at", "")}
                     for k, v in account_mappings.items()]
            st.dataframe(rows, use_container_width=True, hide_index=True)

            if st.button("Clear All Account Mappings"):
                save_account_mappings({})
                st.success("Account mappings cleared.")
                st.rerun()
        else:
            st.info("No account mappings learned yet. Process some invoices to build the mapping.")

    with tab4:
        st.markdown("#### Edit Audit Log")
        edit_log = get_edit_log()
        if edit_log:
            st.dataframe(edit_log[-50:][::-1], use_container_width=True, hide_index=True)  # Show last 50, newest first
        else:
            st.info("No edits logged yet.")
