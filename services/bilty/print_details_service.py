"""
Bilty Print Details Service
=============================
Resolves what a bilty prints with (see migrations/022):

  header   -> permanent_details row:
                1. branch_id = bilty.branch_id AND company_id = bilty.company_id
                2. else the branch's default row (company_id IS NULL)  <- every old bilty
              The `companies` table is NOT used for the print header.
  template -> the bill book (series) the bilty was booked from:
                1. bill_books.print_template via bilty.bill_book_id (new bilties)
                2. else the branch bill book whose prefix/postfix + number range match the GR no (old bilties)
                3. else 'template1'
"""
import re
from services.supabase_client import get_supabase

DEFAULT_TEMPLATE = "template1"
HEADER_COLS = ("id, branch_id, company_id, transport_name, gst, mobile_number, bank_act_no_1, ifsc_code_1, "
               "bank_act_no_2, ifsc_code_2, transport_address, website")
BOOK_COLS = "id, branch_id, prefix, postfix, digits, from_number, to_number, company_id, print_template, is_active"


def pick_print_details(rows: list[dict], branch_id: str | None, company_id: str | None) -> dict | None:
    """Pure lookup over already-fetched permanent_details rows (frontend mirrors this)."""
    branch_rows = [r for r in rows if r.get("branch_id") == branch_id]
    if company_id:
        exact = next((r for r in branch_rows if r.get("company_id") == company_id), None)
        if exact:
            return exact
    return next((r for r in branch_rows if not r.get("company_id")), None)


def match_book_by_gr(books: list[dict], gr_no: str) -> dict | None:
    """Old bilties have no bill_book_id: find the book whose prefix/postfix wrap the
    GR no and whose range contains its number. Active books win over inactive ones."""
    gr = (gr_no or "").strip().upper()
    hits = []
    for b in books:
        pre, post = (b.get("prefix") or "").upper(), (b.get("postfix") or "").upper()
        if not gr.startswith(pre) or (post and not gr.endswith(post)):
            continue
        core = gr[len(pre):len(gr) - len(post) if post else None]
        if not re.fullmatch(r"\d+", core or ""):
            continue
        if int(b.get("from_number") or 0) <= int(core) <= int(b.get("to_number") or 0):
            hits.append(b)
    hits.sort(key=lambda b: (not b.get("is_active"), -len(b.get("prefix") or "")))
    return hits[0] if hits else None


def get_print_details(bilty_id: str | None = None, branch_id: str | None = None,
                      company_id: str | None = None, bill_book_id: str | None = None) -> dict:
    """Either bilty_id (reads its branch/company/book), or branch_id [+ company_id, bill_book_id]
    for a preview before saving."""
    sb = get_supabase()
    gr_no = None
    if bilty_id:
        b = sb.table("bilty").select("gr_no, branch_id, company_id, bill_book_id").eq("id", bilty_id).limit(1).execute().data
        if not b:
            return {"status": "error", "message": "Bilty not found", "status_code": 404}
        gr_no, branch_id, company_id, bill_book_id = b[0]["gr_no"], b[0]["branch_id"], b[0]["company_id"], b[0]["bill_book_id"]
    if not branch_id:
        return {"status": "error", "message": "bilty_id or branch_id is required", "status_code": 400}

    # Template from the series
    book, source = None, "default"
    if bill_book_id:
        r = sb.table("bill_books").select(BOOK_COLS).eq("id", bill_book_id).limit(1).execute().data
        book, source = (r[0], "bill_book") if r else (None, "default")
    if not book and gr_no:
        books = sb.table("bill_books").select(BOOK_COLS).eq("branch_id", branch_id).execute().data or []
        book = match_book_by_gr(books, gr_no)
        source = "gr_match" if book else "default"
    template = (book or {}).get("print_template") or DEFAULT_TEMPLATE
    # Preview only (no saved bilty yet): the chosen book decides the company.
    # A saved bilty always keeps its own company_id, so old bilties never switch header.
    if not bilty_id and company_id is None and book:
        company_id = book.get("company_id")

    # Header from permanent details
    rows = sb.table("permanent_details").select(HEADER_COLS).eq("branch_id", branch_id).execute().data or []
    header = pick_print_details(rows, branch_id, company_id)
    if not header:
        return {"status": "error", "message": "No permanent details set for this branch", "status_code": 404}
    return {
        "status": "success",
        "data": header,
        "print_template": template,
        "bill_book_id": (book or {}).get("id"),
        "template_source": source,          # bill_book | gr_match | default
        # true when the company-specific header was missing and the branch default was used
        "is_fallback": bool(company_id) and header.get("company_id") != company_id,
    }
