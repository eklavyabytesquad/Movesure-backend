"""
Permanent Details (print header) + Print Templates + Bill Book Template Service
================================================================================
permanent_details: one print-header row per (branch_id, company_id) — see
migrations/022. company_id NULL = the branch default row.

print_templates: registry of layouts that exist in the frontend as
templates/<code>.js (template1.js, template2.js, ...).

bill_books.print_template: the layout a series prints with — the bill book
chosen on the bilty page decides the template.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

TABLE = "permanent_details"
FIELDS = ("transport_name", "gst", "mobile_number", "bank_act_no_1", "ifsc_code_1",
          "bank_act_no_2", "ifsc_code_2", "transport_address", "website")
COLS = "id, branch_id, company_id, " + ", ".join(FIELDS)
DEFAULT_TEMPLATE = "template1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400) -> dict:
    return {"status": "error", "message": msg, "status_code": code}


def _clean(v):
    v = (str(v).strip() if v is not None else "")
    return v or None


# ── Templates ─────────────────────────────────────────────────────────────────
def list_print_templates(doc_type: str = "bilty", include_inactive: bool = False) -> dict:
    q = get_supabase().table("print_templates").select("*").eq("doc_type", doc_type).order("sort_order")
    if not include_inactive:
        q = q.eq("is_active", True)
    rows = q.execute().data or []
    return {"status": "success", "data": rows, "count": len(rows), "default_template": DEFAULT_TEMPLATE}


def _check_template(sb, code: str) -> str | None:
    r = sb.table("print_templates").select("code, is_active").eq("code", code).limit(1).execute().data
    if not r:
        return f"Unknown print_template '{code}'"
    if not r[0]["is_active"]:
        return f"Print template '{code}' is inactive"
    return None


# ── Permanent details CRUD ────────────────────────────────────────────────────
def _with_names(sb, rows: list[dict]) -> list[dict]:
    """Attach branch_name / company_name for the settings list."""
    b_ids = list({r["branch_id"] for r in rows if r.get("branch_id")})
    c_ids = list({r["company_id"] for r in rows if r.get("company_id")})
    branches = {b["id"]: b["branch_name"] for b in
                (sb.table("branches").select("id, branch_name").in_("id", b_ids).execute().data if b_ids else [])}
    companies = {c["id"]: c["company_name"] for c in
                 (sb.table("companies").select("id, company_name").in_("id", c_ids).execute().data if c_ids else [])}
    for r in rows:
        r["branch_name"] = branches.get(r.get("branch_id"))
        r["company_name"] = companies.get(r.get("company_id"))
        r["is_default"] = not r.get("company_id")
    return rows


def list_permanent_details(branch_id: str | None = None) -> dict:
    sb = get_supabase()
    q = sb.table(TABLE).select(COLS)
    if branch_id:
        q = q.eq("branch_id", branch_id)
    rows = q.execute().data or []
    rows.sort(key=lambda r: (r.get("branch_id") or "", r.get("company_id") is not None))
    return {"status": "success", "data": _with_names(sb, rows), "count": len(rows)}


def _fetch(sb, pd_id: str):
    r = sb.table(TABLE).select(COLS).eq("id", pd_id).limit(1).execute().data
    return r[0] if r else None


def get_permanent_detail(pd_id: str) -> dict:
    sb = get_supabase()
    row = _fetch(sb, pd_id)
    if not row:
        return _err("Permanent details not found", 404)
    return {"status": "success", "data": _with_names(sb, [row])[0]}


def _slot_taken(sb, branch_id: str, company_id: str | None, exclude_id: str | None = None) -> bool:
    q = sb.table(TABLE).select("id").eq("branch_id", branch_id)
    q = q.eq("company_id", company_id) if company_id else q.is_("company_id", "null")
    return any(r["id"] != exclude_id for r in (q.execute().data or []))


def create_permanent_detail(data: dict) -> dict:
    """data: branch_id (required), company_id (optional, null = branch default), transport_name, gst, mobile_number,
    bank_act_no_1, ifsc_code_1, bank_act_no_2, ifsc_code_2, transport_address, website."""
    sb = get_supabase()
    branch_id = data.get("branch_id")
    if not branch_id:
        return _err("branch_id is required")
    company_id = data.get("company_id") or None
    if company_id and not sb.table("companies").select("id").eq("id", company_id).execute().data:
        return _err("Company not found", 404)
    if _slot_taken(sb, branch_id, company_id):
        return _err("Permanent details already exist for this branch" +
                    (" and company" if company_id else " (default row)"), 409)
    row = {"branch_id": branch_id, "company_id": company_id}
    for f in FIELDS:
        row[f] = _clean(data.get(f))
    r = sb.table(TABLE).insert(row).execute()
    return {"status": "success", "message": "Permanent details created",
            "data": _with_names(sb, [r.data[0]])[0]}


def update_permanent_detail(pd_id: str, data: dict) -> dict:
    """Send only the fields to change. branch_id / company_id can't be changed
    (create a new row instead) so old bilties never switch header."""
    sb = get_supabase()
    row = _fetch(sb, pd_id)
    if not row:
        return _err("Permanent details not found", 404)
    for locked in ("branch_id", "company_id"):
        if locked in data and (data[locked] or None) != row.get(locked):
            return _err(f"{locked} cannot be changed; create a new permanent details row instead")
    payload = {}
    for f in FIELDS:
        if f in data:
            payload[f] = _clean(data.get(f))
    if not payload:
        return _err("Nothing to update")
    sb.table(TABLE).update(payload).eq("id", pd_id).execute()
    return {"status": "success", "message": "Permanent details updated",
            "data": _with_names(sb, [_fetch(sb, pd_id)])[0]}


def delete_permanent_detail(pd_id: str) -> dict:
    """Only company rows can be deleted; the branch default row is protected
    because every old bilty prints from it."""
    sb = get_supabase()
    row = _fetch(sb, pd_id)
    if not row:
        return _err("Permanent details not found", 404)
    if not row.get("company_id"):
        return _err("The branch default row cannot be deleted — old bilties print from it")
    sb.table(TABLE).delete().eq("id", pd_id).execute()
    return {"status": "success", "message": "Permanent details deleted"}


# ── Bill book (series) template ───────────────────────────────────────────────
BOOK_COLS = ("id, branch_id, prefix, postfix, digits, from_number, to_number, current_number, "
             "company_id, print_template, is_active, is_completed")


def list_bill_book_templates(branch_id: str | None = None, include_inactive: bool = False) -> dict:
    """Bill books with their print_template + branch/company names (Settings -> Bill Books)."""
    sb = get_supabase()
    q = sb.table("bill_books").select(BOOK_COLS).order("created_at")
    if branch_id:
        q = q.eq("branch_id", branch_id)
    if not include_inactive:
        q = q.eq("is_active", True)
    rows = _with_names(sb, q.execute().data or [])
    for r in rows:
        r.pop("is_default", None)
    return {"status": "success", "data": rows, "count": len(rows)}


def set_bill_book_template(bill_book_id: str, data: dict) -> dict:
    """data: { print_template }. Only the template changes — numbering, company and
    already-saved bilties are untouched (reprints of this series use the new layout)."""
    sb = get_supabase()
    code = _clean(data.get("print_template"))
    if not code:
        return _err("print_template is required")
    if not sb.table("bill_books").select("id").eq("id", bill_book_id).execute().data:
        return _err("Bill book not found", 404)
    err = _check_template(sb, code)
    if err:
        return _err(err)
    sb.table("bill_books").update({"print_template": code, "updated_at": _now()}).eq("id", bill_book_id).execute()
    row = sb.table("bill_books").select(BOOK_COLS).eq("id", bill_book_id).execute().data[0]
    return {"status": "success", "message": "Print template updated", "data": row}
