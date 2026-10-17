"""
Permanent Details (print header) + Print Templates Service
===========================================================
permanent_details: one print-header row per (branch_id, company_id) — see
migrations/022. company_id NULL = the branch default row. Each row carries
print_template (FK -> print_templates.code), the layout the frontend uses
by default whenever it prints a bilty for that branch/company.

print_templates: registry of layouts that exist in the frontend as
templates/<code>.js (template1.js, template2.js, ...).
"""
from services.supabase_client import get_supabase

TABLE = "permanent_details"
FIELDS = ("transport_name", "gst", "mobile_number", "bank_act_no_1", "ifsc_code_1",
          "bank_act_no_2", "ifsc_code_2", "transport_address", "website")
COLS = "id, branch_id, company_id, print_template, " + ", ".join(FIELDS)
DEFAULT_TEMPLATE = "template1"


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
    """data: branch_id (required), company_id (optional, null = branch default),
    print_template (default template1), transport_name, gst, mobile_number,
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
    template = _clean(data.get("print_template")) or DEFAULT_TEMPLATE
    err = _check_template(sb, template)
    if err:
        return _err(err)
    row = {"branch_id": branch_id, "company_id": company_id, "print_template": template}
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
    if "print_template" in data:
        template = _clean(data.get("print_template")) or DEFAULT_TEMPLATE
        if template != row.get("print_template"):
            err = _check_template(sb, template)
            if err:
                return _err(err)
        payload["print_template"] = template
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
