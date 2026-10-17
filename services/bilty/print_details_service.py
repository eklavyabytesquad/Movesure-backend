"""
Bilty Print Details Service
=============================
Resolves the print header (permanent_details row) for a bilty.

permanent_details has one row per (branch_id, company_id) — see migrations/022:
  1. row with branch_id = bilty.branch_id AND company_id = bilty.company_id
  2. else the branch's default row (company_id IS NULL)  <- every old bilty lands here
The `companies` table is NOT used for the print header.
The row's print_template tells the frontend which templates/<code>.js to render.
"""
from services.supabase_client import get_supabase

COLS = ("id, branch_id, company_id, print_template, transport_name, gst, mobile_number, bank_act_no_1, ifsc_code_1, "
        "bank_act_no_2, ifsc_code_2, transport_address, website")


def pick_print_details(rows: list[dict], branch_id: str | None, company_id: str | None) -> dict | None:
    """Pure lookup over already-fetched permanent_details rows (frontend mirrors this)."""
    branch_rows = [r for r in rows if r.get("branch_id") == branch_id]
    if company_id:
        exact = next((r for r in branch_rows if r.get("company_id") == company_id), None)
        if exact:
            return exact
    return next((r for r in branch_rows if not r.get("company_id")), None)


def get_print_details(bilty_id: str | None = None, branch_id: str | None = None,
                      company_id: str | None = None) -> dict:
    """Either bilty_id (reads its branch_id/company_id), or branch_id (+ optional company_id)."""
    sb = get_supabase()
    if bilty_id:
        b = sb.table("bilty").select("branch_id, company_id").eq("id", bilty_id).limit(1).execute().data
        if not b:
            return {"status": "error", "message": "Bilty not found", "status_code": 404}
        branch_id, company_id = b[0]["branch_id"], b[0]["company_id"]
    if not branch_id:
        return {"status": "error", "message": "bilty_id or branch_id is required", "status_code": 400}

    rows = sb.table("permanent_details").select(COLS).eq("branch_id", branch_id).execute().data or []
    row = pick_print_details(rows, branch_id, company_id)
    if not row:
        return {"status": "error", "message": "No permanent details set for this branch", "status_code": 404}
    return {
        "status": "success",
        "data": row,
        "print_template": row.get("print_template") or "template1",
        # true when the company-specific row was missing and the branch default was used
        "is_fallback": bool(company_id) and row.get("company_id") != company_id,
    }
