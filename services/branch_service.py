"""
Branches — plain lookup
=========================
A minimal branch list for UI branch-pickers (e.g. the ledger system's
branch selector) — id, name, code only. For the full branch record with
manager/bill-book/etc. use whatever the bilty reference-data endpoint
already returns; this is deliberately lightweight.
"""
from services.supabase_client import get_supabase


def list_branches(is_active: bool | None = True) -> dict:
    sb = get_supabase()
    q = sb.table("branches").select("id, branch_name, branch_code").order("branch_name")
    if is_active is not None:
        q = q.eq("is_active", is_active)
    return {"status": "success", "data": q.execute().data or []}


def get_branch_name_map(branch_ids: list) -> dict:
    """{branch_id: branch_name} for a set of ids — used to tag rows with a
    readable branch name whenever a list spans more than one branch (the
    owner's 'all branches' view)."""
    ids = [b for b in set(branch_ids) if b]
    if not ids:
        return {}
    sb = get_supabase()
    rows = sb.table("branches").select("id, branch_name").in_("id", ids).execute().data or []
    return {r["id"]: r["branch_name"] for r in rows}
