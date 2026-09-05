"""
Ledger Audit Log
================
Records every create/update/deactivate/reactivate/cancel across the
ledger accounting system (groups, ledgers, vouchers, bill references).
write_audit_log() is called by every other service in this package —
it never raises, so a logging hiccup can never block the real operation.
"""
from services.supabase_client import get_supabase

PAGE_SIZE = 50


def write_audit_log(sb, entity_type: str, entity_id: str, action: str,
                     old_data, new_data, changed_by: str | None = None) -> None:
    try:
        sb.table("ledger_audit_log").insert({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "action": action,
            "old_data": old_data,
            "new_data": new_data,
            "changed_by": changed_by,
        }).execute()
    except Exception as e:
        print(f"ledger_audit_log write failed ({entity_type}/{entity_id}/{action}): {e}")


def list_audit_log(entity_type: str | None = None, entity_id: str | None = None,
                    page: int = 1, page_size: int = PAGE_SIZE) -> dict:
    sb = get_supabase()
    q = sb.table("ledger_audit_log").select("*", count="exact").order("changed_at", desc=True)
    if entity_type:
        q = q.eq("entity_type", entity_type)
    if entity_id:
        q = q.eq("entity_id", entity_id)

    offset = (page - 1) * page_size
    q = q.range(offset, offset + page_size - 1)
    res = q.execute()
    total = res.count if res.count is not None else len(res.data or [])

    return {
        "status": "success",
        "data": {
            "rows": res.data or [],
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_more": (offset + page_size) < total,
        },
    }
