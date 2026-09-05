"""
Ledger Groups — chart of accounts (Tally-style)
================================================
A group is a category like "Sundry Debtors" or "Bank Accounts". Groups can
nest under a parent (parent_group_id), exactly like Tally's group tree.
The default groups seeded by migration 005 are marked is_system=true and
can never be renamed, re-parented, or deleted — only user-created groups can be.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.audit_log_service import write_audit_log

GROUP_COLS = (
    "id, name, parent_group_id, nature, is_direct, is_system, is_active, "
    "created_by, created_at, updated_at"
)
VALID_NATURES = ("asset", "liability", "income", "expense", "equity")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_groups(is_active: bool | None = True) -> dict:
    sb = get_supabase()
    q = sb.table("ledger_groups").select(GROUP_COLS).order("name")
    if is_active is not None:
        q = q.eq("is_active", is_active)
    return {"status": "success", "data": q.execute().data or []}


def list_groups_tree(is_active: bool | None = True) -> dict:
    """Same groups, nested under `children` by parent_group_id — for a tree-view UI."""
    rows = list_groups(is_active)["data"]
    by_id = {r["id"]: {**r, "children": []} for r in rows}
    roots = []
    for r in rows:
        node = by_id[r["id"]]
        parent_id = r.get("parent_group_id")
        if parent_id and parent_id in by_id:
            by_id[parent_id]["children"].append(node)
        else:
            roots.append(node)
    return {"status": "success", "data": roots}


def get_group(group_id: str) -> dict:
    sb = get_supabase()
    res = sb.table("ledger_groups").select(GROUP_COLS).eq("id", group_id).execute()
    if not res.data:
        return {"status": "error", "message": "Group not found", "status_code": 404}
    return {"status": "success", "data": res.data[0]}


def create_group(data: dict, user_id: str | None = None) -> dict:
    name = (data.get("name") or "").strip()
    nature = data.get("nature")
    if not name:
        return {"status": "error", "message": "name is required", "status_code": 400}
    if nature not in VALID_NATURES:
        return {"status": "error", "message": f"nature must be one of {list(VALID_NATURES)}", "status_code": 400}

    sb = get_supabase()
    parent_group_id = data.get("parent_group_id")
    if parent_group_id and not sb.table("ledger_groups").select("id").eq("id", parent_group_id).execute().data:
        return {"status": "error", "message": "parent_group_id not found", "status_code": 404}

    row = {
        "name": name,
        "parent_group_id": parent_group_id,
        "nature": nature,
        "is_direct": bool(data.get("is_direct", False)),
        "is_system": False,
        "created_by": user_id,
    }
    ins = sb.table("ledger_groups").insert(row).execute()
    if not ins.data:
        return {"status": "error", "message": "Could not create group — that name may already exist under this parent", "status_code": 409}

    created = ins.data[0]
    write_audit_log(sb, "ledger_group", created["id"], "create", None, created, user_id)
    return {"status": "success", "message": f"Group '{name}' created", "data": created}


def update_group(group_id: str, data: dict, user_id: str | None = None) -> dict:
    sb = get_supabase()
    existing_res = sb.table("ledger_groups").select(GROUP_COLS).eq("id", group_id).execute().data
    if not existing_res:
        return {"status": "error", "message": "Group not found", "status_code": 404}
    existing = existing_res[0]

    if existing["is_system"] and any(k in data for k in ("name", "nature", "parent_group_id")):
        return {"status": "error", "message": "This is a default system group — its name/nature/parent cannot be changed", "status_code": 400}

    allowed = {"name", "parent_group_id", "nature", "is_direct"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        return {"status": "error", "message": "No updatable fields provided", "status_code": 400}
    if "nature" in payload and payload["nature"] not in VALID_NATURES:
        return {"status": "error", "message": f"nature must be one of {list(VALID_NATURES)}", "status_code": 400}
    payload["updated_at"] = _now()

    res = sb.table("ledger_groups").update(payload).eq("id", group_id).execute()
    if not res.data:
        return {"status": "error", "message": "Update failed — that name may already exist under this parent", "status_code": 409}

    updated = res.data[0]
    write_audit_log(sb, "ledger_group", group_id, "update", existing, updated, user_id)
    return {"status": "success", "data": updated}


def set_group_status(group_id: str, is_active: bool, user_id: str | None = None) -> dict:
    sb = get_supabase()
    existing_res = sb.table("ledger_groups").select(GROUP_COLS).eq("id", group_id).execute().data
    if not existing_res:
        return {"status": "error", "message": "Group not found", "status_code": 404}
    existing = existing_res[0]

    if not is_active:
        if existing["is_system"]:
            return {"status": "error", "message": "Default system groups cannot be deactivated", "status_code": 400}
        if sb.table("ledger_groups").select("id").eq("parent_group_id", group_id).eq("is_active", True).execute().data:
            return {"status": "error", "message": "This group has active sub-groups under it — move or deactivate them first", "status_code": 400}
        if sb.table("ledgers").select("id").eq("group_id", group_id).eq("is_active", True).execute().data:
            return {"status": "error", "message": "This group has active ledgers under it — move or deactivate them first", "status_code": 400}

    sb.table("ledger_groups").update({"is_active": is_active, "updated_at": _now()}).eq("id", group_id).execute()
    action = "reactivate" if is_active else "deactivate"
    write_audit_log(sb, "ledger_group", group_id, action, existing, {"is_active": is_active}, user_id)
    verb = "reactivated" if is_active else "deactivated"
    return {"status": "success", "message": f"Group '{existing['name']}' {verb}"}
