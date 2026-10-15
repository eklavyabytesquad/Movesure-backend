"""
Bilty Group Service
====================
Master list of groups (migrations/021) — created in Settings, picked from a
dropdown on the manual bilty entry page. Names are stored UPPER-cased and are
unique case-insensitively.

Deleting a group is a soft delete (is_active = false): bilties already tagged
keep their group, but it no longer appears in the entry-page dropdown.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

TABLE = "bilty_groups"
COLS = "id, name, description, is_active, created_by, updated_by, created_at, updated_at"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400) -> dict:
    return {"status": "error", "message": msg, "status_code": code}


def _clean_name(name) -> tuple[str | None, str | None]:
    n = " ".join(str(name or "").split()).upper()
    if not n:
        return None, "name is required"
    if len(n) > 100:
        return None, "name max length is 100"
    return n, None


def _name_taken(sb, name: str, exclude_id: str | None = None) -> bool:
    # names are always stored UPPER-cased, so exact match is case-insensitive
    rows = sb.table(TABLE).select("id").eq("name", name).execute().data or []
    return any(r["id"] != exclude_id for r in rows)


def _bilty_counts(sb, ids: list[str]) -> dict[str, int]:
    counts = {i: 0 for i in ids}
    if not ids:
        return counts
    off = 0
    while True:
        rows = (sb.table("station_bilty_summary").select("group_id").in_("group_id", ids)
                .range(off, off + 999).execute().data or [])
        for r in rows:
            counts[r["group_id"]] = counts.get(r["group_id"], 0) + 1
        if len(rows) < 1000:
            return counts
        off += 1000


def get_group(sb, group_id: str) -> dict | None:
    r = sb.table(TABLE).select(COLS).eq("id", group_id).limit(1).execute()
    return r.data[0] if r.data else None


# ── CRUD ──────────────────────────────────────────────────────────────────────
def list_bilty_groups(q: str | None = None, include_inactive: bool = False, with_counts: bool = True) -> dict:
    sb = get_supabase()
    query = sb.table(TABLE).select(COLS).order("name")
    if not include_inactive:
        query = query.eq("is_active", True)
    if q:
        query = query.ilike("name", f"%{q.strip()}%")
    rows = query.execute().data or []
    if with_counts:
        counts = _bilty_counts(sb, [r["id"] for r in rows])
        for r in rows:
            r["bilty_count"] = counts.get(r["id"], 0)
    return {"status": "success", "data": rows, "count": len(rows)}


def get_bilty_group(group_id: str) -> dict:
    sb = get_supabase()
    g = get_group(sb, group_id)
    if not g:
        return _err("Group not found", 404)
    g["bilty_count"] = _bilty_counts(sb, [group_id])[group_id]
    return {"status": "success", "data": g}


def create_bilty_group(data: dict) -> dict:
    sb = get_supabase()
    name, err = _clean_name(data.get("name"))
    if err:
        return _err(err)
    if _name_taken(sb, name):
        return _err(f"Group '{name}' already exists", 409)
    row = {"name": name, "description": (data.get("description") or "").strip() or None,
           "created_by": data.get("created_by"), "updated_by": data.get("created_by")}
    r = sb.table(TABLE).insert(row).execute()
    return {"status": "success", "message": "Group created", "data": r.data[0]}


def update_bilty_group(group_id: str, data: dict) -> dict:
    """data: any of name, description, is_active, updated_by.
    Renaming also updates the group_ref copy on every tagged bilty."""
    sb = get_supabase()
    g = get_group(sb, group_id)
    if not g:
        return _err("Group not found", 404)
    payload = {}
    if "name" in data:
        name, err = _clean_name(data.get("name"))
        if err:
            return _err(err)
        if name != g["name"]:
            if _name_taken(sb, name, exclude_id=group_id):
                return _err(f"Group '{name}' already exists", 409)
            payload["name"] = name
    if "description" in data:
        payload["description"] = (data.get("description") or "").strip() or None
    if "is_active" in data:
        payload["is_active"] = bool(data.get("is_active"))
    if not payload:
        return _err("Nothing to update")
    payload["updated_at"] = _now()
    if data.get("updated_by"):
        payload["updated_by"] = data["updated_by"]
    sb.table(TABLE).update(payload).eq("id", group_id).execute()
    if "name" in payload:
        sb.table("station_bilty_summary").update({"group_ref": payload["name"]}).eq("group_id", group_id).execute()
    return {"status": "success", "message": "Group updated", "data": get_group(sb, group_id)}


def delete_bilty_group(group_id: str, updated_by: str | None = None) -> dict:
    """Soft delete — hides it from the entry dropdown, tagged bilties keep it."""
    return update_bilty_group(group_id, {"is_active": False, "updated_by": updated_by}) | {"message": "Group deactivated"} \
        if get_group(get_supabase(), group_id) else _err("Group not found", 404)


def list_group_bilties(group_id: str, page: int = 1, page_size: int = 50) -> dict:
    from services.bilty.station_bilty_meta_service import META_COLS, _with_summary
    sb = get_supabase()
    g = get_group(sb, group_id)
    if not g:
        return _err("Group not found", 404)
    page, page_size = max(1, page), min(max(1, page_size), 200)
    start = (page - 1) * page_size
    r = (sb.table("station_bilty_summary").select(META_COLS, count="exact").eq("group_id", group_id)
         .order("created_at", desc=True).range(start, start + page_size - 1).execute())
    return {"status": "success", "group": g, "data": [_with_summary(x) for x in r.data or []],
            "total": r.count or 0, "page": page, "page_size": page_size}
