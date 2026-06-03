"""
Staff Service — list, get, create, update, deactivate.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

STAFF_COLS = (
    "id, name, post, mobile_number, license_number, "
    "aadhar_number, image_url, is_active, created_at, updated_at"
)


def _now():
    return datetime.now(timezone.utc).isoformat()


def list_staff(
    post: str = None,
    active_only: bool = True,
    search: str = None,
    page: int = 1,
    page_size: int = 100,
) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("staff").select(STAFF_COLS, count="exact")
        if active_only:
            q = q.eq("is_active", True)
        if post:
            q = q.ilike("post", f"%{post}%")
        if search:
            q = q.or_(f"name.ilike.%{search}%,mobile_number.ilike.%{search}%")
        offset = (page - 1) * page_size
        q = q.order("name").range(offset, offset + page_size - 1)
        resp = q.execute()
        total = resp.count if resp.count is not None else len(resp.data or [])
        return {
            "status": "success",
            "data": {
                "rows": resp.data or [],
                "total": total,
                "page": page,
                "page_size": page_size,
                "has_more": (offset + page_size) < total,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def get_staff_member(staff_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("staff").select(STAFF_COLS).eq("id", staff_id).single().execute()
        if not resp.data:
            return {"status": "error", "message": "Staff not found", "status_code": 404}
        return {"status": "success", "data": resp.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def create_staff(data: dict) -> dict:
    required = ["name", "post"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return {"status": "error", "message": f"Missing fields: {', '.join(missing)}", "status_code": 400}
    try:
        sb = get_supabase()
        now = _now()
        record = {
            "name":           data["name"].strip(),
            "post":           data["post"].strip(),
            "mobile_number":  data.get("mobile_number", ""),
            "license_number": data.get("license_number", ""),
            "aadhar_number":  data.get("aadhar_number", ""),
            "image_url":      data.get("image_url"),
            "is_active":      True,
            "created_at":     now,
            "updated_at":     now,
        }
        resp = sb.table("staff").insert(record).execute()
        return {"status": "success", "data": (resp.data or [{}])[0], "message": "Staff created"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def update_staff(staff_id: str, data: dict) -> dict:
    try:
        sb = get_supabase()
        allowed = {"name", "post", "mobile_number", "license_number", "aadhar_number", "image_url"}
        update = {k: v for k, v in data.items() if k in allowed}
        if not update:
            return {"status": "error", "message": "No updatable fields provided", "status_code": 400}
        update["updated_at"] = _now()
        resp = sb.table("staff").update(update).eq("id", staff_id).execute()
        if not resp.data:
            return {"status": "error", "message": "Staff not found", "status_code": 404}
        return {"status": "success", "data": resp.data[0], "message": "Staff updated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def deactivate_staff(staff_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("staff").update({"is_active": False, "updated_at": _now()}).eq("id", staff_id).execute()
        if not resp.data:
            return {"status": "error", "message": "Staff not found", "status_code": 404}
        return {"status": "success", "message": "Staff deactivated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
