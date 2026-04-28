"""
Pohonch Service
List, get, update, sign pohonch records.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

POHONCH_COLS = (
    "id, pohonch_number, transport_name, transport_gstin, admin_transport_id, "
    "challan_metadata, bilty_metadata, "
    "total_bilties, total_amount, total_kaat, total_pf, total_dd, "
    "total_packages, total_weight, "
    "is_signed, signed_at, signed_by, "
    "is_active, created_by, created_at, updated_at"
)

PAGE_SIZE = 40


def _now():
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────
# LIST  GET /api/pohonch/list
# ─────────────────────────────────────────────────────────────

def list_pohonch(
    transport_name: str | None = None,
    transport_gstin: str | None = None,
    is_signed: bool | None = None,
    is_active: bool | None = True,
    page: int = 1,
    page_size: int = PAGE_SIZE,
    search: str | None = None,
) -> dict:
    sb = get_supabase()

    offset = (page - 1) * page_size

    query = (
        sb.table("pohonch")
        .select(POHONCH_COLS, count="exact")
        .order("created_at", desc=True)
        .range(offset, offset + page_size - 1)
    )

    if is_active is not None:
        query = query.eq("is_active", is_active)
    if is_signed is not None:
        query = query.eq("is_signed", is_signed)
    if transport_gstin:
        query = query.eq("transport_gstin", transport_gstin.strip().upper())
    elif transport_name:
        query = query.ilike("transport_name", f"%{transport_name.strip()}%")
    if search:
        query = query.or_(
            f"pohonch_number.ilike.%{search}%,"
            f"transport_name.ilike.%{search}%"
        )

    res = query.execute()
    rows = res.data or []
    total = res.count or 0

    return {
        "status": "success",
        "data": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 1,
    }


# ─────────────────────────────────────────────────────────────
# GET SINGLE  GET /api/pohonch/{pohonch_id}
# ─────────────────────────────────────────────────────────────

def get_pohonch(pohonch_id: str) -> dict:
    sb = get_supabase()
    res = (
        sb.table("pohonch")
        .select(POHONCH_COLS)
        .eq("id", pohonch_id)
        .single()
        .execute()
    )
    if not res.data:
        return {"status": "error", "message": "Pohonch not found", "status_code": 404}
    return {"status": "success", "data": res.data}


# ─────────────────────────────────────────────────────────────
# GET BY NUMBER  GET /api/pohonch/number/{pohonch_number}
# ─────────────────────────────────────────────────────────────

def get_pohonch_by_number(pohonch_number: str) -> dict:
    sb = get_supabase()
    res = (
        sb.table("pohonch")
        .select(POHONCH_COLS)
        .eq("pohonch_number", pohonch_number.strip().upper())
        .single()
        .execute()
    )
    if not res.data:
        return {"status": "error", "message": "Pohonch not found", "status_code": 404}
    return {"status": "success", "data": res.data}


# ─────────────────────────────────────────────────────────────
# UPDATE  PUT /api/pohonch/{pohonch_id}
# ─────────────────────────────────────────────────────────────

_UPDATABLE_FIELDS = {
    "transport_name", "transport_gstin", "admin_transport_id",
    "challan_metadata", "bilty_metadata",
    "total_bilties", "total_amount", "total_kaat", "total_pf", "total_dd",
    "total_packages", "total_weight",
}


def update_pohonch(pohonch_id: str, data: dict, user_id: str | None = None) -> dict:
    sb = get_supabase()

    payload = {k: v for k, v in data.items() if k in _UPDATABLE_FIELDS}
    if not payload:
        return {"status": "error", "message": "No updatable fields provided", "status_code": 400}

    payload["updated_at"] = _now()
    if user_id:
        payload["updated_by"] = user_id

    res = (
        sb.table("pohonch")
        .update(payload)
        .eq("id", pohonch_id)
        .execute()
    )
    updated = (res.data or [{}])[0]
    if not updated:
        return {"status": "error", "message": "Pohonch not found or not updated", "status_code": 404}
    return {"status": "success", "data": updated}


# ─────────────────────────────────────────────────────────────
# SIGN  POST /api/pohonch/{pohonch_id}/sign
# ─────────────────────────────────────────────────────────────

def sign_pohonch(pohonch_id: str, user_id: str) -> dict:
    sb = get_supabase()

    # Check exists
    check = sb.table("pohonch").select("id, is_signed").eq("id", pohonch_id).single().execute()
    if not check.data:
        return {"status": "error", "message": "Pohonch not found", "status_code": 404}
    if check.data.get("is_signed"):
        return {"status": "error", "message": "Pohonch already signed", "status_code": 409}

    now = _now()
    res = (
        sb.table("pohonch")
        .update({
            "is_signed": True,
            "signed_at": now,
            "signed_by": user_id,
            "updated_at": now,
            "updated_by": user_id,
        })
        .eq("id", pohonch_id)
        .execute()
    )
    updated = (res.data or [{}])[0]
    return {"status": "success", "data": updated}


# ─────────────────────────────────────────────────────────────
# UNSIGN  POST /api/pohonch/{pohonch_id}/unsign
# ─────────────────────────────────────────────────────────────

def unsign_pohonch(pohonch_id: str, user_id: str) -> dict:
    sb = get_supabase()

    check = sb.table("pohonch").select("id, is_signed").eq("id", pohonch_id).single().execute()
    if not check.data:
        return {"status": "error", "message": "Pohonch not found", "status_code": 404}
    if not check.data.get("is_signed"):
        return {"status": "error", "message": "Pohonch is not signed", "status_code": 409}

    now = _now()
    res = (
        sb.table("pohonch")
        .update({
            "is_signed": False,
            "signed_at": None,
            "signed_by": None,
            "updated_at": now,
            "updated_by": user_id,
        })
        .eq("id", pohonch_id)
        .execute()
    )
    updated = (res.data or [{}])[0]
    return {"status": "success", "data": updated}


# ─────────────────────────────────────────────────────────────
# HARD-DELETE  DELETE /api/pohonch/{pohonch_id}
# Permanently removes the row so pohonch_number can be reused.
# ─────────────────────────────────────────────────────────────

def delete_pohonch(pohonch_id: str, user_id: str | None = None) -> dict:
    sb = get_supabase()

    # Verify it exists first
    check = sb.table("pohonch").select("id, pohonch_number").eq("id", pohonch_id).execute()
    if not check.data:
        return {"status": "error", "message": "Pohonch not found", "status_code": 404}

    pohonch_number = check.data[0]["pohonch_number"]

    sb.table("pohonch").delete().eq("id", pohonch_id).execute()

    return {"status": "success", "message": f"Pohonch {pohonch_number} deleted permanently", "pohonch_number": pohonch_number}
