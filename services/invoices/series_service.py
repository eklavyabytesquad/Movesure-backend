"""
Invoice Series Service
-----------------------
CRUD for invoice_series — numbering configs per tenant.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

SERIES_COLS = (
    "id, tenant_id, series_name, prefix, suffix, financial_year, "
    "digits, current_number, is_default, is_active, created_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_series(tenant_id: Optional[str] = None) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("invoice_series").select(SERIES_COLS)
        if tenant_id:
            q = q.eq("tenant_id", tenant_id)
        res = q.order("created_at").execute()
        return {"status": "success", "data": res.data or []}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def create_series(body: dict) -> dict:
    try:
        required = ["tenant_id", "series_name"]
        missing = [f for f in required if not body.get(f)]
        if missing:
            return {"status": "error", "message": f"Missing: {', '.join(missing)}", "status_code": 400}
        sb = get_supabase()
        now = _now()
        payload = {**body, "created_at": now, "updated_at": now}
        res = sb.table("invoice_series").insert(payload).execute()
        if not res.data:
            return {"status": "error", "message": "Failed to create series", "status_code": 500}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def update_series(series_id: str, body: dict) -> dict:
    try:
        sb = get_supabase()
        payload = {**body, "updated_at": _now()}
        payload.pop("id", None)
        res = (
            sb.table("invoice_series")
            .update(payload)
            .eq("id", series_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Series not found", "status_code": 404}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def delete_series(series_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_series")
            .update({"is_active": False, "updated_at": _now()})
            .eq("id", series_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Series not found", "status_code": 404}
        return {"status": "success", "message": "Series deactivated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
