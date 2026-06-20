"""
Invoice Inventory Service
--------------------------
CRUD for invoice_inventory — item / product / service catalog.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

ITEM_COLS = (
    "id, tenant_id, item_code, item_name, description, "
    "hsn_sac_code, item_type, unit_of_measurement, "
    "default_rate, default_discount_pct, gst_rate, cess_rate, is_tax_inclusive, "
    "is_active, created_by, updated_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_inventory(tenant_id: Optional[str] = None, is_active: Optional[bool] = None) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("invoice_inventory").select(ITEM_COLS)
        if tenant_id:
            q = q.eq("tenant_id", tenant_id)
        if is_active is not None:
            q = q.eq("is_active", is_active)
        res = q.order("item_name").execute()
        return {"status": "success", "data": res.data or []}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def get_inventory_item(item_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_inventory")
            .select(ITEM_COLS)
            .eq("id", item_id)
            .single()
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Item not found", "status_code": 404}
        return {"status": "success", "data": res.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def create_inventory_item(body: dict) -> dict:
    try:
        if not body.get("item_name"):
            return {"status": "error", "message": "item_name is required", "status_code": 400}
        sb = get_supabase()
        now = _now()
        payload = {**body, "created_at": now, "updated_at": now}
        res = sb.table("invoice_inventory").insert(payload).execute()
        if not res.data:
            return {"status": "error", "message": "Failed to create item", "status_code": 500}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def update_inventory_item(item_id: str, body: dict) -> dict:
    try:
        sb = get_supabase()
        payload = {**body, "updated_at": _now()}
        payload.pop("id", None)
        res = (
            sb.table("invoice_inventory")
            .update(payload)
            .eq("id", item_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Item not found", "status_code": 404}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def delete_inventory_item(item_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_inventory")
            .update({"is_active": False, "updated_at": _now()})
            .eq("id", item_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Item not found", "status_code": 404}
        return {"status": "success", "message": "Item deactivated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
