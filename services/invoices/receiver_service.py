"""
Invoice Receivers Service
--------------------------
CRUD for invoice_receivers — buyer / customer master.
Includes optional gstin, aadhar_number fields.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

RECEIVER_COLS = (
    "id, tenant_id, company_name, trade_name, contact_person, "
    "gstin, pan, aadhar_number, mobile, email, "
    "billing_address_line1, billing_address_line2, billing_city, "
    "billing_state, billing_state_code, billing_pincode, "
    "shipping_address_line1, shipping_address_line2, shipping_city, "
    "shipping_state, shipping_state_code, shipping_pincode, "
    "credit_limit, credit_days, outstanding_amount, "
    "consignee_id, is_active, created_by, updated_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_receivers(tenant_id: Optional[str] = None, is_active: Optional[bool] = None) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("invoice_receivers").select(RECEIVER_COLS)
        if tenant_id:
            q = q.eq("tenant_id", tenant_id)
        if is_active is not None:
            q = q.eq("is_active", is_active)
        res = q.order("company_name").execute()
        return {"status": "success", "data": res.data or []}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def get_receiver(receiver_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_receivers")
            .select(RECEIVER_COLS)
            .eq("id", receiver_id)
            .single()
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Receiver not found", "status_code": 404}
        return {"status": "success", "data": res.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def create_receiver(body: dict) -> dict:
    try:
        if not body.get("company_name"):
            return {"status": "error", "message": "company_name is required", "status_code": 400}
        sb = get_supabase()
        now = _now()
        payload = {**body, "created_at": now, "updated_at": now}
        res = sb.table("invoice_receivers").insert(payload).execute()
        if not res.data:
            return {"status": "error", "message": "Failed to create receiver", "status_code": 500}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def update_receiver(receiver_id: str, body: dict) -> dict:
    try:
        sb = get_supabase()
        payload = {**body, "updated_at": _now()}
        payload.pop("id", None)
        res = (
            sb.table("invoice_receivers")
            .update(payload)
            .eq("id", receiver_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Receiver not found", "status_code": 404}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def delete_receiver(receiver_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_receivers")
            .update({"is_active": False, "updated_at": _now()})
            .eq("id", receiver_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Receiver not found", "status_code": 404}
        return {"status": "success", "message": "Receiver deactivated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
