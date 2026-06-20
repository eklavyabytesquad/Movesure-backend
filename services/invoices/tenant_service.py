"""
Invoice Tenants Service
-----------------------
CRUD for invoice_tenants — the company/firm that issues invoices.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

TENANT_COLS = (
    "id, company_name, trade_name, gstin, pan, "
    "address_line1, address_line2, city, state, state_code, pincode, "
    "mobile, alternate_mobile, email, website, "
    "logo_url, signature_url, "
    "bank_name, bank_account_no, bank_ifsc, bank_branch, upi_id, "
    "invoice_prefix, default_payment_terms, default_tax_type, "
    "default_notes, terms_and_conditions, "
    "consignor_id, is_active, created_by, updated_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── list ─────────────────────────────────────────────────────────────────────

def list_tenants(is_active: Optional[bool] = None) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("invoice_tenants").select(TENANT_COLS)
        if is_active is not None:
            q = q.eq("is_active", is_active)
        res = q.order("created_at", desc=True).execute()
        return {"status": "success", "data": res.data or []}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── get one ───────────────────────────────────────────────────────────────────

def get_tenant(tenant_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_tenants")
            .select(TENANT_COLS)
            .eq("id", tenant_id)
            .single()
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Tenant not found", "status_code": 404}
        return {"status": "success", "data": res.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── create ────────────────────────────────────────────────────────────────────

def create_tenant(body: dict) -> dict:
    try:
        if not body.get("company_name"):
            return {"status": "error", "message": "company_name is required", "status_code": 400}
        sb = get_supabase()
        now = _now()
        payload = {**body, "created_at": now, "updated_at": now}
        res = sb.table("invoice_tenants").insert(payload).execute()
        if not res.data:
            return {"status": "error", "message": "Failed to create tenant", "status_code": 500}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── update ────────────────────────────────────────────────────────────────────

def update_tenant(tenant_id: str, body: dict) -> dict:
    try:
        sb = get_supabase()
        payload = {**body, "updated_at": _now()}
        payload.pop("id", None)
        res = (
            sb.table("invoice_tenants")
            .update(payload)
            .eq("id", tenant_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Tenant not found", "status_code": 404}
        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── delete (soft) ─────────────────────────────────────────────────────────────

def delete_tenant(tenant_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_tenants")
            .update({"is_active": False, "updated_at": _now()})
            .eq("id", tenant_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Tenant not found", "status_code": 404}
        return {"status": "success", "message": "Tenant deactivated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
