"""
Invoice Payment Service
-----------------------
Records payments against invoices and updates payment_status + paid/balance amounts.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

PAYMENT_COLS = (
    "id, invoice_id, payment_date, amount, payment_mode, "
    "reference_no, bank_name, cheque_date, notes, "
    "created_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _sync_payment_status(sb, invoice_id: str) -> None:
    """Recalculate paid_amount, balance_amount, payment_status on invoice_master."""
    total_res = (
        sb.table("invoice_master")
        .select("total_amount")
        .eq("id", invoice_id)
        .single()
        .execute()
    )
    if not total_res.data:
        return

    total = _safe_float(total_res.data.get("total_amount", 0))

    payments_res = (
        sb.table("invoice_payments")
        .select("amount")
        .eq("invoice_id", invoice_id)
        .execute()
    )
    paid = sum(_safe_float(p["amount"]) for p in (payments_res.data or []))
    balance = round(total - paid, 2)

    if paid <= 0:
        pstatus = "UNPAID"
    elif balance <= 0:
        pstatus = "PAID"
    else:
        pstatus = "PARTIAL"

    sb.table("invoice_master").update({
        "paid_amount": round(paid, 2),
        "balance_amount": balance,
        "payment_status": pstatus,
        "updated_at": _now(),
    }).eq("id", invoice_id).execute()


def add_payment(body: dict) -> dict:
    try:
        required = ["invoice_id", "amount", "payment_mode"]
        missing = [f for f in required if not body.get(f)]
        if missing:
            return {"status": "error", "message": f"Missing: {', '.join(missing)}", "status_code": 400}

        if _safe_float(body["amount"]) <= 0:
            return {"status": "error", "message": "amount must be > 0", "status_code": 400}

        sb = get_supabase()

        # Verify invoice exists
        chk = (
            sb.table("invoice_master")
            .select("id, status")
            .eq("id", body["invoice_id"])
            .single()
            .execute()
        )
        if not chk.data:
            return {"status": "error", "message": "Invoice not found", "status_code": 404}
        if chk.data.get("status") == "CANCELLED":
            return {"status": "error", "message": "Cannot record payment on cancelled invoice", "status_code": 400}

        now = _now()
        payload = {**body, "created_at": now, "updated_at": now}
        payload.setdefault("payment_date", datetime.now(timezone.utc).date().isoformat())

        res = sb.table("invoice_payments").insert(payload).execute()
        if not res.data:
            return {"status": "error", "message": "Failed to record payment", "status_code": 500}

        _sync_payment_status(sb, body["invoice_id"])

        return {"status": "success", "data": res.data[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def list_payments(invoice_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_payments")
            .select(PAYMENT_COLS)
            .eq("invoice_id", invoice_id)
            .order("payment_date", desc=True)
            .execute()
        )
        return {"status": "success", "data": res.data or []}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def delete_payment(payment_id: str, invoice_id: str) -> dict:
    try:
        sb = get_supabase()
        res = sb.table("invoice_payments").delete().eq("id", payment_id).execute()
        if not res.data:
            return {"status": "error", "message": "Payment not found", "status_code": 404}
        _sync_payment_status(sb, invoice_id)
        return {"status": "success", "message": "Payment deleted"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
