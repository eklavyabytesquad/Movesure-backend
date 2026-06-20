"""
Invoice Service
---------------
Core CRUD for invoice_master + invoices (line items).

Flow:
  POST /api/invoices/create
    body: { invoice header fields } + line_items: [{ item fields }]
    → auto-generates invoice_no, inserts header + line items, calculates totals

  GET  /api/invoices?tenant_id=&status=&payment_status=&from_date=&to_date=
  GET  /api/invoices/{invoice_id}       — header + line items
  PUT  /api/invoices/{invoice_id}       — update header fields
  POST /api/invoices/{invoice_id}/cancel
  DELETE /api/invoices/{invoice_id}     — soft delete (is_active = false)
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

MASTER_COLS = (
    "id, invoice_no, invoice_series_id, invoice_type, invoice_date, due_date, "
    "tenant_id, seller_name, seller_gstin, seller_pan, seller_address, "
    "seller_state, seller_state_code, "
    "receiver_id, buyer_name, buyer_gstin, buyer_pan, buyer_aadhar_number, "
    "billing_address, shipping_address, buyer_state, buyer_state_code, "
    "transport_name, gr_no, bilty_id, challan_no, po_number, po_date, "
    "pvt_marks, "
    "place_of_supply, supply_type, is_reverse_charge, is_export, export_type, "
    "subtotal, total_discount, taxable_amount, "
    "total_cgst, total_sgst, total_igst, total_cess, total_tax, "
    "round_off, total_amount, amount_in_words, "
    "payment_status, paid_amount, balance_amount, "
    "is_e_invoice, irn, ack_no, ack_date, qr_code_data, e_invoice_status, e_invoice_error, "
    "e_way_bill, original_invoice_id, original_invoice_no, credit_debit_reason, "
    "pdf_url, pdf_bucket, notes, terms_and_conditions, "
    "status, is_active, cancelled_at, cancelled_by, cancel_reason, "
    "created_by, updated_by, created_at, updated_at"
)

LINE_COLS = (
    "id, invoice_id, line_number, inventory_item_id, "
    "item_name, description, hsn_sac_code, "
    "quantity, unit, rate, weight, pvt_marks, "
    "discount_percent, discount_amount, taxable_amount, "
    "gst_rate, cgst_rate, cgst_amount, sgst_rate, sgst_amount, "
    "igst_rate, igst_amount, cess_rate, cess_amount, total_amount, "
    "created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# Fields that must be None (not "") when empty — date and UUID columns
_DATE_FIELDS = {"invoice_date", "due_date", "po_date", "invoice_date", "ack_date", "cancelled_at"}
_UUID_FIELDS = {
    "tenant_id", "receiver_id", "bilty_id", "original_invoice_id",
    "invoice_series_id", "cancelled_by", "created_by", "updated_by",
    "inventory_item_id",
}


def _sanitize(body: dict) -> dict:
    """Convert empty strings to None for date/UUID fields — Postgres rejects '' for those types."""
    out = {}
    for k, v in body.items():
        if isinstance(v, str) and v.strip() == "" and (k in _DATE_FIELDS or k in _UUID_FIELDS):
            out[k] = None
        else:
            out[k] = v
    return out


# ── Auto invoice number ───────────────────────────────────────────────────────

def _next_invoice_no(sb, tenant_id: str, series_id: Optional[str]) -> str:
    """
    Generate next invoice number.
    Uses invoice_series if series_id given, otherwise falls back to
    INV-YYYYMM-XXXX pattern.
    """
    if series_id:
        series_res = (
            sb.table("invoice_series")
            .select("prefix, suffix, financial_year, digits, current_number")
            .eq("id", series_id)
            .single()
            .execute()
        )
        if series_res.data:
            s = series_res.data
            num = s["current_number"]
            digits = s.get("digits", 4)
            prefix = s.get("prefix") or "INV"
            suffix = s.get("suffix") or ""
            fy = s.get("financial_year") or ""
            separator = "/" if fy else "-"
            invoice_no = f"{prefix}{separator}{fy}{separator}{str(num).zfill(digits)}{suffix}" if fy else f"{prefix}-{str(num).zfill(digits)}{suffix}"
            # bump current_number
            sb.table("invoice_series").update({"current_number": num + 1}).eq("id", series_id).execute()
            return invoice_no

    # fallback: INV-YYYYMM-XXXX
    prefix = "INV-" + datetime.now(timezone.utc).strftime("%Y%m") + "-"
    res = (
        sb.table("invoice_master")
        .select("invoice_no")
        .ilike("invoice_no", f"{prefix}%")
        .order("invoice_no", desc=True)
        .limit(1)
        .execute()
    )
    last_seq = 0
    if res.data:
        try:
            last_seq = int(res.data[0]["invoice_no"].split("-")[-1])
        except (ValueError, IndexError):
            last_seq = 0
    return f"{prefix}{str(last_seq + 1).zfill(4)}"


# ── Totals calculation ────────────────────────────────────────────────────────

def _calc_line(item: dict) -> dict:
    """Compute all derived amounts for a single line item."""
    qty = _safe_float(item.get("quantity", 1))
    rate = _safe_float(item.get("rate", 0))
    disc_pct = _safe_float(item.get("discount_percent", 0))

    gross = qty * rate
    disc_amt = round(gross * disc_pct / 100, 2)
    taxable = round(gross - disc_amt, 2)

    gst_rate = _safe_float(item.get("gst_rate", 18))
    cess_rate = _safe_float(item.get("cess_rate", 0))

    # supply_type drives CGST+SGST vs IGST
    supply_type = item.get("_supply_type", "INTRA")
    if supply_type == "INTER":
        igst_rate = gst_rate
        igst_amt = round(taxable * igst_rate / 100, 2)
        cgst_rate = cgst_amt = sgst_rate = sgst_amt = 0.0
    else:
        cgst_rate = sgst_rate = round(gst_rate / 2, 3)
        cgst_amt = sgst_amt = round(taxable * cgst_rate / 100, 2)
        igst_rate = igst_amt = 0.0

    cess_amt = round(taxable * cess_rate / 100, 2)
    total = round(taxable + cgst_amt + sgst_amt + igst_amt + cess_amt, 2)

    return {
        **{k: v for k, v in item.items() if not k.startswith("_")},
        "discount_amount": disc_amt,
        "taxable_amount": taxable,
        "cgst_rate": cgst_rate,
        "cgst_amount": cgst_amt,
        "sgst_rate": sgst_rate,
        "sgst_amount": sgst_amt,
        "igst_rate": igst_rate,
        "igst_amount": igst_amt,
        "cess_rate": cess_rate,
        "cess_amount": cess_amt,
        "total_amount": total,
    }


def _aggregate_totals(lines: list[dict]) -> dict:
    subtotal = sum(_safe_float(l.get("quantity", 1)) * _safe_float(l.get("rate", 0)) for l in lines)
    total_discount = sum(_safe_float(l.get("discount_amount", 0)) for l in lines)
    taxable = sum(_safe_float(l.get("taxable_amount", 0)) for l in lines)
    cgst = sum(_safe_float(l.get("cgst_amount", 0)) for l in lines)
    sgst = sum(_safe_float(l.get("sgst_amount", 0)) for l in lines)
    igst = sum(_safe_float(l.get("igst_amount", 0)) for l in lines)
    cess = sum(_safe_float(l.get("cess_amount", 0)) for l in lines)
    total_tax = cgst + sgst + igst + cess
    total = taxable + total_tax
    round_off = round(round(total) - total, 2)
    return {
        "subtotal": round(subtotal, 2),
        "total_discount": round(total_discount, 2),
        "taxable_amount": round(taxable, 2),
        "total_cgst": round(cgst, 2),
        "total_sgst": round(sgst, 2),
        "total_igst": round(igst, 2),
        "total_cess": round(cess, 2),
        "total_tax": round(total_tax, 2),
        "round_off": round_off,
        "total_amount": round(total + round_off, 2),
        "balance_amount": round(total + round_off, 2),
    }


# ── CREATE ────────────────────────────────────────────────────────────────────

def create_invoice(body: dict) -> dict:
    """
    body must include:
      tenant_id, seller_name, buyer_name, created_by
      line_items: list of item dicts
    Optional: invoice_series_id, receiver_id, all snapshot fields, transport_name, etc.
    """
    try:
        body = _sanitize(body)

        required = ["tenant_id", "seller_name", "buyer_name", "created_by"]
        missing = [f for f in required if not body.get(f)]
        if missing:
            return {"status": "error", "message": f"Missing required fields: {', '.join(missing)}", "status_code": 400}

        line_items = body.pop("line_items", [])
        if not line_items:
            return {"status": "error", "message": "line_items cannot be empty", "status_code": 400}

        sb = get_supabase()
        now = _now()

        supply_type_hint = "INTER" if body.get("supply_type") in ("EXPORT", "SEZ") else (
            "INTER" if body.get("buyer_state_code") and body.get("seller_state_code")
            and body["buyer_state_code"] != body["seller_state_code"] else "INTRA"
        )

        # Calculate line items
        calc_lines = []
        for i, item in enumerate(line_items):
            item["_supply_type"] = supply_type_hint
            item["line_number"] = i + 1
            calc_lines.append(_calc_line(item))

        totals = _aggregate_totals(calc_lines)

        invoice_no = _next_invoice_no(sb, body["tenant_id"], body.get("invoice_series_id"))

        header = {
            **body,
            **totals,
            "invoice_no": invoice_no,
            "paid_amount": 0,
            "payment_status": "UNPAID",
            "status": body.get("status", "DRAFT"),
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }

        master_res = sb.table("invoice_master").insert(header).execute()
        if not master_res.data:
            return {"status": "error", "message": "Failed to create invoice", "status_code": 500}

        invoice_id = master_res.data[0]["id"]

        # Insert line items
        for line in calc_lines:
            line.pop("_supply_type", None)
            line["invoice_id"] = invoice_id
            line["created_at"] = now
            line["updated_at"] = now

        lines_res = sb.table("invoices").insert(calc_lines).execute()

        return {
            "status": "success",
            "data": {
                **master_res.data[0],
                "line_items": lines_res.data or [],
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── LIST ──────────────────────────────────────────────────────────────────────

def list_invoices(
    tenant_id: Optional[str] = None,
    receiver_id: Optional[str] = None,
    status: Optional[str] = None,
    payment_status: Optional[str] = None,
    invoice_type: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    gr_no: Optional[str] = None,
    transport_name: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("invoice_master").select(MASTER_COLS).eq("is_active", True)

        if tenant_id:
            q = q.eq("tenant_id", tenant_id)
        if receiver_id:
            q = q.eq("receiver_id", receiver_id)
        if status:
            q = q.eq("status", status)
        if payment_status:
            q = q.eq("payment_status", payment_status)
        if invoice_type:
            q = q.eq("invoice_type", invoice_type)
        if from_date:
            q = q.gte("invoice_date", from_date)
        if to_date:
            q = q.lte("invoice_date", to_date)
        if gr_no:
            q = q.eq("gr_no", gr_no)
        if transport_name:
            q = q.ilike("transport_name", f"%{transport_name}%")

        offset = (page - 1) * page_size
        res = q.order("invoice_date", desc=True).range(offset, offset + page_size - 1).execute()
        return {"status": "success", "data": res.data or [], "page": page, "page_size": page_size}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── GET ONE ───────────────────────────────────────────────────────────────────

def get_invoice(invoice_id: str) -> dict:
    try:
        sb = get_supabase()
        master_res = (
            sb.table("invoice_master")
            .select(MASTER_COLS)
            .eq("id", invoice_id)
            .single()
            .execute()
        )
        if not master_res.data:
            return {"status": "error", "message": "Invoice not found", "status_code": 404}

        lines_res = (
            sb.table("invoices")
            .select(LINE_COLS)
            .eq("invoice_id", invoice_id)
            .order("line_number")
            .execute()
        )

        return {
            "status": "success",
            "data": {
                **master_res.data,
                "line_items": lines_res.data or [],
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── UPDATE HEADER ─────────────────────────────────────────────────────────────

def update_invoice(invoice_id: str, body: dict) -> dict:
    try:
        body = _sanitize(body)
        sb = get_supabase()

        # Check exists and not cancelled
        chk = (
            sb.table("invoice_master")
            .select("id, status")
            .eq("id", invoice_id)
            .single()
            .execute()
        )
        if not chk.data:
            return {"status": "error", "message": "Invoice not found", "status_code": 404}
        if chk.data.get("status") == "CANCELLED":
            return {"status": "error", "message": "Cannot update a cancelled invoice", "status_code": 400}

        body.pop("id", None)
        body.pop("invoice_no", None)
        body["updated_at"] = _now()

        res = (
            sb.table("invoice_master")
            .update(body)
            .eq("id", invoice_id)
            .execute()
        )
        return {"status": "success", "data": res.data[0] if res.data else {}}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CANCEL ────────────────────────────────────────────────────────────────────

def cancel_invoice(invoice_id: str, cancelled_by: str, cancel_reason: Optional[str] = None) -> dict:
    try:
        sb = get_supabase()
        chk = (
            sb.table("invoice_master")
            .select("id, status")
            .eq("id", invoice_id)
            .single()
            .execute()
        )
        if not chk.data:
            return {"status": "error", "message": "Invoice not found", "status_code": 404}
        if chk.data.get("status") == "CANCELLED":
            return {"status": "error", "message": "Invoice already cancelled", "status_code": 400}

        now = _now()
        res = (
            sb.table("invoice_master")
            .update({
                "status": "CANCELLED",
                "payment_status": "CANCELLED",
                "cancelled_at": now,
                "cancelled_by": cancelled_by,
                "cancel_reason": cancel_reason,
                "updated_at": now,
            })
            .eq("id", invoice_id)
            .execute()
        )
        return {"status": "success", "message": "Invoice cancelled", "data": res.data[0] if res.data else {}}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── DELETE (soft) ─────────────────────────────────────────────────────────────

def delete_invoice(invoice_id: str) -> dict:
    try:
        sb = get_supabase()
        res = (
            sb.table("invoice_master")
            .update({"is_active": False, "updated_at": _now()})
            .eq("id", invoice_id)
            .execute()
        )
        if not res.data:
            return {"status": "error", "message": "Invoice not found", "status_code": 404}
        return {"status": "success", "message": "Invoice deleted"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── LINE ITEMS CRUD ───────────────────────────────────────────────────────────

def update_line_items(invoice_id: str, line_items: list, supply_type_hint: str = "INTRA") -> dict:
    """Replace all line items for an invoice and recalculate totals."""
    try:
        if not line_items:
            return {"status": "error", "message": "line_items cannot be empty", "status_code": 400}

        sb = get_supabase()
        now = _now()

        calc_lines = []
        for i, item in enumerate(line_items):
            item["_supply_type"] = supply_type_hint
            item["line_number"] = i + 1
            calc_lines.append(_calc_line(item))

        # Delete existing lines then re-insert
        sb.table("invoices").delete().eq("invoice_id", invoice_id).execute()

        for line in calc_lines:
            line.pop("_supply_type", None)
            line.pop("id", None)
            line["invoice_id"] = invoice_id
            line["created_at"] = now
            line["updated_at"] = now

        lines_res = sb.table("invoices").insert(calc_lines).execute()

        totals = _aggregate_totals(calc_lines)
        sb.table("invoice_master").update({**totals, "updated_at": now}).eq("id", invoice_id).execute()

        return {"status": "success", "data": lines_res.data or [], "totals": totals}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
