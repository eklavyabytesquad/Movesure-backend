"""
Crossing Bill Service
=====================
A crossing bill consolidates multiple pohonch (crossing challans) for one
transport into a single settlement document.

Flow:
  1. GET /api/crossing-bill/pohonch?transport_gstin=&from_date=&to_date=
     → returns eligible (unbilled) pohonch for that transport + date range

  2. POST /api/crossing-bill/create
     → body: { transport_id, transport_gstin, transport_name, from_date, to_date,
               pohonch_numbers: [...], created_by }
     → creates bill, snapshots pohonch totals, links pohonch rows

  3. POST /api/crossing-bill/{bill_id}/transaction
     → records a payment (received from transport or paid to transport)

  4. PUT  /api/crossing-bill/{bill_id}/status
     → updates bill status (draft → sent → partial_paid → paid)

  5. GET  /api/crossing-bill           - list all bills (filterable)
  6. GET  /api/crossing-bill/{bill_id} - get one bill with pohonch snapshot
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from services.supabase_client import get_supabase

PAGE_SIZE = 1000

BILL_COLS = (
    "id, bill_no, transport_id, transport_gstin, transport_name, "
    "from_date, to_date, bill_month, bill_year, status, "
    "total_pohonch, total_bilties, total_kaat, total_pf, total_dd, total_amount, "
    "total_paid_kaat, total_paid_to_transport, "
    "balance_on_us, balance_on_transport, "
    "pohonch_data, transactions, bill_url, "
    "created_by, updated_by, created_at, updated_at, is_active"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _make_bill_no(sb) -> str:
    """Auto-generate bill_no as CB-YYYYMM-XXXX."""
    prefix = "CB-" + datetime.now(timezone.utc).strftime("%Y%m") + "-"
    res = (
        sb.table("crossing_bill")
        .select("bill_no")
        .ilike("bill_no", f"{prefix}%")
        .order("bill_no", desc=True)
        .limit(1)
        .execute()
    )
    last_seq = 0
    if res.data:
        try:
            last_seq = int(res.data[0]["bill_no"].split("-")[-1])
        except (ValueError, IndexError):
            last_seq = 0
    return f"{prefix}{str(last_seq + 1).zfill(4)}"


# ── 1. GET eligible unbilled pohonch ─────────────────────────────────────────

def get_unbilled_pohonch(
    transport_gstin: str = None,
    transport_name: str = None,
    transport_id: str = None,
    from_date: str = None,
    to_date: str = None,
) -> dict:
    """
    Return active pohonch not yet linked to any crossing bill.
    Filter by transport (gstin or name or transport_admin id) and optional date range.
    """
    try:
        sb = get_supabase()
        q = (
            sb.table("pohonch")
            .select(
                "id, pohonch_number, transport_name, transport_gstin, "
                "admin_transport_id, total_bilties, total_kaat, total_pf, "
                "total_dd, total_amount, total_weight, total_packages, "
                "is_signed, challan_metadata, created_at"
            )
            .eq("is_active", True)
            .is_("crossing_bill_id", "null")
        )

        if transport_gstin:
            q = q.ilike("transport_gstin", f"%{transport_gstin.strip()}%")
        elif transport_name:
            q = q.ilike("transport_name", f"%{transport_name.strip()}%")
        elif transport_id:
            q = q.eq("admin_transport_id", transport_id)

        if from_date:
            q = q.gte("created_at", from_date)
        if to_date:
            # inclusive — add 1 day
            from datetime import date, timedelta
            end = str(date.fromisoformat(to_date) + timedelta(days=1))
            q = q.lt("created_at", end)

        q = q.order("created_at")
        resp = q.execute()
        rows = resp.data or []

        # Aggregate preview totals
        preview = {
            "total_pohonch":  len(rows),
            "total_bilties":  sum(r.get("total_bilties", 0) for r in rows),
            "total_kaat":     round(sum(_safe_float(r.get("total_kaat")) for r in rows), 2),
            "total_pf":       round(sum(_safe_float(r.get("total_pf"))   for r in rows), 2),
            "total_dd":       round(sum(_safe_float(r.get("total_dd"))   for r in rows), 2),
            "total_amount":   round(sum(_safe_float(r.get("total_amount")) for r in rows), 2),
        }

        return {
            "status": "success",
            "data": {"pohonch": rows, "preview_totals": preview},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── 2. CREATE BILL ────────────────────────────────────────────────────────────

def create_crossing_bill(data: dict) -> dict:
    """
    Create a crossing bill from selected pohonch numbers.

    Required:
      transport_name   str
      from_date        str  YYYY-MM-DD
      to_date          str  YYYY-MM-DD
      pohonch_numbers  list[str]   e.g. ["NIE0001","NIE0002"]

    Optional:
      transport_id     uuid (transport_admin.transport_id)
      transport_gstin  str
      bill_month       int  (defaults to from_date month)
      bill_year        int
      created_by       uuid
    """
    try:
        sb = get_supabase()

        # Validate required
        required = ["transport_name", "from_date", "to_date", "pohonch_numbers"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return {"status": "error",
                    "message": f"Missing required fields: {', '.join(missing)}",
                    "status_code": 400}

        pohonch_numbers: list[str] = data["pohonch_numbers"]
        if not pohonch_numbers:
            return {"status": "error",
                    "message": "pohonch_numbers cannot be empty",
                    "status_code": 400}

        # Fetch the selected pohonch
        resp = (
            sb.table("pohonch")
            .select(
                "id, pohonch_number, transport_name, transport_gstin, "
                "total_bilties, total_kaat, total_pf, total_dd, total_amount, "
                "total_weight, total_packages, is_signed, challan_metadata, "
                "crossing_bill_id, is_active"
            )
            .in_("pohonch_number", pohonch_numbers)
            .execute()
        )
        pohonch_rows = resp.data or []

        # Validate
        not_found = set(pohonch_numbers) - {r["pohonch_number"] for r in pohonch_rows}
        already_billed = [r["pohonch_number"] for r in pohonch_rows if r.get("crossing_bill_id")]

        if not_found:
            return {"status": "error",
                    "message": f"Pohonch not found: {sorted(not_found)}",
                    "status_code": 404}
        if already_billed:
            return {"status": "error",
                    "message": f"Already in another bill: {already_billed}. Remove them from that bill first.",
                    "status_code": 409}

        # Aggregate totals
        total_kaat   = round(sum(_safe_float(r.get("total_kaat"))   for r in pohonch_rows), 2)
        total_pf     = round(sum(_safe_float(r.get("total_pf"))     for r in pohonch_rows), 2)
        total_dd     = round(sum(_safe_float(r.get("total_dd"))     for r in pohonch_rows), 2)
        total_amount = round(sum(_safe_float(r.get("total_amount")) for r in pohonch_rows), 2)
        total_bilties = sum(int(r.get("total_bilties") or 0) for r in pohonch_rows)

        # Build pohonch_data snapshot
        pohonch_data = [
            {
                "pohonch_id":      r["id"],
                "pohonch_number":  r["pohonch_number"],
                "transport_name":  r.get("transport_name", ""),
                "transport_gstin": r.get("transport_gstin", ""),
                "total_bilties":   r.get("total_bilties", 0),
                "total_kaat":      _safe_float(r.get("total_kaat")),
                "total_pf":        _safe_float(r.get("total_pf")),
                "total_dd":        _safe_float(r.get("total_dd")),
                "total_amount":    _safe_float(r.get("total_amount")),
                "total_weight":    _safe_float(r.get("total_weight")),
                "total_packages":  _safe_float(r.get("total_packages")),
                "is_signed":       bool(r.get("is_signed")),
                "challan_nos":     r.get("challan_metadata") or [],
            }
            for r in pohonch_rows
        ]

        # Derive month/year from from_date
        from datetime import date
        fd = date.fromisoformat(data["from_date"])
        bill_month = data.get("bill_month") or fd.month
        bill_year  = data.get("bill_year")  or fd.year

        bill_no = _make_bill_no(sb)

        record = {
            "bill_no":          bill_no,
            "transport_id":     data.get("transport_id"),
            "transport_gstin":  data.get("transport_gstin", ""),
            "transport_name":   data["transport_name"].strip(),
            "from_date":        data["from_date"],
            "to_date":          data["to_date"],
            "bill_month":       bill_month,
            "bill_year":        bill_year,
            "status":           "draft",
            "total_pohonch":    len(pohonch_rows),
            "total_bilties":    total_bilties,
            "total_kaat":       total_kaat,
            "total_pf":         total_pf,
            "total_dd":         total_dd,
            "total_amount":     total_amount,
            "total_paid_kaat":  0,
            "total_paid_to_transport": 0,
            "pohonch_data":     pohonch_data,
            "transactions":     [],
            "created_by":       data.get("created_by"),
            "is_active":        True,
        }

        ins = sb.table("crossing_bill").insert(record).execute()
        bill = (ins.data or [{}])[0]
        if not bill:
            return {"status": "error", "message": "Insert failed", "status_code": 500}

        bill_id = bill["id"]

        # Link pohonch rows to this bill
        pnos_to_link = [r["pohonch_number"] for r in pohonch_rows]
        sb.table("pohonch").update({"crossing_bill_id": bill_id}).in_("pohonch_number", pnos_to_link).execute()

        return {
            "status":   "success",
            "message":  f"Crossing bill {bill_no} created with {len(pohonch_rows)} pohonch",
            "bill_no":  bill_no,
            "data":     bill,
        }

    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── 3. ADD TRANSACTION ────────────────────────────────────────────────────────

def add_transaction(bill_id: str, txn: dict) -> dict:
    """
    Record a payment transaction on a crossing bill.

    txn fields:
      amount       float   required
      type         str     "received_from_transport" | "paid_to_transport"
      date         str     YYYY-MM-DD
      mode         str     "cash" | "online"
      note         str     optional
      recorded_by  uuid    optional
    """
    try:
        sb = get_supabase()

        bill_resp = (
            sb.table("crossing_bill")
            .select("id, status, transactions, total_kaat, total_pf, "
                    "total_paid_kaat, total_paid_to_transport")
            .eq("id", bill_id)
            .single()
            .execute()
        )
        bill = bill_resp.data
        if not bill:
            return {"status": "error", "message": "Bill not found", "status_code": 404}
        if bill["status"] == "cancelled":
            return {"status": "error", "message": "Cannot add transaction to a cancelled bill", "status_code": 400}

        amount = _safe_float(txn.get("amount"))
        if amount <= 0:
            return {"status": "error", "message": "amount must be > 0", "status_code": 400}

        txn_type = txn.get("type", "")
        if txn_type not in ("received_from_transport", "paid_to_transport"):
            return {"status": "error",
                    "message": "type must be 'received_from_transport' or 'paid_to_transport'",
                    "status_code": 400}

        new_txn = {
            "id":          str(uuid.uuid4()),
            "date":        txn.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
            "amount":      amount,
            "type":        txn_type,
            "mode":        txn.get("mode", "cash"),
            "note":        txn.get("note", ""),
            "recorded_by": txn.get("recorded_by"),
            "recorded_at": _now(),
        }

        existing_txns = bill.get("transactions") or []
        existing_txns.append(new_txn)

        # Recalculate running totals
        paid_kaat     = round(sum(_safe_float(t["amount"]) for t in existing_txns if t["type"] == "received_from_transport"), 2)
        paid_to_trans = round(sum(_safe_float(t["amount"]) for t in existing_txns if t["type"] == "paid_to_transport"), 2)

        # Auto-update status
        total_kaat = _safe_float(bill.get("total_kaat"))
        total_pf   = _safe_float(bill.get("total_pf"))
        new_status = bill["status"]
        if new_status not in ("draft", "sent"):
            new_status = bill["status"]
        if paid_kaat >= total_kaat and paid_to_trans >= total_pf:
            new_status = "paid"
        elif paid_kaat > 0 or paid_to_trans > 0:
            new_status = "partial_paid"

        resp = (
            sb.table("crossing_bill")
            .update({
                "transactions":         existing_txns,
                "total_paid_kaat":      paid_kaat,
                "total_paid_to_transport": paid_to_trans,
                "status":              new_status,
                "updated_by":          txn.get("recorded_by"),
                "updated_at":          _now(),
            })
            .eq("id", bill_id)
            .execute()
        )

        return {
            "status":  "success",
            "message": "Transaction recorded",
            "data":    (resp.data or [{}])[0],
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── 4. UPDATE STATUS / BILL URL ───────────────────────────────────────────────

def update_bill(bill_id: str, data: dict) -> dict:
    """Update status, bill_url, or other editable fields."""
    try:
        sb = get_supabase()

        bill = sb.table("crossing_bill").select("id, status").eq("id", bill_id).single().execute().data
        if not bill:
            return {"status": "error", "message": "Bill not found", "status_code": 404}

        allowed = {"status", "bill_url", "updated_by"}
        if "status" in data and data["status"] not in ("draft","sent","partial_paid","paid","cancelled"):
            return {"status": "error", "message": "Invalid status", "status_code": 400}

        update = {k: v for k, v in data.items() if k in allowed}
        update["updated_at"] = _now()

        resp = sb.table("crossing_bill").update(update).eq("id", bill_id).execute()
        return {"status": "success", "data": (resp.data or [{}])[0], "message": "Bill updated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── 5. LIST BILLS ─────────────────────────────────────────────────────────────

def list_crossing_bills(
    transport_gstin: str = None,
    transport_id: str = None,
    status: str = None,
    bill_month: int = None,
    bill_year: int = None,
    page: int = 1,
    page_size: int = 40,
) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("crossing_bill").select(
            BILL_COLS, count="exact"
        ).eq("is_active", True)

        if transport_gstin:
            q = q.ilike("transport_gstin", f"%{transport_gstin.strip()}%")
        if transport_id:
            q = q.eq("transport_id", transport_id)
        if status:
            q = q.eq("status", status)
        if bill_month:
            q = q.eq("bill_month", bill_month)
        if bill_year:
            q = q.eq("bill_year", bill_year)

        offset = (page - 1) * page_size
        q = q.order("created_at", desc=True).range(offset, offset + page_size - 1)
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


# ── 6. GET ONE BILL ───────────────────────────────────────────────────────────

def get_crossing_bill(bill_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("crossing_bill").select(BILL_COLS).eq("id", bill_id).single().execute()
        bill = resp.data
        if not bill:
            return {"status": "error", "message": "Bill not found", "status_code": 404}
        return {"status": "success", "data": bill}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── 7. REMOVE POHONCH FROM BILL (before finalization) ────────────────────────

def remove_pohonch_from_bill(bill_id: str, pohonch_number: str, updated_by: str = None) -> dict:
    """Unlink one pohonch from a draft bill and recalculate totals."""
    try:
        sb = get_supabase()

        bill = sb.table("crossing_bill").select(BILL_COLS).eq("id", bill_id).single().execute().data
        if not bill:
            return {"status": "error", "message": "Bill not found", "status_code": 404}
        if bill["status"] not in ("draft",):
            return {"status": "error", "message": "Only draft bills can be modified", "status_code": 400}

        existing = bill.get("pohonch_data") or []
        remaining = [p for p in existing if p.get("pohonch_number") != pohonch_number]
        removed   = [p for p in existing if p.get("pohonch_number") == pohonch_number]

        if not removed:
            return {"status": "error", "message": f"Pohonch {pohonch_number} not in this bill", "status_code": 404}

        # Recalculate totals from remaining snapshot
        total_kaat   = round(sum(_safe_float(p.get("total_kaat"))   for p in remaining), 2)
        total_pf     = round(sum(_safe_float(p.get("total_pf"))     for p in remaining), 2)
        total_dd     = round(sum(_safe_float(p.get("total_dd"))     for p in remaining), 2)
        total_amount = round(sum(_safe_float(p.get("total_amount")) for p in remaining), 2)
        total_bilties = sum(int(p.get("total_bilties") or 0) for p in remaining)

        sb.table("crossing_bill").update({
            "pohonch_data":  remaining,
            "total_pohonch": len(remaining),
            "total_bilties": total_bilties,
            "total_kaat":    total_kaat,
            "total_pf":      total_pf,
            "total_dd":      total_dd,
            "total_amount":  total_amount,
            "updated_by":    updated_by,
            "updated_at":    _now(),
        }).eq("id", bill_id).execute()

        # Unlink pohonch row
        sb.table("pohonch").update({"crossing_bill_id": None}).eq("pohonch_number", pohonch_number).execute()

        return {"status": "success", "message": f"Pohonch {pohonch_number} removed from bill", "remaining_pohonch": len(remaining)}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── 8. CANCEL BILL ────────────────────────────────────────────────────────────

def cancel_crossing_bill(bill_id: str, updated_by: str = None) -> dict:
    """Cancel a bill and unlink all its pohonch."""
    try:
        sb = get_supabase()

        bill = sb.table("crossing_bill").select("id, status, pohonch_data").eq("id", bill_id).single().execute().data
        if not bill:
            return {"status": "error", "message": "Bill not found", "status_code": 404}
        if bill["status"] == "paid":
            return {"status": "error", "message": "Cannot cancel a fully paid bill", "status_code": 400}

        pnos = [p["pohonch_number"] for p in (bill.get("pohonch_data") or []) if p.get("pohonch_number")]
        if pnos:
            sb.table("pohonch").update({"crossing_bill_id": None}).in_("pohonch_number", pnos).execute()

        sb.table("crossing_bill").update({
            "status": "cancelled", "is_active": False,
            "updated_by": updated_by, "updated_at": _now(),
        }).eq("id", bill_id).execute()

        return {"status": "success", "message": "Bill cancelled and pohonch unlinked"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
