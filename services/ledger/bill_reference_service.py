"""
Ledger Bill References — bill-by-bill tracking
================================================
Tally's "Maintain balances bill-by-bill": instead of a debtor/creditor
ledger just having one lump balance, each bill/invoice raised against it
is tracked separately, so you can see exactly which bill is still unpaid.

recompute_bill_balance() is the single source of truth for a bill's
outstanding amount — it always recalculates from live voucher_entries
(only counting ones on an active, non-cancelled voucher) rather than
patching a running total. That means cancelling a voucher automatically
restores whatever balance it had settled, with no separate "reverse"
bookkeeping anywhere.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.audit_log_service import write_audit_log

BILL_COLS = (
    "id, ledger_id, reference_no, reference_date, due_date, bill_amount, balance_amount, "
    "entry_type, source_table, source_id, metadata, is_settled, created_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_bills(ledger_id: str, is_settled: bool | None = None) -> dict:
    sb = get_supabase()
    q = sb.table("ledger_bill_references").select(BILL_COLS).eq("ledger_id", ledger_id).order("reference_date")
    if is_settled is not None:
        q = q.eq("is_settled", is_settled)
    return {"status": "success", "data": q.execute().data or []}


def get_bill(bill_id: str) -> dict:
    sb = get_supabase()
    res = sb.table("ledger_bill_references").select(BILL_COLS).eq("id", bill_id).execute()
    if not res.data:
        return {"status": "error", "message": "Bill not found", "status_code": 404}
    return {"status": "success", "data": res.data[0]}


def create_bill(data: dict, user_id: str | None = None, sb=None) -> dict:
    """Create one bill reference. Called directly (manual "add a bill to
    this ledger") or internally by voucher_service when a voucher entry
    raises a brand-new bill (bill_allocation_type='new_ref')."""
    ledger_id = data.get("ledger_id")
    reference_no = (data.get("reference_no") or "").strip()
    bill_amount = data.get("bill_amount")
    entry_type = data.get("entry_type")
    created_by = user_id or data.get("created_by")

    if not ledger_id or not reference_no or bill_amount is None or entry_type not in ("dr", "cr"):
        return {"status": "error", "message": "ledger_id, reference_no, bill_amount and entry_type ('dr'/'cr') are required", "status_code": 400}
    if not created_by:
        return {"status": "error", "message": "created_by is required", "status_code": 400}

    sb = sb or get_supabase()
    row = {
        "ledger_id": ledger_id,
        "reference_no": reference_no,
        "due_date": data.get("due_date"),
        "bill_amount": bill_amount,
        "balance_amount": bill_amount,
        "entry_type": entry_type,
        "source_table": data.get("source_table"),
        "source_id": data.get("source_id"),
        "metadata": data.get("metadata"),
        "created_by": created_by,
    }
    if data.get("reference_date"):
        row["reference_date"] = data["reference_date"]  # else let the DB default (CURRENT_DATE) apply

    ins = sb.table("ledger_bill_references").insert(row).execute()
    if not ins.data:
        return {"status": "error", "message": "Could not create bill reference", "status_code": 500}

    created = ins.data[0]
    write_audit_log(sb, "ledger_bill_reference", created["id"], "create", None, created, user_id)
    return {"status": "success", "message": f"Bill '{reference_no}' recorded", "data": created}


def recompute_bill_balance(sb, bill_reference_id: str) -> None:
    bill_res = sb.table("ledger_bill_references").select("id, bill_amount").eq("id", bill_reference_id).execute().data
    if not bill_res:
        return
    bill = bill_res[0]

    entries = (
        sb.table("voucher_entries")
        .select("amount, voucher_id")
        .eq("bill_reference_id", bill_reference_id)
        .eq("bill_allocation_type", "agst_ref")
        .execute()
        .data or []
    )
    settled = 0.0
    if entries:
        voucher_ids = list({e["voucher_id"] for e in entries})
        vouchers = sb.table("vouchers").select("id, is_active").in_("id", voucher_ids).execute().data or []
        active_ids = {v["id"] for v in vouchers if v["is_active"]}
        settled = sum(float(e["amount"]) for e in entries if e["voucher_id"] in active_ids)

    new_balance = round(float(bill["bill_amount"]) - settled, 2)
    sb.table("ledger_bill_references").update({
        "balance_amount": new_balance,
        "is_settled": new_balance <= 0,
        "updated_at": _now(),
    }).eq("id", bill_reference_id).execute()
