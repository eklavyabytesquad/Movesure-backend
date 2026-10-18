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

direction (computed, not stored):
  receivable = bill raised on the Dr side (they owe us)
  payable    = bill raised on the Cr side (we owe them)
is_void (computed): the voucher that raised the bill was cancelled — such
bills are hidden from lists unless include_void=true.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.audit_log_service import write_audit_log

BILL_COLS = (
    "id, ledger_id, reference_no, reference_date, due_date, bill_amount, balance_amount, "
    "entry_type, source_table, source_id, metadata, is_settled, created_by, created_at, updated_at"
)
DIRECTIONS = {"receivable": "dr", "payable": "cr"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400) -> dict:
    return {"status": "error", "message": msg, "status_code": code}


def _void_bill_ids(sb, bill_ids: list[str]) -> set[str]:
    """Bills whose raising (new_ref) voucher has been cancelled."""
    if not bill_ids:
        return set()
    raising = sb.table("voucher_entries").select("bill_reference_id, voucher_id") \
        .in_("bill_reference_id", bill_ids).eq("bill_allocation_type", "new_ref").execute().data or []
    if not raising:
        return set()
    v_ids = list({r["voucher_id"] for r in raising})
    inactive = {v["id"] for v in (sb.table("vouchers").select("id, is_active").in_("id", v_ids).execute().data or [])
                if not v["is_active"]}
    return {r["bill_reference_id"] for r in raising if r["voucher_id"] in inactive}


def _decorate(sb, bills: list[dict]) -> list[dict]:
    void = _void_bill_ids(sb, [b["id"] for b in bills])
    for b in bills:
        b["direction"] = "receivable" if b.get("entry_type") == "dr" else "payable"
        b["is_void"] = b["id"] in void
    return bills


def list_bills(ledger_id: str, is_settled: bool | None = None, direction: str | None = None,
               include_void: bool = False) -> dict:
    sb = get_supabase()
    q = sb.table("ledger_bill_references").select(BILL_COLS).eq("ledger_id", ledger_id).order("reference_date")
    if is_settled is not None:
        q = q.eq("is_settled", is_settled)
    if direction:
        if direction not in DIRECTIONS:
            return _err("direction must be 'receivable' or 'payable'")
        q = q.eq("entry_type", DIRECTIONS[direction])
    bills = _decorate(sb, q.execute().data or [])
    if not include_void:
        bills = [b for b in bills if not b["is_void"]]
    return {"status": "success", "data": bills}


def get_bill(bill_id: str) -> dict:
    sb = get_supabase()
    res = sb.table("ledger_bill_references").select(BILL_COLS).eq("id", bill_id).execute()
    if not res.data:
        return _err("Bill not found", 404)
    return {"status": "success", "data": _decorate(sb, res.data)[0]}


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
        return _err("ledger_id, reference_no, bill_amount and entry_type ('dr'/'cr') are required")
    if not created_by:
        return _err("created_by is required")

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
        return _err("Could not create bill reference", 500)

    created = ins.data[0]
    write_audit_log(sb, "ledger_bill_reference", created["id"], "create", None, created, user_id)
    return {"status": "success", "message": f"Bill '{reference_no}' recorded", "data": created}


def update_bill(bill_id: str, data: dict) -> dict:
    """data = { reference_no?, due_date?, updated_by }. Amounts are never edited
    here — change the voucher that raised the bill instead (PUT /vouchers/{id})."""
    sb = get_supabase()
    res = sb.table("ledger_bill_references").select(BILL_COLS).eq("id", bill_id).execute().data
    if not res:
        return _err("Bill not found", 404)
    bill = res[0]

    payload = {}
    if "reference_no" in data:
        ref = (data.get("reference_no") or "").strip()
        if not ref:
            return _err("reference_no can't be empty")
        if ref.upper() != (bill["reference_no"] or "").upper():
            same_ledger = sb.table("ledger_bill_references").select("id, reference_no") \
                .eq("ledger_id", bill["ledger_id"]).execute().data or []
            if any(b["id"] != bill_id and (b["reference_no"] or "").strip().upper() == ref.upper() for b in same_ledger):
                return _err(f"Bill '{ref}' already exists on this ledger", 409)
        payload["reference_no"] = ref
    if "due_date" in data:
        payload["due_date"] = data.get("due_date") or None
    if not payload:
        return _err("Nothing to update — only reference_no and due_date can be edited here")
    payload["updated_at"] = _now()

    sb.table("ledger_bill_references").update(payload).eq("id", bill_id).execute()
    updated = get_bill(bill_id)
    write_audit_log(sb, "ledger_bill_reference", bill_id, "update", bill, updated.get("data"), data.get("updated_by"))
    if updated["status"] == "success":
        updated["message"] = "Bill updated"
    return updated


def paid_against_bill(sb, bill_reference_id: str, exclude_voucher_id: str | None = None) -> float:
    """Total of active agst_ref payments applied to this bill."""
    entries = sb.table("voucher_entries").select("amount, voucher_id") \
        .eq("bill_reference_id", bill_reference_id).eq("bill_allocation_type", "agst_ref").execute().data or []
    entries = [e for e in entries if e["voucher_id"] != exclude_voucher_id]
    if not entries:
        return 0.0
    voucher_ids = list({e["voucher_id"] for e in entries})
    vouchers = sb.table("vouchers").select("id, is_active").in_("id", voucher_ids).execute().data or []
    active_ids = {v["id"] for v in vouchers if v["is_active"]}
    return round(sum(float(e["amount"]) for e in entries if e["voucher_id"] in active_ids), 2)


def recompute_bill_balance(sb, bill_reference_id: str) -> None:
    bill_res = sb.table("ledger_bill_references").select("id, bill_amount").eq("id", bill_reference_id).execute().data
    if not bill_res:
        return
    bill = bill_res[0]

    # If the voucher that ORIGINALLY raised this bill (bill_allocation_type
    # = 'new_ref') has since been cancelled, the bill itself is void — it
    # should never show as outstanding again, matching the ledger's real
    # balance (which already excludes cancelled vouchers' entries). Without
    # this check, cancelling a pf-bill/payable-bill/bhada/labour-expense
    # voucher left its bill permanently stuck "Open" with its original
    # amount, even though the true balance had already moved on.
    creating_entry = (
        sb.table("voucher_entries")
        .select("voucher_id")
        .eq("bill_reference_id", bill_reference_id)
        .eq("bill_allocation_type", "new_ref")
        .limit(1)
        .execute()
        .data
    )
    if creating_entry:
        creating_voucher = (
            sb.table("vouchers").select("is_active")
            .eq("id", creating_entry[0]["voucher_id"])
            .execute().data
        )
        if creating_voucher and not creating_voucher[0]["is_active"]:
            sb.table("ledger_bill_references").update({
                "balance_amount": 0,
                "is_settled": True,
                "updated_at": _now(),
            }).eq("id", bill_reference_id).execute()
            return

    settled = paid_against_bill(sb, bill_reference_id)
    new_balance = round(float(bill["bill_amount"]) - settled, 2)
    sb.table("ledger_bill_references").update({
        "balance_amount": new_balance,
        "is_settled": new_balance <= 0,
        "updated_at": _now(),
    }).eq("id", bill_reference_id).execute()
