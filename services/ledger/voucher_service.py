"""
Vouchers — transaction headers + their Dr/Cr entries (double entry)
=====================================================================
A voucher is one transaction: Payment, Receipt, Journal, Contra, Sales,
Purchase, Debit Note, or Credit Note — Tally's exact voucher types. Every
voucher has 2+ entries (voucher_entries), each either a Dr or a Cr line,
and the totals MUST match.

create_voucher() checks Dr==Cr itself first (for a clean error message),
then inserts every entry for the voucher in ONE batched call — the
database's own deferred constraint trigger (migration 005) then re-checks
the whole voucher at once and would reject anything that slipped through,
so the balance guarantee is real, not just application-level.

Cancelling a voucher never deletes it (audit trail) — it just flips
is_active=false. Every balance calculation elsewhere in this package
already filters on vouchers.is_active, so a cancelled voucher's entries
simply stop counting everywhere, automatically.
"""
from datetime import date, datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.audit_log_service import write_audit_log
from services.ledger.bill_reference_service import create_bill, recompute_bill_balance

VOUCHER_PREFIX = {
    "payment": "PAY", "receipt": "REC", "journal": "JV", "contra": "CTR",
    "sales": "SAL", "purchase": "PUR", "debit_note": "DBN", "credit_note": "CRN",
}

VOUCHER_COLS = (
    "id, branch_id, voucher_type, voucher_no, voucher_date, narration, reference_no, "
    "total_amount, is_active, cancelled_at, cancelled_by, cancel_reason, "
    "created_by, updated_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _next_voucher_no(sb, branch_id: str, voucher_type: str) -> str:
    prefix = VOUCHER_PREFIX[voucher_type]
    res = (
        sb.table("vouchers")
        .select("voucher_no")
        .eq("branch_id", branch_id)
        .eq("voucher_type", voucher_type)
        .ilike("voucher_no", f"{prefix}%")
        .order("voucher_no", desc=True)
        .limit(1)
        .execute()
    )
    last_seq = 0
    if res.data:
        try:
            last_seq = int(res.data[0]["voucher_no"][len(prefix):])
        except (ValueError, IndexError):
            last_seq = 0
    return f"{prefix}{str(last_seq + 1).zfill(4)}"


def list_vouchers(branch_id: str | None = None, voucher_type: str | None = None,
                   from_date: str | None = None, to_date: str | None = None,
                   is_active: bool | None = True, page: int = 1, page_size: int = 40) -> dict:
    sb = get_supabase()
    q = sb.table("vouchers").select(VOUCHER_COLS, count="exact").order("voucher_date", desc=True)
    if branch_id:
        q = q.eq("branch_id", branch_id)
    if voucher_type:
        q = q.eq("voucher_type", voucher_type)
    if is_active is not None:
        q = q.eq("is_active", is_active)
    if from_date:
        q = q.gte("voucher_date", from_date)
    if to_date:
        q = q.lte("voucher_date", to_date)

    offset = (page - 1) * page_size
    q = q.range(offset, offset + page_size - 1)
    res = q.execute()
    total = res.count if res.count is not None else len(res.data or [])
    rows = res.data or []

    if not branch_id and rows:
        from services.branch_service import get_branch_name_map
        branch_map = get_branch_name_map([r["branch_id"] for r in rows])
        for r in rows:
            r["branch_name"] = branch_map.get(r["branch_id"])

    return {
        "status": "success",
        "data": {
            "rows": rows, "total": total, "page": page,
            "page_size": page_size, "has_more": (offset + page_size) < total,
        },
    }


def get_voucher(voucher_id: str) -> dict:
    sb = get_supabase()
    v = sb.table("vouchers").select(VOUCHER_COLS).eq("id", voucher_id).execute().data
    if not v:
        return {"status": "error", "message": "Voucher not found", "status_code": 404}
    voucher = v[0]

    entries = (
        sb.table("voucher_entries")
        .select("id, ledger_id, entry_type, amount, narration, bill_reference_id, "
                 "bill_allocation_type, created_at, ledgers(name)")
        .eq("voucher_id", voucher_id)
        .execute()
        .data or []
    )
    for e in entries:
        ledger = e.pop("ledgers", None)
        e["ledger_name"] = ledger.get("name") if ledger else None
    voucher["entries"] = entries

    return {"status": "success", "data": voucher}


def create_voucher(data: dict) -> dict:
    """
    data = {
      branch_id, voucher_type, voucher_date?, narration?, reference_no?, created_by,
      entries: [
        { ledger_id, entry_type: 'dr'|'cr', amount, narration?,
          bill_allocation_type?: 'new_ref' | 'agst_ref' | 'advance' | 'on_account',
          bill_reference_id?   (required when bill_allocation_type='agst_ref'),
          new_bill?: { reference_no?, reference_date?, due_date?, source_table?, source_id?, metadata? }
              (used when bill_allocation_type='new_ref'; reference_no defaults
              to this voucher's own voucher_no if omitted; metadata is a
              freeform breakdown, e.g. Labour Kharcha's charge-by-charge total)
        }, ...
      ]
    }
    Needs at least 2 entries, and total Dr must equal total Cr.
    """
    branch_id = data.get("branch_id")
    voucher_type = data.get("voucher_type")
    entries_in = data.get("entries") or []
    created_by = data.get("created_by")

    if not branch_id or not voucher_type or not created_by:
        return {"status": "error", "message": "branch_id, voucher_type and created_by are required", "status_code": 400}
    if voucher_type not in VOUCHER_PREFIX:
        return {"status": "error", "message": f"voucher_type must be one of {sorted(VOUCHER_PREFIX)}", "status_code": 400}
    if len(entries_in) < 2:
        return {"status": "error", "message": "A voucher needs at least 2 entries (one Dr, one Cr)", "status_code": 400}

    for e in entries_in:
        if not e.get("ledger_id") or e.get("entry_type") not in ("dr", "cr") or not e.get("amount") or float(e["amount"]) <= 0:
            return {"status": "error", "message": "Every entry needs ledger_id, entry_type ('dr'/'cr') and a positive amount", "status_code": 400}
        if e.get("bill_allocation_type") == "agst_ref" and not e.get("bill_reference_id"):
            return {"status": "error", "message": "bill_allocation_type 'agst_ref' requires bill_reference_id", "status_code": 400}

    dr_total = round(sum(float(e["amount"]) for e in entries_in if e["entry_type"] == "dr"), 2)
    cr_total = round(sum(float(e["amount"]) for e in entries_in if e["entry_type"] == "cr"), 2)
    if dr_total != cr_total:
        return {"status": "error", "message": f"Voucher is not balanced: Dr {dr_total} != Cr {cr_total}", "status_code": 400}

    sb = get_supabase()
    voucher_date = data.get("voucher_date") or str(date.today())
    voucher_no = _next_voucher_no(sb, branch_id, voucher_type)

    voucher_row = {
        "branch_id": branch_id,
        "voucher_type": voucher_type,
        "voucher_no": voucher_no,
        "voucher_date": voucher_date,
        "narration": data.get("narration"),
        "reference_no": data.get("reference_no"),
        "total_amount": dr_total,
        "created_by": created_by,
    }
    ins = sb.table("vouchers").insert(voucher_row).execute()
    if not ins.data:
        return {"status": "error", "message": "Could not create voucher", "status_code": 500}
    voucher = ins.data[0]
    voucher_id = voucher["id"]

    try:
        entry_rows = []
        for e in entries_in:
            bill_reference_id = e.get("bill_reference_id")
            if e.get("bill_allocation_type") == "new_ref" and not bill_reference_id:
                nb = e.get("new_bill") or {}
                created_bill = create_bill({
                    "ledger_id": e["ledger_id"],
                    "reference_no": nb.get("reference_no") or voucher_no,
                    "reference_date": nb.get("reference_date") or voucher_date,
                    "due_date": nb.get("due_date"),
                    "bill_amount": e["amount"],
                    "entry_type": e["entry_type"],
                    "source_table": nb.get("source_table"),
                    "source_id": nb.get("source_id"),
                    "metadata": nb.get("metadata"),
                    "created_by": created_by,
                }, sb=sb)
                if created_bill["status"] != "success":
                    raise RuntimeError(created_bill["message"])
                bill_reference_id = created_bill["data"]["id"]

            entry_rows.append({
                "voucher_id": voucher_id,
                "ledger_id": e["ledger_id"],
                "entry_type": e["entry_type"],
                "amount": e["amount"],
                "narration": e.get("narration"),
                "bill_reference_id": bill_reference_id,
                "bill_allocation_type": e.get("bill_allocation_type"),
            })

        # ONE batched insert — the DB's deferred balance trigger validates
        # the whole voucher at once (see migration 005's header comment).
        sb.table("voucher_entries").insert(entry_rows).execute()

    except Exception as exc:
        sb.table("vouchers").delete().eq("id", voucher_id).execute()
        return {"status": "error", "message": f"Failed to create voucher: {exc}", "status_code": 400}

    touched_bills = {r["bill_reference_id"] for r in entry_rows if r.get("bill_reference_id")}
    for bill_id in touched_bills:
        recompute_bill_balance(sb, bill_id)

    write_audit_log(sb, "voucher", voucher_id, "create", None, {**voucher, "entries": entry_rows}, created_by)

    result = get_voucher(voucher_id)
    if result["status"] == "success":
        result["message"] = f"Voucher {voucher_no} created"
    return result


def cancel_voucher(voucher_id: str, user_id: str | None = None, reason: str | None = None) -> dict:
    sb = get_supabase()
    existing_res = sb.table("vouchers").select(VOUCHER_COLS).eq("id", voucher_id).execute().data
    if not existing_res:
        return {"status": "error", "message": "Voucher not found", "status_code": 404}
    voucher = existing_res[0]
    if not voucher["is_active"]:
        return {"status": "error", "message": "Voucher is already cancelled", "status_code": 400}

    entries = sb.table("voucher_entries").select("bill_reference_id").eq("voucher_id", voucher_id).execute().data or []

    now = _now()
    sb.table("vouchers").update({
        "is_active": False, "cancelled_at": now, "cancelled_by": user_id,
        "cancel_reason": reason, "updated_at": now,
    }).eq("id", voucher_id).execute()

    touched_bills = {e["bill_reference_id"] for e in entries if e.get("bill_reference_id")}
    for bill_id in touched_bills:
        recompute_bill_balance(sb, bill_id)

    write_audit_log(sb, "voucher", voucher_id, "cancel", voucher, {"is_active": False, "cancel_reason": reason}, user_id)
    return {"status": "success", "message": f"Voucher {voucher['voucher_no']} cancelled"}
