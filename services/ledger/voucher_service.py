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

update_voucher() edits a voucher in place (same id, same voucher_no) through
the ledger_update_voucher() Postgres function (migration 023), so the header
and every entry change in ONE transaction and the same deferred trigger
re-checks Dr == Cr at commit.

Cancelling a voucher never deletes it (audit trail) — it just flips
is_active=false. Every balance calculation elsewhere in this package
already filters on vouchers.is_active, so a cancelled voucher's entries
simply stop counting everywhere, automatically.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.audit_log_service import write_audit_log
from services.ledger.ledger_helpers import today_ist
from services.ledger.bill_reference_service import (
    create_bill, recompute_bill_balance, paid_against_bill,
)

VOUCHER_PREFIX = {
    "payment": "PAY", "receipt": "REC", "journal": "JV", "contra": "CTR",
    "sales": "SAL", "purchase": "PUR", "debit_note": "DBN", "credit_note": "CRN",
}

VOUCHER_COLS = (
    "id, branch_id, voucher_type, voucher_no, voucher_date, narration, reference_no, "
    "total_amount, is_active, cancelled_at, cancelled_by, cancel_reason, "
    "created_by, updated_by, created_at, updated_at"
)
ENTRY_COLS = "id, ledger_id, entry_type, amount, narration, bill_reference_id, bill_allocation_type"

# Frontend spelling -> DB spelling (the DB CHECK constraint only knows 'agst_ref')
ALLOCATION_ALIASES = {"against_ref": "agst_ref", "agst": "agst_ref", "against": "agst_ref"}

CASH_BANK_GROUP_NAMES = {"Cash-in-Hand", "Bank Accounts"}

_source_supported: bool | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400) -> dict:
    return {"status": "error", "message": msg, "status_code": code}


def source_supported(sb) -> bool:
    """vouchers.source exists only after migration 023, so this code is safe to
    deploy before the migration runs. Only a positive result is cached — while
    the column is missing it is re-checked, so running 023 takes effect without
    a restart."""
    global _source_supported
    if _source_supported:
        return True
    try:
        sb.table("vouchers").select("source").limit(1).execute()
        _source_supported = True
    except Exception:
        _source_supported = False
    return _source_supported


def _voucher_cols(sb) -> str:
    return VOUCHER_COLS + (", source" if source_supported(sb) else "")


def _normalize_entries(entries_in: list[dict]) -> list[dict]:
    out = []
    for e in entries_in:
        e = dict(e)
        alloc = e.get("bill_allocation_type")
        if alloc:
            e["bill_allocation_type"] = ALLOCATION_ALIASES.get(alloc, alloc)
        out.append(e)
    return out


def _validate_entries(entries_in: list[dict]) -> tuple[float, str | None]:
    """Returns (dr_total, error_message)."""
    if len(entries_in) < 2:
        return 0, "A voucher needs at least 2 entries (one Dr, one Cr)"
    for e in entries_in:
        try:
            amount_ok = e.get("amount") is not None and float(e["amount"]) > 0
        except (TypeError, ValueError):
            amount_ok = False
        if not e.get("ledger_id") or e.get("entry_type") not in ("dr", "cr") or not amount_ok:
            return 0, "Every entry needs ledger_id, entry_type ('dr'/'cr') and a positive amount"
        alloc = e.get("bill_allocation_type")
        if alloc and alloc not in ("new_ref", "agst_ref", "advance", "on_account"):
            return 0, "bill_allocation_type must be one of new_ref, agst_ref (against_ref), advance, on_account"
        if alloc == "agst_ref" and not e.get("bill_reference_id"):
            return 0, "bill_allocation_type 'agst_ref' requires bill_reference_id"
    dr_total = round(sum(float(e["amount"]) for e in entries_in if e["entry_type"] == "dr"), 2)
    cr_total = round(sum(float(e["amount"]) for e in entries_in if e["entry_type"] == "cr"), 2)
    if dr_total != cr_total:
        return 0, f"Voucher is not balanced: Dr {dr_total} != Cr {cr_total}"
    return dr_total, None


def last_voucher_seq(sb, branch_id: str, voucher_type: str) -> int:
    """Highest number used for this branch + type. voucher_no is text, so a plain
    text sort puts SAL10000 before SAL9999 — check the longest numbers first
    (party ledger imports use thousands of sales numbers)."""
    prefix = VOUCHER_PREFIX[voucher_type]
    for width in range(9, 3, -1):
        res = (
            sb.table("vouchers")
            .select("voucher_no")
            .eq("branch_id", branch_id)
            .eq("voucher_type", voucher_type)
            .like("voucher_no", prefix + "_" * width)
            .order("voucher_no", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            try:
                return int(res.data[0]["voucher_no"][len(prefix):])
            except (ValueError, IndexError):
                continue
    return 0


def _next_voucher_no(sb, branch_id: str, voucher_type: str) -> str:
    return f"{VOUCHER_PREFIX[voucher_type]}{str(last_voucher_seq(sb, branch_id, voucher_type) + 1).zfill(4)}"


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
    v = sb.table("vouchers").select(_voucher_cols(sb)).eq("id", voucher_id).execute().data
    if not v:
        return _err("Voucher not found", 404)
    voucher = v[0]

    entries = (
        sb.table("voucher_entries")
        .select(ENTRY_COLS + ", created_at, ledgers(name)")
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
      source? (party screens: the original create body, see migrations/023),
      entries: [
        { ledger_id, entry_type: 'dr'|'cr', amount, narration?,
          bill_allocation_type?: 'new_ref' | 'agst_ref' (alias 'against_ref') | 'advance' | 'on_account',
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
    entries_in = _normalize_entries(data.get("entries") or [])
    created_by = data.get("created_by")

    if not branch_id or not voucher_type or not created_by:
        return _err("branch_id, voucher_type and created_by are required")
    if voucher_type not in VOUCHER_PREFIX:
        return _err(f"voucher_type must be one of {sorted(VOUCHER_PREFIX)}")
    dr_total, err = _validate_entries(entries_in)
    if err:
        return _err(err)

    sb = get_supabase()
    voucher_date = data.get("voucher_date") or today_ist()
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
    if data.get("source") is not None and source_supported(sb):
        voucher_row["source"] = data["source"]
    ins = sb.table("vouchers").insert(voucher_row).execute()
    if not ins.data:
        return _err("Could not create voucher", 500)
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
        return _err(f"Failed to create voucher: {exc}")

    touched_bills = {r["bill_reference_id"] for r in entry_rows if r.get("bill_reference_id")}
    for bill_id in touched_bills:
        recompute_bill_balance(sb, bill_id)

    write_audit_log(sb, "voucher", voucher_id, "create", None, {**voucher, "entries": entry_rows}, created_by)

    result = get_voucher(voucher_id)
    if result["status"] == "success":
        result["message"] = f"Voucher {voucher_no} created"
    return result


# ── Edit ─────────────────────────────────────────────────────────────────────
def _auto_match_ids(old_entries: list[dict], new_entries: list[dict]) -> None:
    """Party screens rebuild entries from a form, without ids. Give each new
    entry the id of an unused old entry on the same ledger + side, so bills
    and audit history stay attached to the same rows."""
    used = set()
    for ne in new_entries:
        if ne.get("id"):
            used.add(ne["id"])
            continue
        match = next((oe for oe in old_entries
                      if oe["id"] not in used
                      and oe["ledger_id"] == ne["ledger_id"]
                      and oe["entry_type"] == ne["entry_type"]), None)
        if match:
            ne["id"] = match["id"]
            used.add(match["id"])


def update_voucher(voucher_id: str, data: dict, auto_match: bool = False) -> dict:
    """
    data = { voucher_date?, narration?, reference_no?, entries?, updated_by, edit_reason?, source? }
    Omit `entries` to change only the header. When sent, `entries` is the FULL new
    list: entry with `id` = updated, without `id` = new, existing id left out = removed.
    Keeps the same id and voucher_no. Bill rules:
      - an entry that raised a bill (new_ref) may change amount, but can't be removed
        or moved to another ledger/side; its bill_amount follows the new amount and
        can't drop below what is already paid against it
      - agst_ref (payment against a bill) amount changes recompute that bill
    """
    sb = get_supabase()
    vres = sb.table("vouchers").select(_voucher_cols(sb)).eq("id", voucher_id).execute().data
    if not vres:
        return _err("Voucher not found", 404)
    old_voucher = vres[0]
    if not old_voucher["is_active"]:
        return _err("Cancelled voucher can't be edited")
    old_entries = sb.table("voucher_entries").select(ENTRY_COLS).eq("voucher_id", voucher_id).execute().data or []
    updated_by = data.get("updated_by") or data.get("created_by")

    header = {}
    if data.get("voucher_date"):          # '' would fail the ::date cast inside the RPC
        header["voucher_date"] = data["voucher_date"]
    for k in ("narration", "reference_no"):
        if k in data:
            header[k] = data[k]
    if "source" in data and source_supported(sb):
        header["source"] = data["source"]
    if updated_by:
        header["updated_by"] = updated_by

    entries_payload = None
    bill_updates: dict[str, dict] = {}
    created_bill_ids: list[str] = []
    touched_bills = {e["bill_reference_id"] for e in old_entries if e.get("bill_reference_id")}

    if "entries" in data and data["entries"] is not None:
        new_entries = _normalize_entries(data["entries"])
        dr_total, err = _validate_entries(new_entries)
        if err:
            return _err(err)
        if auto_match:
            _auto_match_ids(old_entries, new_entries)
        old_by_id = {e["id"]: e for e in old_entries}
        for ne in new_entries:
            if ne.get("id") and ne["id"] not in old_by_id:
                return _err(f"Entry {ne['id']} does not belong to this voucher")

        # Bills raised by this voucher (new_ref) — keep them attached and valid
        new_by_id = {ne["id"]: ne for ne in new_entries if ne.get("id")}
        for oe in old_entries:
            if oe.get("bill_allocation_type") != "new_ref" or not oe.get("bill_reference_id"):
                continue
            ne = new_by_id.get(oe["id"])
            if not ne or ne["ledger_id"] != oe["ledger_id"] or ne["entry_type"] != oe["entry_type"]:
                return _err("A line that raised a bill can't be removed or moved to another ledger/side — "
                            "cancel the voucher instead")
            bill_id = oe["bill_reference_id"]
            new_amount = round(float(ne["amount"]), 2)
            paid = paid_against_bill(sb, bill_id, exclude_voucher_id=voucher_id)
            if new_amount < paid:
                return _err(f"Bill already has ₹{paid:,.2f} paid against it")
            ne["bill_allocation_type"] = "new_ref"
            ne["bill_reference_id"] = bill_id
            upd = {"bill_amount": new_amount}
            nb = ne.get("new_bill") or {}
            for f in ("reference_no", "reference_date", "due_date", "metadata"):
                if nb.get(f) is not None:
                    upd[f] = nb[f]
            bill_updates[bill_id] = upd

        voucher_date = data.get("voucher_date") or old_voucher["voucher_date"]
        try:
            for ne in new_entries:
                if ne.get("bill_allocation_type") == "new_ref" and not ne.get("bill_reference_id"):
                    nb = ne.get("new_bill") or {}
                    cb = create_bill({
                        "ledger_id": ne["ledger_id"],
                        "reference_no": nb.get("reference_no") or old_voucher["voucher_no"],
                        "reference_date": nb.get("reference_date") or voucher_date,
                        "due_date": nb.get("due_date"),
                        "bill_amount": ne["amount"],
                        "entry_type": ne["entry_type"],
                        "source_table": nb.get("source_table"),
                        "source_id": nb.get("source_id"),
                        "metadata": nb.get("metadata"),
                        "created_by": updated_by or old_voucher["created_by"],
                    }, sb=sb)
                    if cb["status"] != "success":
                        raise RuntimeError(cb["message"])
                    ne["bill_reference_id"] = cb["data"]["id"]
                    created_bill_ids.append(cb["data"]["id"])
        except Exception as exc:
            _drop_bills(sb, created_bill_ids)
            return _err(f"Failed to update voucher: {exc}")

        entries_payload = [{
            "id": ne.get("id"),
            "ledger_id": ne["ledger_id"],
            "entry_type": ne["entry_type"],
            "amount": float(ne["amount"]),
            "narration": ne.get("narration"),
            "bill_reference_id": ne.get("bill_reference_id"),
            "bill_allocation_type": ne.get("bill_allocation_type"),
        } for ne in new_entries]
        header["total_amount"] = dr_total
        touched_bills |= {ne["bill_reference_id"] for ne in entries_payload if ne.get("bill_reference_id")}

    if not header or (list(header) == ["updated_by"] and entries_payload is None):
        return _err("Nothing to update")

    try:
        sb.rpc("ledger_update_voucher", {
            "p_voucher_id": voucher_id, "p_header": header, "p_entries": entries_payload,
        }).execute()
    except Exception as exc:
        _drop_bills(sb, created_bill_ids)
        msg = str(exc)
        if "ledger_update_voucher" in msg and ("does not exist" in msg or "Could not find" in msg):
            return _err("Voucher edit needs migrations/023_ledger_voucher_edit.sql — run it first", 500)
        if "not balanced" in msg:
            return _err("Voucher is not balanced")
        if "Cancelled voucher" in msg:
            return _err("Cancelled voucher can't be edited")
        return _err(f"Failed to update voucher: {msg}")

    for bill_id, upd in bill_updates.items():
        sb.table("ledger_bill_references").update({**upd, "updated_at": _now()}).eq("id", bill_id).execute()
    for bill_id in touched_bills:
        recompute_bill_balance(sb, bill_id)

    result = get_voucher(voucher_id)
    write_audit_log(sb, "voucher", voucher_id, "update",
                    {**old_voucher, "entries": old_entries},
                    {**(result.get("data") or {}), "edit_reason": data.get("edit_reason")},
                    updated_by)
    if result["status"] == "success":
        result["message"] = f"Voucher {old_voucher['voucher_no']} updated"
    return result


def _drop_bills(sb, bill_ids: list[str]) -> None:
    for bid in bill_ids:
        try:
            sb.table("ledger_bill_references").delete().eq("id", bid).execute()
        except Exception as exc:
            print(f"Could not roll back bill {bid}: {exc}")


def save_party_voucher(payload: dict, party_ledger_id: str, action: str,
                       body: dict, voucher_id: str | None = None) -> dict:
    """Shared create/edit path for the party screens (Transport PF, Truck Bhada,
    Labour). `payload` is the create_voucher() body the screen built; `body` is
    the request body, stored as vouchers.source so the UI can pre-fill an edit."""
    source = {"action": action, **{k: v for k, v in body.items()
                                   if k not in ("created_by", "updated_by", "edit_reason")}}
    if not voucher_id:
        return create_voucher({**payload, "source": source})

    sb = get_supabase()
    vres = sb.table("vouchers").select(_voucher_cols(sb)).eq("id", voucher_id).execute().data
    if not vres:
        return _err("Voucher not found", 404)
    v = vres[0]
    if v["branch_id"] != payload["branch_id"]:
        return _err("This voucher belongs to a different branch")
    if v["voucher_type"] != payload["voucher_type"]:
        return _err(f"This is a {v['voucher_type']} voucher, not a '{action}' entry")
    old_action = (v.get("source") or {}).get("action")
    if old_action and old_action != action:
        return _err(f"This voucher was created as '{old_action}', not '{action}'")
    has_party = sb.table("voucher_entries").select("id").eq("voucher_id", voucher_id) \
        .eq("ledger_id", party_ledger_id).limit(1).execute().data
    if not has_party:
        return _err("This voucher doesn't belong to this ledger")

    edit = {k: payload[k] for k in ("voucher_date", "narration", "reference_no") if payload.get(k) is not None}
    return update_voucher(voucher_id, {
        **edit,
        "entries": payload["entries"],
        "source": source,
        "updated_by": body.get("updated_by") or body.get("created_by"),
        "edit_reason": body.get("edit_reason"),
    }, auto_match=True)


def prepare_party_edit(voucher_id: str, data: dict) -> dict:
    """Edit bodies reuse the create builders, which default `date` to today —
    keep the voucher's own date unless the caller sends a new one, and accept
    updated_by where the builders expect created_by."""
    data = dict(data or {})
    if not data.get("date"):
        v = get_supabase().table("vouchers").select("voucher_date").eq("id", voucher_id).execute().data
        if v:
            data["date"] = v[0]["voucher_date"]
    if not data.get("created_by"):
        data["created_by"] = data.get("updated_by")
    return data


# ── Cancel ───────────────────────────────────────────────────────────────────
def cancel_voucher(voucher_id: str, user_id: str | None = None, reason: str | None = None) -> dict:
    """Cancelling a bill voucher (new_ref) is REJECTED while payments are still
    applied to that bill — cancel those payments first. Cancelling a payment
    (agst_ref) gives the amount back to the bill's outstanding and clears is_settled."""
    sb = get_supabase()
    existing_res = sb.table("vouchers").select(VOUCHER_COLS).eq("id", voucher_id).execute().data
    if not existing_res:
        return _err("Voucher not found", 404)
    voucher = existing_res[0]
    if not voucher["is_active"]:
        return _err("Voucher is already cancelled")

    entries = sb.table("voucher_entries").select("bill_reference_id, bill_allocation_type") \
        .eq("voucher_id", voucher_id).execute().data or []

    raised = [e["bill_reference_id"] for e in entries
              if e.get("bill_allocation_type") == "new_ref" and e.get("bill_reference_id")]
    for bill_id in raised:
        payers = payments_against_bill(sb, bill_id, exclude_voucher_id=voucher_id)
        if payers:
            nos = ", ".join(p["voucher_no"] for p in payers)
            return _err(f"Cancel the payments against this bill first ({nos})")

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


def payments_against_bill(sb, bill_id: str, exclude_voucher_id: str | None = None) -> list[dict]:
    """Active vouchers that settle (agst_ref) this bill."""
    rows = sb.table("voucher_entries").select("voucher_id").eq("bill_reference_id", bill_id) \
        .eq("bill_allocation_type", "agst_ref").execute().data or []
    ids = list({r["voucher_id"] for r in rows if r["voucher_id"] != exclude_voucher_id})
    if not ids:
        return []
    return sb.table("vouchers").select("id, voucher_no").in_("id", ids).eq("is_active", True).execute().data or []


# ── Ledger entry shortcut ────────────────────────────────────────────────────
def _is_cash_or_bank(groups: dict, group_id: str | None) -> bool:
    seen = set()
    while group_id and group_id not in seen:
        seen.add(group_id)
        g = groups.get(group_id)
        if not g:
            return False
        if g["name"] in CASH_BANK_GROUP_NAMES:
            return True
        group_id = g.get("parent_group_id")
    return False


def post_ledger_entry(ledger_id: str, data: dict) -> dict:
    """
    data = { side: 'dr'|'cr', amount, against_ledger_id, voucher_type?, voucher_date?,
             narration?, reference_no?, created_by }
    Posts a 2-line voucher: this ledger on `side`, against_ledger_id on the other side.
    voucher_type is inferred when omitted (see README).
    """
    side = data.get("side")
    against_id = data.get("against_ledger_id")
    created_by = data.get("created_by")
    try:
        amount = round(float(data.get("amount")), 2)
    except (TypeError, ValueError):
        amount = 0
    if side not in ("dr", "cr") or amount <= 0 or not against_id or not created_by:
        return _err("side ('dr'/'cr'), a positive amount, against_ledger_id and created_by are required")
    if against_id == ledger_id:
        return _err("against_ledger_id can't be the same ledger")

    sb = get_supabase()
    rows = sb.table("ledgers").select("id, name, branch_id, group_id, is_active") \
        .in_("id", [ledger_id, against_id]).execute().data or []
    by_id = {r["id"]: r for r in rows}
    this, other = by_id.get(ledger_id), by_id.get(against_id)
    if not this:
        return _err("Ledger not found", 404)
    if not other:
        return _err("against_ledger_id not found", 404)
    if not this["is_active"] or not other["is_active"]:
        return _err("Both ledgers must be active")
    if this["branch_id"] != other["branch_id"]:
        return _err("Both ledgers must be in the same branch")

    voucher_type = data.get("voucher_type")
    if not voucher_type:
        groups = {g["id"]: g for g in (sb.table("ledger_groups").select("id, name, parent_group_id").execute().data or [])}
        this_cb, other_cb = _is_cash_or_bank(groups, this["group_id"]), _is_cash_or_bank(groups, other["group_id"])
        if this_cb and other_cb:
            voucher_type = "contra"
        elif other_cb:
            voucher_type = "receipt" if side == "cr" else "payment"
        elif this_cb:
            voucher_type = "receipt" if side == "dr" else "payment"
        else:
            voucher_type = "journal"

    other_side = "cr" if side == "dr" else "dr"
    return create_voucher({
        "branch_id": this["branch_id"],
        "voucher_type": voucher_type,
        "voucher_date": data.get("voucher_date"),
        "narration": data.get("narration"),
        "reference_no": data.get("reference_no"),
        "created_by": created_by,
        "entries": [
            {"ledger_id": ledger_id, "entry_type": side, "amount": amount},
            {"ledger_id": against_id, "entry_type": other_side, "amount": amount},
        ],
    })
