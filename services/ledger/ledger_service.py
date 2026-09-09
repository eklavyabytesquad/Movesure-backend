"""
Ledgers — the actual accounts under a group
============================================
A ledger is one real account: a party ("XYZ Transport"), a bank account
("HDFC Bank CA"), or a fixed account like "Cash". Every ledger sits under
exactly one group (its category) and belongs to exactly one branch.

get_ledger_balance() and get_ledger_statement() compute everything live
from voucher_entries joined through vouchers.is_active — a cancelled
voucher's entries simply stop counting, no separate "undo" bookkeeping.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.audit_log_service import write_audit_log

LEDGER_COLS = (
    "id, branch_id, name, group_id, opening_balance, opening_balance_type, opening_balance_date, "
    "gstin, pan, address, phone, email, city_id, credit_period_days, credit_limit, "
    "is_bill_wise, is_active, created_by, updated_by, created_at, updated_at"
)

# The two groups that mean "this ledger represents a person/company you owe
# or who owes you" — Transporters nests under Sundry Debtors, Drivers and
# Labour nest under Sundry Creditors, so checking the whole ancestor chain
# (not just the immediate group) catches all of them.
PARTY_GROUP_NAMES = {"Sundry Debtors", "Sundry Creditors"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _all_groups_map(sb) -> dict:
    """{group_id: {name, nature, parent_group_id}} — the whole chart of
    accounts is small, one fetch covers every ledger's lookup below."""
    rows = sb.table("ledger_groups").select("id, name, nature, parent_group_id").execute().data or []
    return {r["id"]: r for r in rows}


def _group_context(groups_map: dict, group_id: str | None) -> dict:
    """The immediate group's name/nature, plus whether Sundry Debtors or
    Sundry Creditors appears anywhere in this ledger's ancestry — i.e.
    whether it's a PARTY ledger (a debtor/creditor relationship with a
    person or company) as opposed to a control ledger like Cash, Bank,
    an Income category, or an Expense category. This is what a bank
    ledger was missing entirely before — nothing told the caller it was a
    bank, so a "they owe you" debtor-style label got shown for it too."""
    group = groups_map.get(group_id) if group_id else None
    if not group:
        return {"group_name": None, "group_nature": None, "is_party_ledger": False}

    is_party = False
    current_id = group_id
    seen = set()
    while current_id and current_id not in seen:
        seen.add(current_id)
        g = groups_map.get(current_id)
        if not g:
            break
        if g["name"] in PARTY_GROUP_NAMES:
            is_party = True
            break
        current_id = g.get("parent_group_id")

    return {"group_name": group["name"], "group_nature": group["nature"], "is_party_ledger": is_party}


def _balance_label(group_nature: str | None, is_party_ledger: bool, balance_type: str) -> str:
    """A ready-to-render phrase for a balance, correct for what this
    ledger actually is — never a one-size-fits-all "they owe you"."""
    if is_party_ledger:
        if group_nature == "asset":        # Sundry Debtors / Transporters
            return "They owe you" if balance_type == "dr" else "You owe them"
        return "You owe them" if balance_type == "cr" else "They owe you"  # Sundry Creditors / Drivers / Labour
    if group_nature == "asset":            # Cash-in-Hand, Bank Accounts, etc.
        return "Balance available" if balance_type == "dr" else "Overdrawn"
    if group_nature == "liability":
        return "Outstanding balance" if balance_type == "cr" else "Overpaid"
    if group_nature == "income":
        return "Total earned" if balance_type == "cr" else "Net reversed"
    if group_nature == "expense":
        return "Total spent" if balance_type == "dr" else "Net refunded"
    return "Balance"


def _signed(amount: float, side: str) -> float:
    """Dr is positive, Cr is negative — the one sign convention every
    balance calculation in this file uses internally."""
    return amount if side == "dr" else -amount


def _split_signed(signed: float) -> tuple[float, str]:
    return (round(abs(signed), 2), "dr" if signed >= 0 else "cr")


def list_ledgers(branch_id: str | None = None, group_id: str | None = None,
                  search: str | None = None, is_active: bool | None = True,
                  page: int = 1, page_size: int = 50) -> dict:
    sb = get_supabase()
    q = sb.table("ledgers").select(LEDGER_COLS, count="exact").order("name")
    if branch_id:
        q = q.eq("branch_id", branch_id)
    if group_id:
        q = q.eq("group_id", group_id)
    if is_active is not None:
        q = q.eq("is_active", is_active)
    if search:
        q = q.ilike("name", f"%{search.strip()}%")

    offset = (page - 1) * page_size
    q = q.range(offset, offset + page_size - 1)
    res = q.execute()
    total = res.count if res.count is not None else len(res.data or [])
    rows = res.data or []

    # Tag each row with its branch name whenever branch_id was omitted
    # (the owner's "all branches" view) — with one branch_id filtered in,
    # every row already shares it, so the tag would be pure noise.
    if not branch_id and rows:
        from services.branch_service import get_branch_name_map
        branch_map = get_branch_name_map([r["branch_id"] for r in rows])
        for r in rows:
            r["branch_name"] = branch_map.get(r["branch_id"])

    # Tag each row with what TYPE of ledger it is (its group's name/nature,
    # and whether it's a party/debtor-creditor ledger) — without this, the
    # UI has no way to know a ledger is a Bank Account vs a Transporter,
    # and ends up applying debtor-style "they owe you" wording to everything.
    if rows:
        groups_map = _all_groups_map(sb)
        for r in rows:
            r.update(_group_context(groups_map, r.get("group_id")))

    return {
        "status": "success",
        "data": {
            "rows": rows, "total": total, "page": page,
            "page_size": page_size, "has_more": (offset + page_size) < total,
        },
    }


def get_ledger(ledger_id: str) -> dict:
    sb = get_supabase()
    res = sb.table("ledgers").select(LEDGER_COLS).eq("id", ledger_id).execute()
    if not res.data:
        return {"status": "error", "message": "Ledger not found", "status_code": 404}
    ledger = res.data[0]
    ledger.update(_group_context(_all_groups_map(sb), ledger.get("group_id")))
    return {"status": "success", "data": ledger}


def create_ledger(data: dict, user_id: str | None = None) -> dict:
    name = (data.get("name") or "").strip()
    branch_id = data.get("branch_id")
    group_id = data.get("group_id")
    created_by = user_id or data.get("created_by")
    if not name or not branch_id or not group_id or not created_by:
        return {"status": "error", "message": "name, branch_id, group_id and created_by are required", "status_code": 400}

    sb = get_supabase()
    if not sb.table("ledger_groups").select("id").eq("id", group_id).execute().data:
        return {"status": "error", "message": "group_id not found", "status_code": 404}

    row = {
        "branch_id": branch_id,
        "name": name,
        "group_id": group_id,
        "opening_balance": data.get("opening_balance", 0),
        "opening_balance_type": data.get("opening_balance_type", "dr"),
        "opening_balance_date": data.get("opening_balance_date"),
        "gstin": data.get("gstin"),
        "pan": data.get("pan"),
        "address": data.get("address"),
        "phone": data.get("phone"),
        "email": data.get("email"),
        "city_id": data.get("city_id"),
        "credit_period_days": data.get("credit_period_days"),
        "credit_limit": data.get("credit_limit"),
        "is_bill_wise": bool(data.get("is_bill_wise", False)),
        "created_by": created_by,
    }
    ins = sb.table("ledgers").insert(row).execute()
    if not ins.data:
        return {"status": "error", "message": "Could not create ledger — that name may already exist in this branch", "status_code": 409}

    created = ins.data[0]
    write_audit_log(sb, "ledger", created["id"], "create", None, created, user_id)
    return {"status": "success", "message": f"Ledger '{name}' created", "data": created}


def update_ledger(ledger_id: str, data: dict, user_id: str | None = None) -> dict:
    sb = get_supabase()
    existing_res = sb.table("ledgers").select(LEDGER_COLS).eq("id", ledger_id).execute().data
    if not existing_res:
        return {"status": "error", "message": "Ledger not found", "status_code": 404}
    existing = existing_res[0]

    allowed = {
        "name", "group_id", "opening_balance", "opening_balance_type", "opening_balance_date",
        "gstin", "pan", "address", "phone", "email", "city_id",
        "credit_period_days", "credit_limit", "is_bill_wise",
    }
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        return {"status": "error", "message": "No updatable fields provided", "status_code": 400}
    payload["updated_by"] = user_id
    payload["updated_at"] = _now()

    res = sb.table("ledgers").update(payload).eq("id", ledger_id).execute()
    if not res.data:
        return {"status": "error", "message": "Update failed — that name may already exist in this branch", "status_code": 409}

    updated = res.data[0]
    write_audit_log(sb, "ledger", ledger_id, "update", existing, updated, user_id)
    return {"status": "success", "data": updated}


def set_ledger_status(ledger_id: str, is_active: bool, user_id: str | None = None) -> dict:
    sb = get_supabase()
    existing_res = sb.table("ledgers").select(LEDGER_COLS).eq("id", ledger_id).execute().data
    if not existing_res:
        return {"status": "error", "message": "Ledger not found", "status_code": 404}
    existing = existing_res[0]

    sb.table("ledgers").update({
        "is_active": is_active, "updated_by": user_id, "updated_at": _now(),
    }).eq("id", ledger_id).execute()

    action = "reactivate" if is_active else "deactivate"
    write_audit_log(sb, "ledger", ledger_id, action, existing, {"is_active": is_active}, user_id)
    verb = "reactivated" if is_active else "deactivated"
    return {"status": "success", "message": f"Ledger '{existing['name']}' {verb}"}


def get_ledger_balance(ledger_id: str) -> dict:
    sb = get_supabase()
    ledger_res = sb.table("ledgers").select("id, name, group_id, opening_balance, opening_balance_type").eq("id", ledger_id).execute().data
    if not ledger_res:
        return {"status": "error", "message": "Ledger not found", "status_code": 404}
    ledger = ledger_res[0]

    entries = sb.table("voucher_entries").select("entry_type, amount, voucher_id").eq("ledger_id", ledger_id).execute().data or []
    active_ids = _active_voucher_ids(sb, entries)

    dr_total = sum(float(e["amount"]) for e in entries if e["voucher_id"] in active_ids and e["entry_type"] == "dr")
    cr_total = sum(float(e["amount"]) for e in entries if e["voucher_id"] in active_ids and e["entry_type"] == "cr")

    opening_signed = _signed(float(ledger["opening_balance"]), ledger["opening_balance_type"])
    closing_signed = opening_signed + dr_total - cr_total
    amount, side = _split_signed(closing_signed)

    group_ctx = _group_context(_all_groups_map(sb), ledger.get("group_id"))

    return {
        "status": "success",
        "data": {
            "ledger_id": ledger_id,
            "name": ledger["name"],
            "opening_balance": ledger["opening_balance"],
            "opening_balance_type": ledger["opening_balance_type"],
            "total_dr": round(dr_total, 2),
            "total_cr": round(cr_total, 2),
            "balance": amount,
            "balance_type": side,
            **group_ctx,
            "balance_label": _balance_label(group_ctx["group_nature"], group_ctx["is_party_ledger"], side),
        },
    }


def get_ledger_statement(ledger_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    """A running-balance statement, Tally-style: the balance shown for each
    row is always correct against the ledger's TRUE opening balance, even
    when from_date/to_date only shows a slice of the period."""
    sb = get_supabase()
    ledger_res = sb.table("ledgers").select("id, name, group_id, opening_balance, opening_balance_type").eq("id", ledger_id).execute().data
    if not ledger_res:
        return {"status": "error", "message": "Ledger not found", "status_code": 404}
    ledger = ledger_res[0]
    group_ctx = _group_context(_all_groups_map(sb), ledger.get("group_id"))

    rows = (
        sb.table("voucher_entries")
        .select("id, entry_type, amount, narration, voucher_id, created_at, "
                 "vouchers(voucher_no, voucher_type, voucher_date, narration, is_active)")
        .eq("ledger_id", ledger_id)
        .execute()
        .data or []
    )
    active = [r for r in rows if r.get("vouchers") and r["vouchers"].get("is_active")]
    # Sort by voucher_date first, then by the actual insert timestamp — NOT
    # by row id — so same-day entries land in the order they really
    # happened (a uuid has no chronological meaning at all).
    active.sort(key=lambda r: (r["vouchers"]["voucher_date"], r["created_at"]))

    running = _signed(float(ledger["opening_balance"]), ledger["opening_balance_type"])
    statement = []
    for r in active:
        v = r["vouchers"]
        running += _signed(float(r["amount"]), r["entry_type"])
        vd = v["voucher_date"]
        if (from_date and vd < from_date) or (to_date and vd > to_date):
            continue
        amount, side = _split_signed(running)
        statement.append({
            "voucher_id": r["voucher_id"],
            "voucher_no": v["voucher_no"],
            "voucher_type": v["voucher_type"],
            "voucher_date": vd,
            "time": r["created_at"],
            "narration": r.get("narration") or v.get("narration"),
            "entry_type": r["entry_type"],
            "amount": r["amount"],
            "running_balance": amount,
            "running_balance_type": side,
        })

    closing_amount, closing_side = _split_signed(running)
    opening_amount, opening_side = _split_signed(_signed(float(ledger["opening_balance"]), ledger["opening_balance_type"]))
    return {
        "status": "success",
        "data": {
            "ledger_id": ledger_id,
            "name": ledger["name"],
            "opening_balance": ledger["opening_balance"],
            "opening_balance_type": ledger["opening_balance_type"],
            "entries": statement,
            "closing_balance": closing_amount,
            "closing_balance_type": closing_side,
            **group_ctx,
            "opening_balance_label": _balance_label(group_ctx["group_nature"], group_ctx["is_party_ledger"], opening_side),
            "closing_balance_label": _balance_label(group_ctx["group_nature"], group_ctx["is_party_ledger"], closing_side),
        },
    }


def _active_voucher_ids(sb, entries: list[dict]) -> set[str]:
    """Given a list of rows with a voucher_id, return the subset of those
    voucher_ids whose voucher is still active (not cancelled)."""
    voucher_ids = list({e["voucher_id"] for e in entries})
    if not voucher_ids:
        return set()
    vouchers = sb.table("vouchers").select("id, is_active").in_("id", voucher_ids).execute().data or []
    return {v["id"] for v in vouchers if v["is_active"]}
