"""
Bank ledgers — list + default selection
=========================================
A branch can have several bank ledgers (e.g. "EKLAVYA BANK ACCOUNT",
"SS TRANSPORT CORPORATION BANK ACCOUNT"). Every "pay/receive by bank"
screen needs a dropdown of these, and one of them can be marked default —
the one every quick-entry screen falls back to when the user doesn't pick
a specific bank (see ledger_helpers.resolve_payment_ledger).
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.ledger_service import get_ledger_balance
from services.ledger.ledger_helpers import BANK_GROUP_NAME


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_banks(branch_id: str) -> dict:
    """Every active Bank Accounts ledger for this branch, with balance and
    is_default — exactly what the bank-picker dropdown needs."""
    sb = get_supabase()
    group = sb.table("ledger_groups").select("id").eq("name", BANK_GROUP_NAME).execute().data
    if not group:
        return {"status": "error", "message": f"No '{BANK_GROUP_NAME}' group exists in the chart of accounts", "status_code": 404}

    rows = (
        sb.table("ledgers")
        .select("id, name, is_default, is_active")
        .eq("branch_id", branch_id)
        .eq("group_id", group[0]["id"])
        .eq("is_active", True)
        .order("name")
        .execute()
        .data or []
    )
    out = []
    for r in rows:
        bal = get_ledger_balance(r["id"])["data"]
        out.append({**r, "balance": bal["balance"], "balance_type": bal["balance_type"]})
    return {"status": "success", "data": out}


def set_default_bank(ledger_id: str, branch_id: str) -> dict:
    """Marks this ledger as the branch's default bank — unsets any
    previous default in the same branch+group first, so there's always
    at most one (also enforced by a DB unique index)."""
    sb = get_supabase()
    group = sb.table("ledger_groups").select("id").eq("name", BANK_GROUP_NAME).execute().data
    if not group:
        return {"status": "error", "message": f"No '{BANK_GROUP_NAME}' group exists in the chart of accounts", "status_code": 404}
    group_id = group[0]["id"]

    row = sb.table("ledgers").select("id, name, branch_id, group_id, is_active").eq("id", ledger_id).execute().data
    if not row or not row[0]["is_active"]:
        return {"status": "error", "message": "Ledger not found", "status_code": 404}
    if row[0]["branch_id"] != branch_id or row[0]["group_id"] != group_id:
        return {"status": "error", "message": "That ledger is not an active Bank Accounts ledger for this branch", "status_code": 400}

    sb.table("ledgers").update({"is_default": False, "updated_at": _now()}).eq("branch_id", branch_id).eq("group_id", group_id).execute()
    sb.table("ledgers").update({"is_default": True, "updated_at": _now()}).eq("id", ledger_id).execute()

    return {"status": "success", "message": f"'{row[0]['name']}' is now the default bank for this branch"}
