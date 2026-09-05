"""
LEDGERS overview
==================
Every ledger for a branch, grouped by its category, for the "browse all
ledgers, group-wise" tab — Transporters, Drivers, Labour, Bank Accounts,
Cash-in-Hand, Direct Incomes, Direct Expenses, etc. all show up as their
own section automatically (whatever groups actually have ledgers under
them for this branch — nothing hardcoded).
"""
from services.supabase_client import get_supabase
from services.ledger.ledger_service import get_ledger_balance


def get_ledger_overview(branch_id: str) -> dict:
    sb = get_supabase()

    ledgers = (
        sb.table("ledgers")
        .select("id, name, group_id, gstin, is_bill_wise")
        .eq("branch_id", branch_id)
        .eq("is_active", True)
        .order("name")
        .execute()
        .data or []
    )
    if not ledgers:
        return {"status": "success", "data": {}}

    group_ids = list({l["group_id"] for l in ledgers})
    groups = sb.table("ledger_groups").select("id, name").in_("id", group_ids).execute().data or []
    group_name_map = {g["id"]: g["name"] for g in groups}

    out: dict[str, list[dict]] = {}
    for l in ledgers:
        category = group_name_map.get(l["group_id"], "Other")
        balance = get_ledger_balance(l["id"])["data"]
        out.setdefault(category, []).append({
            "id": l["id"],
            "name": l["name"],
            "gstin": l["gstin"],
            "is_bill_wise": l["is_bill_wise"],
            "balance": balance["balance"],
            "balance_type": balance["balance_type"],
        })

    return {"status": "success", "data": out}
