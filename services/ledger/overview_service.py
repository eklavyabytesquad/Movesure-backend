"""
LEDGERS overview
==================
Every ledger, grouped by its category, for the "browse all ledgers,
group-wise" tab — Transporters, Drivers, Labour, Bank Accounts,
Cash-in-Hand, Direct Incomes, Direct Expenses, etc. all show up as their
own section automatically (whatever groups actually have ledgers under
them, nothing hardcoded).

Omit branch_id for the owner's "all branches" view — every ledger across
every branch, still grouped by category, each one tagged with which
branch it belongs to.
"""
from services.supabase_client import get_supabase
from services.ledger.ledger_service import get_ledger_balance
from services.branch_service import get_branch_name_map


def get_ledger_overview(branch_id: str | None = None) -> dict:
    sb = get_supabase()

    q = (
        sb.table("ledgers")
        .select("id, name, branch_id, group_id, gstin, is_bill_wise")
        .eq("is_active", True)
        .order("name")
    )
    if branch_id:
        q = q.eq("branch_id", branch_id)
    ledgers = q.execute().data or []
    if not ledgers:
        return {"status": "success", "data": {}}

    group_ids = list({l["group_id"] for l in ledgers})
    groups = sb.table("ledger_groups").select("id, name").in_("id", group_ids).execute().data or []
    group_name_map = {g["id"]: g["name"] for g in groups}

    branch_map = get_branch_name_map([l["branch_id"] for l in ledgers]) if not branch_id else {}

    out: dict[str, list[dict]] = {}
    for l in ledgers:
        category = group_name_map.get(l["group_id"], "Other")
        balance = get_ledger_balance(l["id"])["data"]
        row = {
            "id": l["id"],
            "name": l["name"],
            "gstin": l["gstin"],
            "is_bill_wise": l["is_bill_wise"],
            "balance": balance["balance"],
            "balance_type": balance["balance_type"],
        }
        if not branch_id:
            row["branch_id"] = l["branch_id"]
            row["branch_name"] = branch_map.get(l["branch_id"])
        out.setdefault(category, []).append(row)

    return {"status": "success", "data": out}
