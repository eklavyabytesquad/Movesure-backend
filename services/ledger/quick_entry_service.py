"""
Quick Entry — simple Income/Expense recording, no Dr/Cr shown anywhere
========================================================================
For day-to-day cash/bank movements that aren't a transporter bill (a
delivery charge collected in cash against a GR, a small cash expense),
the frontend should never need to know a voucher's Dr/Cr shape, AND it
should never need to guess "which ledger is this branch's Cash/Bank
account" — that guess is exactly what caused the "No Bank Account ledger
found" bug: the frontend had its own copy of that lookup logic, name-
mismatched against the real data.

resolve_branch_ledger() is now the ONLY place that lookup happens, by
GROUP name ('Cash-in-Hand', 'Bank Accounts') — never by guessing a ledger
name — and it always returns a clear, actionable error (matching exactly
what the UI should show) if the ledger doesn't exist yet or is ambiguous.
Every quick-entry function below, and transporter_service.py, both call
this ONE function instead of duplicating the lookup.
"""
from datetime import date
from services.supabase_client import get_supabase
from services.ledger.voucher_service import create_voucher

CASH_GROUP_NAME = "Cash-in-Hand"
BANK_GROUP_NAME = "Bank Accounts"


def resolve_branch_ledger(branch_id: str, group_name: str) -> dict:
    """The one active ledger for this branch under the given GROUP name."""
    sb = get_supabase()
    group = sb.table("ledger_groups").select("id").eq("name", group_name).execute().data
    if not group:
        return {"status": "error", "message": f"No '{group_name}' group exists in the chart of accounts", "status_code": 404}
    group_id = group[0]["id"]

    ledgers = (
        sb.table("ledgers")
        .select("id, name")
        .eq("branch_id", branch_id)
        .eq("group_id", group_id)
        .eq("is_active", True)
        .execute()
        .data or []
    )
    if not ledgers:
        return {"status": "error", "message": f"No {group_name} ledger found for this branch. Create one in Ledger Master first.", "status_code": 404}
    if len(ledgers) > 1:
        names = ", ".join(l["name"] for l in ledgers)
        return {"status": "error", "message": f"This branch has more than one {group_name} ledger ({names}) — use the full Voucher screen to pick one", "status_code": 409}

    return {"status": "success", "data": ledgers[0]}


def resolve_branch_ledger_by_name(branch_id: str, name: str) -> dict:
    """The one active ledger for this branch with exactly this NAME —
    for well-known, single, by-convention ledgers like 'PF Income' or
    'Delivery Income' (unlike Cash/Bank, these aren't a whole group of
    interchangeable accounts, they're one specific ledger you name this
    way on purpose in every branch)."""
    sb = get_supabase()
    rows = (
        sb.table("ledgers")
        .select("id, name")
        .eq("branch_id", branch_id)
        .ilike("name", name)
        .eq("is_active", True)
        .execute()
        .data or []
    )
    if not rows:
        return {"status": "error", "message": f"No '{name}' ledger found for this branch. Create one in Ledger Master first (name it exactly '{name}').", "status_code": 404}
    return {"status": "success", "data": rows[0]}


def record_income(data: dict) -> dict:
    """
    data = { branch_id, income_ledger_id, cash_or_bank: 'cash'|'bank',
              amount, reference_no?, narration?, date?, created_by }
    Creates: Dr <branch's Cash or Bank ledger>  /  Cr income_ledger_id
    """
    branch_id = data.get("branch_id")
    income_ledger_id = data.get("income_ledger_id")
    cash_or_bank = data.get("cash_or_bank")
    amount = data.get("amount")
    created_by = data.get("created_by")

    if not branch_id or not income_ledger_id or cash_or_bank not in ("cash", "bank") or not amount or float(amount) <= 0 or not created_by:
        return {"status": "error", "message": "branch_id, income_ledger_id, cash_or_bank ('cash'/'bank'), amount and created_by are required", "status_code": 400}

    resolved = resolve_branch_ledger(branch_id, CASH_GROUP_NAME if cash_or_bank == "cash" else BANK_GROUP_NAME)
    if resolved["status"] != "success":
        return resolved
    receiving_ledger_id = resolved["data"]["id"]

    reference_no = data.get("reference_no")
    narration = data.get("narration") or (f"Ref {reference_no}" if reference_no else None)

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "receipt",
        "voucher_date": data.get("date") or str(date.today()),
        "narration": narration,
        "reference_no": reference_no,
        "created_by": created_by,
        "entries": [
            {"ledger_id": receiving_ledger_id, "entry_type": "dr", "amount": amount},
            {"ledger_id": income_ledger_id, "entry_type": "cr", "amount": amount},
        ],
    })


def record_expense(data: dict) -> dict:
    """
    data = { branch_id, expense_ledger_id, cash_or_bank: 'cash'|'bank',
              amount, reference_no?, narration?, date?, created_by }
    Creates: Dr expense_ledger_id  /  Cr <branch's Cash or Bank ledger>
    """
    branch_id = data.get("branch_id")
    expense_ledger_id = data.get("expense_ledger_id")
    cash_or_bank = data.get("cash_or_bank")
    amount = data.get("amount")
    created_by = data.get("created_by")

    if not branch_id or not expense_ledger_id or cash_or_bank not in ("cash", "bank") or not amount or float(amount) <= 0 or not created_by:
        return {"status": "error", "message": "branch_id, expense_ledger_id, cash_or_bank ('cash'/'bank'), amount and created_by are required", "status_code": 400}

    resolved = resolve_branch_ledger(branch_id, CASH_GROUP_NAME if cash_or_bank == "cash" else BANK_GROUP_NAME)
    if resolved["status"] != "success":
        return resolved
    paying_ledger_id = resolved["data"]["id"]

    reference_no = data.get("reference_no")
    narration = data.get("narration") or (f"Ref {reference_no}" if reference_no else None)

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "payment",
        "voucher_date": data.get("date") or str(date.today()),
        "narration": narration,
        "reference_no": reference_no,
        "created_by": created_by,
        "entries": [
            {"ledger_id": expense_ledger_id, "entry_type": "dr", "amount": amount},
            {"ledger_id": paying_ledger_id, "entry_type": "cr", "amount": amount},
        ],
    })


def record_delivery_income(data: dict) -> dict:
    """
    data = { branch_id, gr_no, amount, cash_or_bank, date?, created_by }
    Convenience over record_income(): resolves THIS branch's 'Delivery
    Income' ledger by name, and uses gr_no as the reference/narration —
    exactly "Ref GRNO 5142, Delivery Amount 300, cash".
    """
    branch_id = data.get("branch_id")
    delivery_income = resolve_branch_ledger_by_name(branch_id, "Delivery Income")
    if delivery_income["status"] != "success":
        return delivery_income

    gr_no = data.get("gr_no")
    return record_income({
        **data,
        "income_ledger_id": delivery_income["data"]["id"],
        "reference_no": gr_no,
        "narration": data.get("narration") or (f"Delivery income - GR {gr_no}" if gr_no else None),
    })


def record_delivery_expense(data: dict) -> dict:
    """
    data = { branch_id, gr_no?, expense_ledger_id, amount, cash_or_bank, date?, created_by }
    Same as record_expense(), just standardizes the GR-based reference —
    the expense category (fuel, labour, etc.) is still an explicit choice
    since "expense" isn't one fixed ledger the way Delivery Income is.
    """
    gr_no = data.get("gr_no")
    return record_expense({
        **data,
        "reference_no": data.get("reference_no") or gr_no,
        "narration": data.get("narration") or (f"Delivery expense - GR {gr_no}" if gr_no else None),
    })
