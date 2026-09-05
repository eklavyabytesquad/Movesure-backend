"""
LABOUR KHARCHA
================
Labour expense per truck/trip, with a note on what it was for (unloading,
food, etc). Same pattern as Truck Bhada: each labourer/labour-gang is a
ledger under 'Labour' (a creditor group — you owe them), you add an
expense as a bill (Dr Labour Kharcha Expense / Cr the labour ledger),
then pay it off (Dr the labour ledger / Cr Cash-or-Bank).
"""
from datetime import date
from services.supabase_client import get_supabase
from services.ledger.ledger_service import create_ledger, get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.bill_reference_service import list_bills
from services.ledger.voucher_service import create_voucher
from services.ledger.ledger_helpers import resolve_payment_ledger, get_or_create_named_ledger, get_or_create_group

LABOUR_GROUP_NAME = "Labour"
SUNDRY_CREDITORS_GROUP_NAME = "Sundry Creditors"
LABOUR_KHARCHA_EXPENSE_LEDGER_NAME = "Labour Kharcha Expense"
DIRECT_EXPENSES_GROUP_NAME = "Direct Expenses"


def _labour_group_id() -> str | None:
    return get_or_create_group(LABOUR_GROUP_NAME, SUNDRY_CREDITORS_GROUP_NAME, "liability")


def list_labour(branch_id: str) -> dict:
    group_id = _labour_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Labour group", "status_code": 500}

    sb = get_supabase()
    rows = (
        sb.table("ledgers")
        .select("id, name, phone, is_active")
        .eq("branch_id", branch_id)
        .eq("group_id", group_id)
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


def create_labour(data: dict) -> dict:
    """data = { branch_id, name, phone?, address?, created_by }"""
    branch_id = data.get("branch_id")
    name = (data.get("name") or "").strip()
    created_by = data.get("created_by")
    if not branch_id or not name or not created_by:
        return {"status": "error", "message": "branch_id, name and created_by are required", "status_code": 400}

    group_id = _labour_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Labour group", "status_code": 500}

    return create_ledger({
        "branch_id": branch_id,
        "name": name,
        "group_id": group_id,
        "phone": data.get("phone"),
        "address": data.get("address"),
        "is_bill_wise": True,
        "created_by": created_by,
    })


def get_labour_detail(ledger_id: str) -> dict:
    ledger_res = get_ledger(ledger_id)
    if ledger_res["status"] != "success":
        return ledger_res

    balance = get_ledger_balance(ledger_id)["data"]
    bills = list_bills(ledger_id)["data"]
    statement = get_ledger_statement(ledger_id)["data"]

    return {
        "status": "success",
        "data": {
            "ledger": ledger_res["data"],
            "balance": balance,
            "bills": bills,
            "statement": statement["entries"],
        },
    }


def add_labour_expense(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, expense_type (e.g. 'Unloading', 'Food'), truck_number?,
              challan_no?, amount, date?, narration?, created_by }
    Creates: Dr this branch's 'Labour Kharcha Expense' ledger  /  Cr this labourer (new bill)
    """
    branch_id = data.get("branch_id")
    expense_type = data.get("expense_type")
    amount = data.get("amount")
    created_by = data.get("created_by")
    if not branch_id or not expense_type or not amount or not created_by:
        return {"status": "error", "message": "branch_id, expense_type, amount and created_by are required", "status_code": 400}

    expense = get_or_create_named_ledger(branch_id, LABOUR_KHARCHA_EXPENSE_LEDGER_NAME, DIRECT_EXPENSES_GROUP_NAME, created_by)
    if expense["status"] != "success":
        return expense

    truck_number = data.get("truck_number")
    challan_no = data.get("challan_no")
    reference_no = challan_no or expense_type
    narration = data.get("narration") or (
        expense_type
        + (f" - Truck {truck_number}" if truck_number else "")
        + (f" - Challan {challan_no}" if challan_no else "")
    )

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "purchase",
        "voucher_date": data.get("date") or str(date.today()),
        "narration": narration,
        "reference_no": reference_no,
        "created_by": created_by,
        "entries": [
            {"ledger_id": expense["data"]["id"], "entry_type": "dr", "amount": amount},
            {"ledger_id": ledger_id, "entry_type": "cr", "amount": amount,
             "bill_allocation_type": "new_ref",
             "new_bill": {"reference_no": reference_no}},
        ],
    })


def pay_labour(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, amount, payment_mode: 'cash'|'bank', bank_ledger_id?,
              bill_reference_id?, date?, narration?, created_by }
    Creates: Dr this labourer (settles their bill)  /  Cr <resolved Cash/Bank>.
    """
    branch_id = data.get("branch_id")
    amount = data.get("amount")
    payment_mode = data.get("payment_mode")
    created_by = data.get("created_by")
    if not branch_id or not amount or payment_mode not in ("cash", "bank") or not created_by:
        return {"status": "error", "message": "branch_id, amount, payment_mode ('cash'/'bank') and created_by are required", "status_code": 400}

    resolved = resolve_payment_ledger(branch_id, payment_mode, data.get("bank_ledger_id"))
    if resolved["status"] != "success":
        return resolved

    bill_reference_id = data.get("bill_reference_id")
    labour_entry = {"ledger_id": ledger_id, "entry_type": "dr", "amount": amount}
    if bill_reference_id:
        labour_entry["bill_allocation_type"] = "agst_ref"
        labour_entry["bill_reference_id"] = bill_reference_id
    else:
        labour_entry["bill_allocation_type"] = "on_account"

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "payment",
        "voucher_date": data.get("date") or str(date.today()),
        "narration": data.get("narration") or f"Labour Kharcha paid ({payment_mode})",
        "created_by": created_by,
        "entries": [
            labour_entry,
            {"ledger_id": resolved["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })
