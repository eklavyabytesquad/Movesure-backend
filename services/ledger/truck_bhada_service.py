"""
TRUCK BHADA
=============
Per-trip (challan) truck-hire expense, driver/truck-wise. Each driver or
truck-owner is a ledger under 'Drivers' — a CREDITOR group, since the
branch owes them for each trip (the opposite direction from Transporters).

Add a trip's Bhada as a bill referencing the challan number (Dr Truck
Bhada Expense / Cr the driver, new bill); pay it off later via cash or
bank (Dr the driver / Cr Cash-or-Bank) — same bill-by-bill pattern as
Transport PF Collection, just reversed.
"""
from services.supabase_client import get_supabase
from services.ledger.ledger_service import create_ledger, get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.bill_reference_service import list_bills
from services.ledger.voucher_service import create_voucher
from services.ledger.ledger_helpers import resolve_payment_ledger, get_or_create_named_ledger, get_or_create_group, today_ist

DRIVERS_GROUP_NAME = "Drivers"
SUNDRY_CREDITORS_GROUP_NAME = "Sundry Creditors"
TRUCK_BHADA_EXPENSE_LEDGER_NAME = "Truck Bhada Expense"
DIRECT_EXPENSES_GROUP_NAME = "Direct Expenses"


def _drivers_group_id() -> str | None:
    return get_or_create_group(DRIVERS_GROUP_NAME, SUNDRY_CREDITORS_GROUP_NAME, "liability")


def list_drivers(branch_id: str | None = None) -> dict:
    """Every driver/truck-owner ledger, with live balance (a Cr balance =
    what you still owe them). Omit branch_id for the owner's "all
    branches" view."""
    group_id = _drivers_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Drivers group", "status_code": 500}

    sb = get_supabase()
    q = (
        sb.table("ledgers")
        .select("id, name, branch_id, phone, is_active")
        .eq("group_id", group_id)
        .eq("is_active", True)
        .order("name")
    )
    if branch_id:
        q = q.eq("branch_id", branch_id)
    rows = q.execute().data or []

    if not branch_id and rows:
        from services.branch_service import get_branch_name_map
        branch_map = get_branch_name_map([r["branch_id"] for r in rows])
        for r in rows:
            r["branch_name"] = branch_map.get(r["branch_id"])

    out = []
    for r in rows:
        bal = get_ledger_balance(r["id"])["data"]
        out.append({**r, "balance": bal["balance"], "balance_type": bal["balance_type"]})
    return {"status": "success", "data": out}


def create_driver(data: dict) -> dict:
    """data = { branch_id, name, phone?, address?, created_by }"""
    branch_id = data.get("branch_id")
    name = (data.get("name") or "").strip()
    created_by = data.get("created_by")
    if not branch_id or not name or not created_by:
        return {"status": "error", "message": "branch_id, name and created_by are required", "status_code": 400}

    group_id = _drivers_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Drivers group", "status_code": 500}

    return create_ledger({
        "branch_id": branch_id,
        "name": name,
        "group_id": group_id,
        "phone": data.get("phone"),
        "address": data.get("address"),
        "is_bill_wise": True,
        "created_by": created_by,
    })


def get_driver_detail(ledger_id: str) -> dict:
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


def add_trip_bhada(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, challan_no, truck_number?, amount, date?, narration?, created_by }
    Creates: Dr this branch's 'Truck Bhada Expense' ledger  /  Cr this driver (new bill, ref = challan_no)
    """
    branch_id = data.get("branch_id")
    challan_no = data.get("challan_no")
    amount = data.get("amount")
    created_by = data.get("created_by")
    if not branch_id or not challan_no or not amount or not created_by:
        return {"status": "error", "message": "branch_id, challan_no, amount and created_by are required", "status_code": 400}

    expense = get_or_create_named_ledger(branch_id, TRUCK_BHADA_EXPENSE_LEDGER_NAME, DIRECT_EXPENSES_GROUP_NAME, created_by)
    if expense["status"] != "success":
        return expense

    truck_number = data.get("truck_number")
    narration = data.get("narration") or (
        f"Truck Bhada - Challan {challan_no}" + (f" - Truck {truck_number}" if truck_number else "")
    )

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "purchase",
        "voucher_date": data.get("date") or today_ist(),
        "narration": narration,
        "reference_no": challan_no,
        "created_by": created_by,
        "entries": [
            {"ledger_id": expense["data"]["id"], "entry_type": "dr", "amount": amount},
            {"ledger_id": ledger_id, "entry_type": "cr", "amount": amount,
             "bill_allocation_type": "new_ref",
             "new_bill": {"reference_no": challan_no}},
        ],
    })


def pay_driver(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, amount, payment_mode: 'cash'|'bank', bank_ledger_id?,
              bill_reference_id? (omit for an on-account payment), date?,
              narration?, created_by }
    Creates: Dr this driver (settles their bill)  /  Cr <resolved Cash/Bank>.
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
    driver_entry = {"ledger_id": ledger_id, "entry_type": "dr", "amount": amount}
    if bill_reference_id:
        driver_entry["bill_allocation_type"] = "agst_ref"
        driver_entry["bill_reference_id"] = bill_reference_id
    else:
        driver_entry["bill_allocation_type"] = "on_account"

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "payment",
        "voucher_date": data.get("date") or today_ist(),
        "narration": data.get("narration") or f"Truck Bhada paid ({payment_mode})",
        "created_by": created_by,
        "entries": [
            driver_entry,
            {"ledger_id": resolved["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })
