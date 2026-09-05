"""
LABOUR KHARCHA
================
Labour expense per truck/trip, itemized: Challan No, Weight, Unloading,
Crossing, Dala Munshiyana, Labour Wage, Other Charge — the bill total is
always the sum of those 5 charge fields, never typed in separately, so
they can never disagree. Same pattern as Truck Bhada: each labourer/
labour-gang is a ledger under 'Labour' (a creditor group — you owe them),
you add the itemized expense as a bill (Dr Labour Kharcha Expense / Cr
the labour ledger, breakdown saved as the bill's metadata), then pay it
off (Dr the labour ledger / Cr Cash-or-Bank).
"""
from services.supabase_client import get_supabase
from services.ledger.ledger_service import create_ledger, get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.bill_reference_service import list_bills
from services.ledger.voucher_service import create_voucher
from services.ledger.ledger_helpers import resolve_payment_ledger, get_or_create_named_ledger, get_or_create_group, today_ist

LABOUR_GROUP_NAME = "Labour"
SUNDRY_CREDITORS_GROUP_NAME = "Sundry Creditors"
LABOUR_KHARCHA_EXPENSE_LEDGER_NAME = "Labour Kharcha Expense"
DIRECT_EXPENSES_GROUP_NAME = "Direct Expenses"


def _labour_group_id() -> str | None:
    return get_or_create_group(LABOUR_GROUP_NAME, SUNDRY_CREDITORS_GROUP_NAME, "liability")


def list_labour(branch_id: str | None = None) -> dict:
    """Omit branch_id for the owner's "all branches" view."""
    group_id = _labour_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Labour group", "status_code": 500}

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


CHARGE_FIELDS = ("unloading", "crossing", "dala_munshiyana", "labour_wage", "other_charge")


def add_labour_expense(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, challan_no?, weight?,
              unloading?, crossing?, dala_munshiyana?, labour_wage?, other_charge?
              (each optional, defaults to 0 — fill in whichever apply),
              date?, narration?, created_by }

    The bill total is always the SUM of the 5 charge fields — never typed
    in directly — so the breakdown and the total can never disagree.
    Creates: Dr this branch's 'Labour Kharcha Expense' ledger  /
             Cr this labourer (new bill, with the full breakdown saved on
             it as metadata so it can be shown again later, itemized).
    """
    branch_id = data.get("branch_id")
    created_by = data.get("created_by")
    if not branch_id or not created_by:
        return {"status": "error", "message": "branch_id and created_by are required", "status_code": 400}

    charges = {f: round(float(data.get(f) or 0), 2) for f in CHARGE_FIELDS}
    total = round(sum(charges.values()), 2)
    if total <= 0:
        return {
            "status": "error",
            "message": "At least one charge (unloading/crossing/dala_munshiyana/labour_wage/other_charge) must be greater than 0",
            "status_code": 400,
        }

    expense = get_or_create_named_ledger(branch_id, LABOUR_KHARCHA_EXPENSE_LEDGER_NAME, DIRECT_EXPENSES_GROUP_NAME, created_by)
    if expense["status"] != "success":
        return expense

    challan_no = data.get("challan_no")
    weight = data.get("weight")
    reference_no = challan_no or f"LBR-{data.get('date') or today_ist()}"
    narration = data.get("narration") or (f"Labour Kharcha - Challan {challan_no}" if challan_no else "Labour Kharcha")
    metadata = {"challan_no": challan_no, "weight": weight, **charges}

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "purchase",
        "voucher_date": data.get("date") or today_ist(),
        "narration": narration,
        "reference_no": reference_no,
        "created_by": created_by,
        "entries": [
            {"ledger_id": expense["data"]["id"], "entry_type": "dr", "amount": total},
            {"ledger_id": ledger_id, "entry_type": "cr", "amount": total,
             "bill_allocation_type": "new_ref",
             "new_bill": {"reference_no": reference_no, "metadata": metadata}},
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
        "voucher_date": data.get("date") or today_ist(),
        "narration": data.get("narration") or f"Labour Kharcha paid ({payment_mode})",
        "created_by": created_by,
        "entries": [
            labour_entry,
            {"ledger_id": resolved["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })
