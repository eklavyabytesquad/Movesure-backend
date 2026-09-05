"""
TRANSPORT PF COLLECTION
=========================
Add a transporter, raise their PF bill, collect payment FROM them, give
payment TO them (an advance or settling something outside a bill), click
their name to see everything about them at once.

All transporters live under one shared "Transporters" group (auto-created
under Sundry Debtors the first time it's needed).
"""
from services.supabase_client import get_supabase
from services.ledger.ledger_service import create_ledger, get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.bill_reference_service import list_bills
from services.ledger.voucher_service import create_voucher
from services.ledger.ledger_helpers import resolve_payment_ledger, get_or_create_named_ledger, get_or_create_group, today_ist

TRANSPORTERS_GROUP_NAME = "Transporters"
SUNDRY_DEBTORS_GROUP_NAME = "Sundry Debtors"
PF_INCOME_LEDGER_NAME = "PF Income"
DIRECT_INCOMES_GROUP_NAME = "Direct Incomes"


def _transporters_group_id() -> str | None:
    return get_or_create_group(TRANSPORTERS_GROUP_NAME, SUNDRY_DEBTORS_GROUP_NAME, "asset")


def list_transporters(branch_id: str | None = None) -> dict:
    """Every transporter ledger, each with its live balance already
    attached — enough to render the whole list page in one call. Omit
    branch_id for the owner's "all branches" view (each row then also
    carries branch_id/branch_name so you can tell them apart)."""
    group_id = _transporters_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Transporters group", "status_code": 500}

    sb = get_supabase()
    q = (
        sb.table("ledgers")
        .select("id, name, branch_id, gstin, phone, is_active")
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


def create_transporter(data: dict) -> dict:
    """data = { branch_id, name, gstin?, phone?, address?, opening_balance?, opening_balance_type?, created_by }"""
    branch_id = data.get("branch_id")
    name = (data.get("name") or "").strip()
    created_by = data.get("created_by")
    if not branch_id or not name or not created_by:
        return {"status": "error", "message": "branch_id, name and created_by are required", "status_code": 400}

    group_id = _transporters_group_id()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Transporters group", "status_code": 500}

    return create_ledger({
        "branch_id": branch_id,
        "name": name,
        "group_id": group_id,
        "gstin": data.get("gstin"),
        "phone": data.get("phone"),
        "address": data.get("address"),
        "opening_balance": data.get("opening_balance", 0),
        "opening_balance_type": data.get("opening_balance_type", "dr"),
        "is_bill_wise": True,
        "created_by": created_by,
    })


def get_transporter_detail(ledger_id: str) -> dict:
    """Everything needed for the 'click a transporter to see their ledger'
    page, in one call: the ledger record, its balance, every bill (paid
    and unpaid), and its full transaction statement (every receipt/payment)."""
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


def raise_pf_bill(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, amount, reference_no, due_date?, date?, narration?, created_by }
    Creates: Dr this transporter (a new bill)  /  Cr this branch's 'PF Income' ledger.
    """
    branch_id = data.get("branch_id")
    amount = data.get("amount")
    reference_no = data.get("reference_no")
    created_by = data.get("created_by")
    if not branch_id or not amount or not reference_no or not created_by:
        return {"status": "error", "message": "branch_id, amount, reference_no and created_by are required", "status_code": 400}

    pf_income = get_or_create_named_ledger(branch_id, PF_INCOME_LEDGER_NAME, DIRECT_INCOMES_GROUP_NAME, created_by)
    if pf_income["status"] != "success":
        return pf_income

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "sales",
        "voucher_date": data.get("date") or today_ist(),
        "narration": data.get("narration") or f"PF settlement bill {reference_no}",
        "created_by": created_by,
        "entries": [
            {"ledger_id": ledger_id, "entry_type": "dr", "amount": amount,
             "bill_allocation_type": "new_ref",
             "new_bill": {"reference_no": reference_no, "due_date": data.get("due_date")}},
            {"ledger_id": pf_income["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })


def collect_payment(ledger_id: str, data: dict) -> dict:
    """
    Money COLLECTED FROM this transporter.
    data = { branch_id, amount, payment_mode: 'cash'|'bank', bank_ledger_id?,
              bill_reference_id? (omit for an on-account collection not
              tied to one specific bill), date?, narration?, created_by }
    Creates: Dr <resolved Cash/Bank>  /  Cr this transporter.
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
    transporter_entry = {"ledger_id": ledger_id, "entry_type": "cr", "amount": amount}
    if bill_reference_id:
        transporter_entry["bill_allocation_type"] = "agst_ref"
        transporter_entry["bill_reference_id"] = bill_reference_id
    else:
        transporter_entry["bill_allocation_type"] = "on_account"

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "receipt",
        "voucher_date": data.get("date") or today_ist(),
        "narration": data.get("narration") or f"Payment collected ({payment_mode})",
        "created_by": created_by,
        "entries": [
            {"ledger_id": resolved["data"]["id"], "entry_type": "dr", "amount": amount},
            transporter_entry,
        ],
    })


def give_payment(ledger_id: str, data: dict) -> dict:
    """
    Money GIVEN TO this transporter (e.g. an advance).
    data = { branch_id, amount, payment_mode: 'cash'|'bank', bank_ledger_id?,
              date?, narration?, created_by }
    Creates: Dr this transporter (advance)  /  Cr <resolved Cash/Bank>.
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

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "payment",
        "voucher_date": data.get("date") or today_ist(),
        "narration": data.get("narration") or f"Payment given ({payment_mode})",
        "created_by": created_by,
        "entries": [
            {"ledger_id": ledger_id, "entry_type": "dr", "amount": amount, "bill_allocation_type": "advance"},
            {"ledger_id": resolved["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })
