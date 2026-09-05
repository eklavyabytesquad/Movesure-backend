"""
Transporter Ledger — a single-page-friendly wrapper
=====================================================
Purpose-built for exactly the workflow you asked for: add a transporter,
raise their PF bill, click their name to see everything about them at
once, record a payment against a bill. Every function here is a thin
wrapper around the generic group/ledger/voucher services — it just saves
the frontend from having to know Dr/Cr, group ids, or how to find "this
branch's PF Income ledger" itself.

All transporters live under one shared "Transporters" group (auto-created
under Sundry Debtors the first time it's needed) — this group is the
SAME one used by list_transporters/create_transporter no matter which
branch calls them, so a transporter always shows up correctly regardless
of which branch's UI is asking.
"""
from datetime import date
from services.supabase_client import get_supabase
from services.ledger.group_service import create_group
from services.ledger.ledger_service import create_ledger, get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.bill_reference_service import list_bills
from services.ledger.voucher_service import create_voucher
from services.ledger.quick_entry_service import resolve_branch_ledger, resolve_branch_ledger_by_name

TRANSPORTERS_GROUP_NAME = "Transporters"
SUNDRY_DEBTORS_GROUP_NAME = "Sundry Debtors"
PF_INCOME_LEDGER_NAME = "PF Income"


def _get_or_create_transporters_group() -> str | None:
    sb = get_supabase()
    existing = sb.table("ledger_groups").select("id").eq("name", TRANSPORTERS_GROUP_NAME).execute().data
    if existing:
        return existing[0]["id"]

    parent = sb.table("ledger_groups").select("id").eq("name", SUNDRY_DEBTORS_GROUP_NAME).execute().data
    parent_id = parent[0]["id"] if parent else None
    created = create_group({"name": TRANSPORTERS_GROUP_NAME, "parent_group_id": parent_id, "nature": "asset"})
    return created["data"]["id"] if created["status"] == "success" else None


def list_transporters(branch_id: str) -> dict:
    """Every transporter ledger for this branch, each with its live balance
    already attached — enough to render the whole list page in one call."""
    group_id = _get_or_create_transporters_group()
    if not group_id:
        return {"status": "error", "message": "Could not resolve the Transporters group", "status_code": 500}

    sb = get_supabase()
    rows = (
        sb.table("ledgers")
        .select("id, name, gstin, phone, is_active")
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


def create_transporter(data: dict) -> dict:
    """data = { branch_id, name, gstin?, phone?, address?, opening_balance?, opening_balance_type?, created_by }"""
    branch_id = data.get("branch_id")
    name = (data.get("name") or "").strip()
    created_by = data.get("created_by")
    if not branch_id or not name or not created_by:
        return {"status": "error", "message": "branch_id, name and created_by are required", "status_code": 400}

    group_id = _get_or_create_transporters_group()
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
    page, in one call: the ledger record, its balance, every bill
    (paid and unpaid), and its full transaction statement."""
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

    pf_income = resolve_branch_ledger_by_name(branch_id, PF_INCOME_LEDGER_NAME)
    if pf_income["status"] != "success":
        return pf_income

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "sales",
        "voucher_date": data.get("date") or str(date.today()),
        "narration": data.get("narration") or f"PF settlement bill {reference_no}",
        "created_by": created_by,
        "entries": [
            {"ledger_id": ledger_id, "entry_type": "dr", "amount": amount,
             "bill_allocation_type": "new_ref",
             "new_bill": {"reference_no": reference_no, "due_date": data.get("due_date")}},
            {"ledger_id": pf_income["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })


def record_payment(ledger_id: str, data: dict) -> dict:
    """
    data = { branch_id, amount, cash_or_bank: 'cash'|'bank', bill_reference_id, date?, narration?, created_by }
    Creates: Dr this branch's Cash or Bank ledger  /  Cr this transporter (settles that bill).
    """
    branch_id = data.get("branch_id")
    amount = data.get("amount")
    cash_or_bank = data.get("cash_or_bank")
    bill_reference_id = data.get("bill_reference_id")
    created_by = data.get("created_by")
    if not branch_id or not amount or cash_or_bank not in ("cash", "bank") or not bill_reference_id or not created_by:
        return {"status": "error", "message": "branch_id, amount, cash_or_bank ('cash'/'bank'), bill_reference_id and created_by are required", "status_code": 400}

    resolved = resolve_branch_ledger(branch_id, "Cash-in-Hand" if cash_or_bank == "cash" else "Bank Accounts")
    if resolved["status"] != "success":
        return resolved

    return create_voucher({
        "branch_id": branch_id,
        "voucher_type": "receipt",
        "voucher_date": data.get("date") or str(date.today()),
        "narration": data.get("narration") or f"Payment received ({cash_or_bank})",
        "created_by": created_by,
        "entries": [
            {"ledger_id": resolved["data"]["id"], "entry_type": "dr", "amount": amount},
            {"ledger_id": ledger_id, "entry_type": "cr", "amount": amount,
             "bill_allocation_type": "agst_ref", "bill_reference_id": bill_reference_id},
        ],
    })
