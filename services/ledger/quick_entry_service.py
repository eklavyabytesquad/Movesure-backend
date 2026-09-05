"""
Quick Entry — simple Income/Expense recording, no Dr/Cr shown anywhere
========================================================================
For day-to-day cash/bank movements that aren't a transporter/driver/labour
bill (a delivery charge collected in cash against a GR, a small cash
expense), the frontend never needs to know a voucher's Dr/Cr shape, and it
never picks the Cash/Bank ledger itself — see ledger_helpers.resolve_payment_ledger.
"""
from datetime import date
from services.ledger.voucher_service import create_voucher
from services.ledger.ledger_helpers import resolve_payment_ledger, get_or_create_named_ledger

DELIVERY_INCOME_LEDGER = "Delivery Income"
DELIVERY_INCOME_GROUP = "Direct Incomes"


def record_income(data: dict) -> dict:
    """
    data = { branch_id, income_ledger_id, payment_mode: 'cash'|'bank',
              bank_ledger_id? (required if payment_mode='bank' and this
              branch has no default bank set), amount, reference_no?,
              narration?, date?, created_by }
    Creates: Dr <resolved Cash/Bank ledger>  /  Cr income_ledger_id
    """
    branch_id = data.get("branch_id")
    income_ledger_id = data.get("income_ledger_id")
    payment_mode = data.get("payment_mode")
    amount = data.get("amount")
    created_by = data.get("created_by")

    if not branch_id or not income_ledger_id or payment_mode not in ("cash", "bank") or not amount or float(amount) <= 0 or not created_by:
        return {"status": "error", "message": "branch_id, income_ledger_id, payment_mode ('cash'/'bank'), amount and created_by are required", "status_code": 400}

    resolved = resolve_payment_ledger(branch_id, payment_mode, data.get("bank_ledger_id"))
    if resolved["status"] != "success":
        return resolved

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
            {"ledger_id": resolved["data"]["id"], "entry_type": "dr", "amount": amount},
            {"ledger_id": income_ledger_id, "entry_type": "cr", "amount": amount},
        ],
    })


def record_expense(data: dict) -> dict:
    """
    data = { branch_id, expense_ledger_id, payment_mode: 'cash'|'bank',
              bank_ledger_id?, amount, reference_no?, narration?, date?, created_by }
    Creates: Dr expense_ledger_id  /  Cr <resolved Cash/Bank ledger>
    """
    branch_id = data.get("branch_id")
    expense_ledger_id = data.get("expense_ledger_id")
    payment_mode = data.get("payment_mode")
    amount = data.get("amount")
    created_by = data.get("created_by")

    if not branch_id or not expense_ledger_id or payment_mode not in ("cash", "bank") or not amount or float(amount) <= 0 or not created_by:
        return {"status": "error", "message": "branch_id, expense_ledger_id, payment_mode ('cash'/'bank'), amount and created_by are required", "status_code": 400}

    resolved = resolve_payment_ledger(branch_id, payment_mode, data.get("bank_ledger_id"))
    if resolved["status"] != "success":
        return resolved

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
            {"ledger_id": resolved["data"]["id"], "entry_type": "cr", "amount": amount},
        ],
    })


def record_delivery_income(data: dict) -> dict:
    """
    data = { branch_id, gr_no, amount, payment_mode, bank_ledger_id?, date?, created_by }
    Auto-resolves (creating if needed) this branch's 'Delivery Income'
    ledger, and uses gr_no as the reference — exactly
    "Ref GRNO 5142, Delivery Amount 300, cash".
    """
    branch_id = data.get("branch_id")
    delivery_income = get_or_create_named_ledger(branch_id, DELIVERY_INCOME_LEDGER, DELIVERY_INCOME_GROUP, data.get("created_by"))
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
    data = { branch_id, gr_no?, expense_ledger_id, amount, payment_mode, bank_ledger_id?, date?, created_by }
    Same as record_expense(), standardizes the GR-based reference — the
    expense category (fuel, labour, etc.) is still an explicit choice.
    """
    gr_no = data.get("gr_no")
    return record_expense({
        **data,
        "reference_no": data.get("reference_no") or gr_no,
        "narration": data.get("narration") or (f"Delivery expense - GR {gr_no}" if gr_no else None),
    })
