"""
CASH MANAGER (GALLA)
======================
One page for this branch's physical cash box. Every quick-entry screen
that touches cash (Transport PF Collection, Kanpur Delivery, Truck Bhada,
Labour Kharcha — anything with payment_mode='cash') posts to the same
underlying Cash-in-Hand ledger, so this page just shows that ledger's
balance and statement — nothing new is written from here except a plain
cash expense.
"""
from services.ledger.ledger_service import get_ledger_balance, get_ledger_statement
from services.ledger.ledger_helpers import resolve_branch_ledger, CASH_GROUP_NAME
from services.ledger.quick_entry_service import record_expense


def get_cash_manager(branch_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    """Balance + full statement for this branch's Cash-in-Hand ledger,
    in one call — every income/expense that ever touched cash shows up
    here regardless of which screen created it."""
    resolved = resolve_branch_ledger(branch_id, CASH_GROUP_NAME)
    if resolved["status"] != "success":
        return resolved
    ledger_id = resolved["data"]["id"]

    balance = get_ledger_balance(ledger_id)["data"]
    statement = get_ledger_statement(ledger_id, from_date, to_date)["data"]

    return {
        "status": "success",
        "data": {
            "ledger_id": ledger_id,
            "name": resolved["data"]["name"],
            "balance": balance["balance"],
            "balance_type": balance["balance_type"],
            "opening_balance": statement["opening_balance"],
            "opening_balance_type": statement["opening_balance_type"],
            "entries": statement["entries"],
            "closing_balance": statement["closing_balance"],
            "closing_balance_type": statement["closing_balance_type"],
        },
    }


def add_cash_expense(data: dict) -> dict:
    """
    data = { branch_id, expense_ledger_id, amount, reference_no?, narration?, date?, created_by }
    A plain cash-out expense from the Galla — thin wrapper over
    record_expense() with payment_mode pinned to 'cash'.
    """
    return record_expense({**data, "payment_mode": "cash"})
