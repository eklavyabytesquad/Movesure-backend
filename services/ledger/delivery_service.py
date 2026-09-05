"""
Kanpur Delivery — read side
=============================
POST /api/ledger/delivery/income and /expense record entries, but there
was no GET to list what's actually in the database — the "Today's
entries" list on the frontend was just local browser state, gone on
refresh, not real data.

get_delivery_income() fixes that: it looks up the branch's real "Delivery
Income" ledger (the exact one every quick-entry income call posts to,
auto-created the first time income is added) and returns its balance +
statement — so the page shows what's really recorded, persists across
refreshes, and works from any browser/session.

Expenses are intentionally NOT unified here. Kanpur Delivery expenses
each go to whichever expense ledger the user picks (fuel, labour, misc —
see record_delivery_expense), not one fixed ledger, so there's no single
place to list them all from without faking a combined feed across
ledgers that don't actually share an identity. Point that view at Cash
Manager (every cash-side expense, from any screen) or Ledger Master (any
one specific expense ledger's own statement) instead.
"""
from datetime import date
from services.ledger.ledger_service import get_ledger_balance, get_ledger_statement
from services.ledger.ledger_helpers import resolve_branch_ledger_by_name

DELIVERY_INCOME_LEDGER_NAME = "Delivery Income"


def get_delivery_income(branch_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    """
    Balance + statement for this branch's Delivery Income ledger.
    Defaults the date range to today if not given. Returns a valid EMPTY
    shape (not an error) when this branch has never recorded delivery
    income yet — a brand-new branch's first visit here shouldn't error.
    """
    if not branch_id:
        return {"status": "error", "message": "branch_id is required", "status_code": 400}

    today = str(date.today())
    from_date = from_date or today
    to_date = to_date or today

    resolved = resolve_branch_ledger_by_name(branch_id, DELIVERY_INCOME_LEDGER_NAME)
    if resolved["status"] != "success":
        return {
            "status": "success",
            "data": {
                "ledger_id": None,
                "balance": 0, "balance_type": "dr",
                "from_date": from_date, "to_date": to_date,
                "entries": [], "total_income": 0,
            },
        }

    ledger_id = resolved["data"]["id"]
    balance = get_ledger_balance(ledger_id)["data"]
    statement = get_ledger_statement(ledger_id, from_date, to_date)["data"]
    entries = statement["entries"]

    # Delivery Income is an income ledger — every real income entry posts
    # a Cr here (Dr goes to Cash/Bank instead). Filtering on 'cr' guards
    # against ever double-counting if a correction/credit-note-style entry
    # (a 'dr' row) is ever posted against this ledger.
    total_income = round(sum(e["amount"] for e in entries if e["entry_type"] == "cr"), 2)

    return {
        "status": "success",
        "data": {
            "ledger_id": ledger_id,
            "balance": balance["balance"],
            "balance_type": balance["balance_type"],
            "from_date": from_date,
            "to_date": to_date,
            "entries": [
                {
                    "time": e["time"],
                    "voucher_no": e["voucher_no"],
                    "narration": e["narration"],
                    "amount": e["amount"],
                    "running_balance": e["running_balance"],
                    "running_balance_type": e["running_balance_type"],
                }
                for e in entries
            ],
            "total_income": total_income,
        },
    }
