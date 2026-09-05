"""
Kanpur Delivery — read side
=============================
Two canonical ledgers per branch, both under the shared 'Delivery' group
(same pattern as Transporters/Drivers/Labour): 'Delivery Income' and
'Delivery Expense'. Every POST /api/ledger/delivery/income or /expense
call posts to whichever one applies — always the same ledger, never a
picked-per-entry category — which is what makes a single combined page
possible: get_delivery_summary() merges both ledgers' statements into one
chronological, day-grouped feed, exactly like Cash Manager but scoped to
delivery instead of cash.

get_delivery_income() / get_delivery_expense() remain as single-ledger
views if you only need one side.
"""
from services.ledger.ledger_service import get_ledger_balance, get_ledger_statement
from services.ledger.ledger_helpers import resolve_branch_ledger_by_name, today_ist

DELIVERY_INCOME_LEDGER_NAME = "Delivery Income"
DELIVERY_EXPENSE_LEDGER_NAME = "Delivery Expense"


def _empty_ledger_view(from_date: str, to_date: str) -> dict:
    return {"ledger_id": None, "balance": 0, "balance_type": "dr",
            "from_date": from_date, "to_date": to_date, "entries": [], "total": 0}


def _ledger_view(branch_id: str, ledger_name: str, from_date: str, to_date: str, amount_side: str) -> dict:
    """amount_side = 'cr' for income ledgers, 'dr' for expense ledgers —
    which entry_type actually represents a real transaction on that ledger
    (the other side only ever appears for corrections)."""
    resolved = resolve_branch_ledger_by_name(branch_id, ledger_name)
    if resolved["status"] != "success":
        return _empty_ledger_view(from_date, to_date)

    ledger_id = resolved["data"]["id"]
    balance = get_ledger_balance(ledger_id)["data"]
    statement = get_ledger_statement(ledger_id, from_date, to_date)["data"]
    entries = statement["entries"]
    total = round(sum(e["amount"] for e in entries if e["entry_type"] == amount_side), 2)

    return {
        "ledger_id": ledger_id,
        "balance": balance["balance"],
        "balance_type": balance["balance_type"],
        "from_date": from_date,
        "to_date": to_date,
        "entries": [
            {
                "time": e["time"], "voucher_date": e["voucher_date"], "voucher_no": e["voucher_no"],
                "narration": e["narration"], "amount": e["amount"],
            }
            for e in entries
        ],
        "total": total,
    }


def get_delivery_income(branch_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    if not branch_id:
        return {"status": "error", "message": "branch_id is required", "status_code": 400}
    today = today_ist()
    view = _ledger_view(branch_id, DELIVERY_INCOME_LEDGER_NAME, from_date or today, to_date or today, "cr")
    view["total_income"] = view.pop("total")
    return {"status": "success", "data": view}


def get_delivery_expense(branch_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    if not branch_id:
        return {"status": "error", "message": "branch_id is required", "status_code": 400}
    today = today_ist()
    view = _ledger_view(branch_id, DELIVERY_EXPENSE_LEDGER_NAME, from_date or today, to_date or today, "dr")
    view["total_expense"] = view.pop("total")
    return {"status": "success", "data": view}


def get_delivery_summary(branch_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    """
    THE combined "Kanpur Delivery" page: every income AND expense entry,
    merged into one chronological feed, grouped by day — same shape as
    Cash Manager's day view, so the frontend can reuse that rendering.
    """
    if not branch_id:
        return {"status": "error", "message": "branch_id is required", "status_code": 400}

    today = today_ist()
    from_date = from_date or today
    to_date = to_date or today

    income = get_delivery_income(branch_id, from_date, to_date)["data"]
    expense = get_delivery_expense(branch_id, from_date, to_date)["data"]

    merged = (
        [{"kind": "income", **e} for e in income["entries"]]
        + [{"kind": "expense", **e} for e in expense["entries"]]
    )
    merged.sort(key=lambda r: r["time"])

    # Group by voucher_date (the correct, IST-aware calendar day), never by
    # slicing the raw UTC timestamp — an entry made at 2am IST has a UTC
    # timestamp still dated the day before, which would misfile it here
    # exactly like the bug that caused voucher_date itself to be wrong.
    days: dict[str, list[dict]] = {}
    for r in merged:
        days.setdefault(r["voucher_date"], []).append(r)

    day_rows = []
    running_net = 0.0
    for day in sorted(days.keys()):
        rows = days[day]
        day_income = round(sum(r["amount"] for r in rows if r["kind"] == "income"), 2)
        day_expense = round(sum(r["amount"] for r in rows if r["kind"] == "expense"), 2)
        opening_net = round(running_net, 2)
        running_net += day_income - day_expense
        day_rows.append({
            "date": day,
            "opening_net": opening_net,
            "closing_net": round(running_net, 2),
            "total_income": day_income,
            "total_expense": day_expense,
            "net": round(day_income - day_expense, 2),
            "entries": rows,
        })

    # All-time net (not just this period) — income is normally 'cr',
    # expense is normally 'dr'; convert both to the same sign so they're
    # directly comparable regardless of which side either currently sits on.
    income_signed = income["balance"] if income["balance_type"] == "cr" else -income["balance"]
    expense_signed = expense["balance"] if expense["balance_type"] == "dr" else -expense["balance"]

    return {
        "status": "success",
        "data": {
            "income_ledger_id": income["ledger_id"],
            "expense_ledger_id": expense["ledger_id"],
            "from_date": from_date,
            "to_date": to_date,
            "period_total_income": income["total_income"],
            "period_total_expense": expense["total_expense"],
            "period_net": round(income["total_income"] - expense["total_expense"], 2),
            "all_time_net": round(income_signed - expense_signed, 2),
            "days": day_rows,
        },
    }
