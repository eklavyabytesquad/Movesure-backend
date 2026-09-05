"""
CASH MANAGER (GALLA)
======================
One page for this branch's physical cash box. Every quick-entry screen
that touches cash (Transport PF Collection, Kanpur Delivery, Truck Bhada,
Labour Kharcha — anything with payment_mode='cash') posts to the same
underlying Cash-in-Hand ledger, so this page just shows that ledger's
balance and statement — nothing new is written from here except a plain
cash expense.

Grouped day-by-day, each day showing its own opening balance, every entry
(labelled plainly as "in" or "out", never Dr/Cr, with the exact time it
happened), and its closing balance — so "what came in, what went out,
what's left" is visible at a glance instead of buried in a flat list.
"""
from services.ledger.ledger_service import get_ledger_balance, get_ledger_statement
from services.ledger.ledger_helpers import resolve_branch_ledger, CASH_GROUP_NAME
from services.ledger.quick_entry_service import record_expense


def _signed(amount: float, side: str) -> float:
    return amount if side == "dr" else -amount


def _split_signed(signed: float) -> tuple[float, str]:
    return (round(abs(signed), 2), "dr" if signed >= 0 else "cr")


def _group_by_day(entries: list[dict]) -> list[dict]:
    """entries are already chronologically ordered (see get_ledger_statement).
    For Cash-in-Hand, dr = money IN, cr = money OUT — that's the whole
    translation needed to talk about this in plain 'income/expense' terms."""
    days: dict[str, list[dict]] = {}
    for e in entries:
        days.setdefault(e["voucher_date"], []).append(e)

    out = []
    for day in sorted(days.keys()):
        rows = days[day]

        first = rows[0]
        first_after = _signed(first["running_balance"], first["running_balance_type"])
        first_delta = _signed(first["amount"], first["entry_type"])
        opening_amount, opening_type = _split_signed(first_after - first_delta)

        last = rows[-1]

        total_in = round(sum(r["amount"] for r in rows if r["entry_type"] == "dr"), 2)
        total_out = round(sum(r["amount"] for r in rows if r["entry_type"] == "cr"), 2)

        out.append({
            "date": day,
            "opening_balance": opening_amount,
            "opening_balance_type": opening_type,
            "closing_balance": last["running_balance"],
            "closing_balance_type": last["running_balance_type"],
            "total_in": total_in,
            "total_out": total_out,
            "entries": [
                {
                    "time": r["time"],
                    "voucher_no": r["voucher_no"],
                    "voucher_type": r["voucher_type"],
                    "narration": r["narration"],
                    "direction": "in" if r["entry_type"] == "dr" else "out",
                    "amount": r["amount"],
                    "running_balance": r["running_balance"],
                    "running_balance_type": r["running_balance_type"],
                }
                for r in rows
            ],
        })
    return out


def get_cash_manager(branch_id: str, from_date: str | None = None, to_date: str | None = None) -> dict:
    """Current balance + a day-by-day breakdown (opening, every entry with
    its time and plain in/out direction, closing) for this branch's
    Cash-in-Hand ledger — every income/expense that ever touched cash,
    regardless of which screen created it."""
    resolved = resolve_branch_ledger(branch_id, CASH_GROUP_NAME)
    if resolved["status"] != "success":
        return resolved
    ledger_id = resolved["data"]["id"]

    balance = get_ledger_balance(ledger_id)["data"]
    statement = get_ledger_statement(ledger_id, from_date, to_date)["data"]
    days = _group_by_day(statement["entries"])

    return {
        "status": "success",
        "data": {
            "ledger_id": ledger_id,
            "name": resolved["data"]["name"],
            "current_balance": balance["balance"],
            "current_balance_type": balance["balance_type"],
            "period_opening_balance": statement["opening_balance"],
            "period_opening_balance_type": statement["opening_balance_type"],
            "period_closing_balance": statement["closing_balance"],
            "period_closing_balance_type": statement["closing_balance_type"],
            "days": days,
        },
    }


def add_cash_expense(data: dict) -> dict:
    """
    data = { branch_id, expense_ledger_id, amount, reference_no?, narration?, date?, created_by }
    A plain cash-out expense from the Galla — thin wrapper over
    record_expense() with payment_mode pinned to 'cash'.
    """
    return record_expense({**data, "payment_mode": "cash"})
