"""
Shared plumbing for the "smart" ledger screens
================================================
Transport PF Collection, Kanpur Delivery, Cash Manager, Truck Bhada, and
Labour Kharcha all need the same few things: find this branch's Cash
ledger, find or create a well-known named ledger (PF Income, Delivery
Income, Truck Bhada Expense, Labour Kharcha Expense), find or create a
party group (Transporters, Drivers, Labour), and — since a branch can now
have SEVERAL bank ledgers — resolve which one to use for a "bank" payment.

This lives here ONCE. Copying this logic into every screen's service file
is exactly what caused the original "No Bank Account ledger found" bug.
"""
from datetime import datetime, timezone, timedelta
from services.supabase_client import get_supabase
from services.ledger.group_service import create_group
from services.ledger.ledger_service import create_ledger

CASH_GROUP_NAME = "Cash-in-Hand"
BANK_GROUP_NAME = "Bank Accounts"

IST = timezone(timedelta(hours=5, minutes=30))


def today_ist() -> str:
    """'Today', in India time — NOT the server's local clock.

    Every voucher-creating function in this package defaults its date to
    'today' when the caller doesn't pass one explicitly. Plain
    `date.today()` uses whatever timezone the SERVER process happens to
    run in — commonly UTC in a cloud deployment — which is up to a full
    calendar day behind India (UTC+5:30) for roughly 5.5 hours every
    single day (midnight-to-5:30am IST). A real entry made at
    "2026-09-06 02:02 IST" was getting saved as voucher_date
    "2026-09-05" because the server's UTC clock still read the 5th —
    correct 'now', wrong calendar day for an India-run business. This is
    a safety net; the frontend should still send an explicit `date`
    (computed from the browser's LOCAL clock) wherever it can, since only
    the client actually knows the user's timezone for certain.
    """
    return datetime.now(IST).date().isoformat()


def resolve_branch_ledger(branch_id: str, group_name: str) -> dict:
    """The one active ledger for this branch under `group_name`. Only use
    this for groups meant to hold exactly ONE ledger per branch
    (Cash-in-Hand). NOT for Bank Accounts anymore — see resolve_payment_ledger."""
    sb = get_supabase()
    group = sb.table("ledger_groups").select("id").eq("name", group_name).execute().data
    if not group:
        return {"status": "error", "message": f"No '{group_name}' group exists in the chart of accounts", "status_code": 404}
    group_id = group[0]["id"]

    ledgers = (
        sb.table("ledgers")
        .select("id, name")
        .eq("branch_id", branch_id)
        .eq("group_id", group_id)
        .eq("is_active", True)
        .execute()
        .data or []
    )
    if not ledgers:
        return {"status": "error", "message": f"No {group_name} ledger found for this branch. Create one in Ledger Master first.", "status_code": 404}
    if len(ledgers) > 1:
        names = ", ".join(l["name"] for l in ledgers)
        return {"status": "error", "message": f"This branch has more than one {group_name} ledger ({names}) — pick one explicitly", "status_code": 409}
    return {"status": "success", "data": ledgers[0]}


def resolve_payment_ledger(branch_id: str, payment_mode: str, bank_ledger_id: str | None = None) -> dict:
    """
    payment_mode='cash' -> this branch's one Cash-in-Hand ledger.
    payment_mode='bank' -> bank_ledger_id if given (validated to be an
                           active Bank Accounts ledger for this branch),
                           else this branch's DEFAULT bank ledger
                           (ledgers.is_default = true).
    A branch can have several banks — this is the one place that
    ambiguity gets resolved, so no screen has to guess or hardcode one.
    """
    if payment_mode not in ("cash", "bank"):
        return {"status": "error", "message": "payment_mode must be 'cash' or 'bank'", "status_code": 400}
    if payment_mode == "cash":
        return resolve_branch_ledger(branch_id, CASH_GROUP_NAME)

    sb = get_supabase()
    bank_group = sb.table("ledger_groups").select("id").eq("name", BANK_GROUP_NAME).execute().data
    if not bank_group:
        return {"status": "error", "message": f"No '{BANK_GROUP_NAME}' group exists in the chart of accounts", "status_code": 404}
    bank_group_id = bank_group[0]["id"]

    if bank_ledger_id:
        row = sb.table("ledgers").select("id, name, branch_id, group_id, is_active").eq("id", bank_ledger_id).execute().data
        if not row or not row[0]["is_active"] or row[0]["branch_id"] != branch_id or row[0]["group_id"] != bank_group_id:
            return {"status": "error", "message": "bank_ledger_id is not a valid, active Bank Accounts ledger for this branch", "status_code": 400}
        return {"status": "success", "data": {"id": row[0]["id"], "name": row[0]["name"]}}

    default_row = (
        sb.table("ledgers")
        .select("id, name")
        .eq("branch_id", branch_id).eq("group_id", bank_group_id)
        .eq("is_active", True).eq("is_default", True)
        .execute().data
    )
    if not default_row:
        return {
            "status": "error",
            "message": "No default Bank Accounts ledger is set for this branch — pass bank_ledger_id explicitly, or mark one bank ledger as default via POST /api/ledger/banks/{id}/set-default",
            "status_code": 404,
        }
    return {"status": "success", "data": default_row[0]}


def resolve_branch_ledger_by_name(branch_id: str, name: str) -> dict:
    sb = get_supabase()
    rows = (
        sb.table("ledgers").select("id, name")
        .eq("branch_id", branch_id).ilike("name", name).eq("is_active", True)
        .execute().data or []
    )
    if not rows:
        return {"status": "error", "message": f"No '{name}' ledger found for this branch.", "status_code": 404}
    return {"status": "success", "data": rows[0]}


def get_or_create_named_ledger(branch_id: str, name: str, group_name: str, created_by: str | None = None) -> dict:
    """Find the one ledger named `name` for this branch; auto-create it
    under `group_name` if missing — for well-known, one-per-branch ledgers
    like 'PF Income', 'Delivery Income', 'Truck Bhada Expense', 'Labour
    Kharcha Expense', so a branch never needs manual setup before using
    these screens the first time."""
    existing = resolve_branch_ledger_by_name(branch_id, name)
    if existing["status"] == "success":
        return existing

    sb = get_supabase()
    group = sb.table("ledger_groups").select("id").eq("name", group_name).execute().data
    if not group:
        return {"status": "error", "message": f"No '{group_name}' group exists in the chart of accounts", "status_code": 404}

    created = create_ledger({
        "branch_id": branch_id, "name": name, "group_id": group[0]["id"],
        "is_bill_wise": False, "created_by": created_by,
    }, created_by)
    if created["status"] != "success":
        return created
    return {"status": "success", "data": created["data"]}


def get_or_create_group(group_name: str, parent_group_name: str | None, nature: str) -> str | None:
    """Find a group by name; auto-create it (under parent_group_name, if
    given) if missing — for party groups like 'Transporters', 'Drivers',
    'Labour'. Groups are global (not branch-scoped), so this only ever
    creates one, the first time any branch needs it."""
    sb = get_supabase()
    existing = sb.table("ledger_groups").select("id").eq("name", group_name).execute().data
    if existing:
        return existing[0]["id"]
    parent_id = None
    if parent_group_name:
        parent = sb.table("ledger_groups").select("id").eq("name", parent_group_name).execute().data
        parent_id = parent[0]["id"] if parent else None
    created = create_group({"name": group_name, "parent_group_id": parent_id, "nature": nature})
    return created["data"]["id"] if created["status"] == "success" else None
