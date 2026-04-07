"""
Challan Service
Create challans, list, dispatch, hub receipt.
Auto-generates challan numbers from challan_books.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

CHALLAN_COLS = (
    "id, challan_no, branch_id, truck_id, owner_id, driver_id, "
    "date, total_bilty_count, remarks, is_active, "
    "is_dispatched, dispatch_date, "
    "is_received_at_hub, received_at_hub_timing, received_by_user, "
    "created_by, created_at, updated_at"
)

PAGE_SIZE = 40


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── helpers ───────────────────────────────────────────────────

def _generate_challan_no(book: dict) -> str:
    """Generate next challan number from a challan book."""
    prefix = book.get("prefix") or ""
    postfix = book.get("postfix") or ""
    digits = book.get("digits", 4)
    current = book.get("current_number", 1)
    return f"{prefix}{str(current).zfill(digits)}{postfix}"


def _resolve_names(rows: list) -> list:
    """Resolve truck_id→truck_number, driver_id→name, owner_id→name, created_by→name."""
    if not rows:
        return rows

    truck_ids = set()
    staff_ids = set()
    user_ids = set()
    for r in rows:
        if r.get("truck_id"):
            truck_ids.add(r["truck_id"])
        if r.get("driver_id"):
            staff_ids.add(r["driver_id"])
        if r.get("owner_id"):
            staff_ids.add(r["owner_id"])
        if r.get("created_by") and isinstance(r["created_by"], str) and len(r["created_by"]) > 20:
            user_ids.add(r["created_by"])

    sb = get_supabase()
    truck_map = {}
    staff_map = {}
    user_map = {}

    try:
        if truck_ids:
            resp = sb.table("trucks").select("id, truck_number").in_("id", list(truck_ids)).execute()
            truck_map = {t["id"]: t["truck_number"] for t in (resp.data or [])}
        if staff_ids:
            resp = sb.table("staff").select("id, name").in_("id", list(staff_ids)).execute()
            staff_map = {s["id"]: s["name"] or s["id"] for s in (resp.data or [])}
        if user_ids:
            resp = sb.table("users").select("id, name").in_("id", list(user_ids)).execute()
            user_map = {u["id"]: u["name"] or u["id"] for u in (resp.data or [])}
    except Exception:
        pass

    for r in rows:
        r["truck_number"] = truck_map.get(r.get("truck_id"))
        r["driver_name"] = staff_map.get(r.get("driver_id"))
        r["owner_name"] = staff_map.get(r.get("owner_id"))
        if r.get("created_by") and r["created_by"] in user_map:
            r["created_by"] = user_map[r["created_by"]]

    return rows


# ── LIST CHALLANS ─────────────────────────────────────────────

def list_challans(branch_id: str = None, is_dispatched: bool = None,
                  page: int = 1, page_size: int = PAGE_SIZE,
                  search: str = None) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("challan_details").select(CHALLAN_COLS, count="exact")

        if branch_id:
            q = q.eq("branch_id", branch_id)
        if is_dispatched is not None:
            q = q.eq("is_dispatched", is_dispatched)
        q = q.eq("is_active", True)

        if search:
            q = q.or_(f"challan_no.ilike.%{search}%")

        offset = (page - 1) * page_size
        q = q.order("created_at", desc=True).range(offset, offset + page_size - 1)
        resp = q.execute()
        rows = _resolve_names(resp.data or [])
        total = resp.count if resp.count is not None else len(rows)

        return {
            "status": "success",
            "data": {"rows": rows, "page": page, "page_size": page_size,
                     "total": total, "has_more": (offset + page_size) < total},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── GET ONE ───────────────────────────────────────────────────

def get_challan(challan_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("challan_details").select(CHALLAN_COLS).eq("id", challan_id).single().execute()
        row = resp.data
        if row:
            _resolve_names([row])
        return {"status": "success", "data": row}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CREATE CHALLAN ────────────────────────────────────────────

def create_challan(data: dict) -> dict:
    """
    Create a new challan from a challan book.
    Required: challan_book_id, branch_id, created_by
    Optional: truck_id, driver_id, owner_id, date, remarks
    """
    try:
        sb = get_supabase()
        book_id = data.get("challan_book_id")
        if not book_id:
            return {"status": "error", "message": "challan_book_id is required", "status_code": 400}

        required = ["branch_id", "created_by"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return {"status": "error", "message": f"Missing fields: {', '.join(missing)}", "status_code": 400}

        # 1. Fetch and validate challan book
        book_resp = sb.table("challan_books").select("*").eq("id", book_id).single().execute()
        book = book_resp.data
        if not book:
            return {"status": "error", "message": "Challan book not found", "status_code": 404}
        if not book.get("is_active"):
            return {"status": "error", "message": "Challan book is not active", "status_code": 400}
        if book.get("is_completed"):
            return {"status": "error", "message": "Challan book is completed (range exhausted)", "status_code": 400}
        if book["current_number"] > book["to_number"]:
            return {"status": "error", "message": "Challan book range exhausted", "status_code": 400}

        # 2. Generate challan number
        challan_no = _generate_challan_no(book)

        # 3. Insert challan
        now = _now()
        challan_data = {
            "challan_no": challan_no,
            "branch_id": data["branch_id"],
            "truck_id": data.get("truck_id"),
            "owner_id": data.get("owner_id"),
            "driver_id": data.get("driver_id"),
            "date": data.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
            "total_bilty_count": 0,
            "remarks": data.get("remarks"),
            "is_active": True,
            "is_dispatched": False,
            "created_by": data["created_by"],
            "created_at": now,
            "updated_at": now,
        }

        resp = sb.table("challan_details").insert(challan_data).execute()
        if not resp.data:
            return {"status": "error", "message": "Failed to create challan", "status_code": 500}

        # 4. Increment current_number on book
        next_num = book["current_number"] + 1
        book_update = {"current_number": next_num, "updated_at": now}
        if next_num > book["to_number"]:
            book_update["is_completed"] = True
        sb.table("challan_books").update(book_update).eq("id", book_id).execute()

        # 5. Resolve names for response
        row = resp.data[0]
        _resolve_names([row])

        return {"status": "success", "data": row, "message": f"Challan {challan_no} created"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── UPDATE CHALLAN ────────────────────────────────────────────

def update_challan(challan_id: str, data: dict) -> dict:
    """Update challan details (truck, driver, remarks). Cannot update if dispatched."""
    try:
        sb = get_supabase()

        # Check dispatch status
        existing = sb.table("challan_details").select("id, is_dispatched").eq("id", challan_id).single().execute()
        if not existing.data:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        if existing.data.get("is_dispatched"):
            return {"status": "error", "message": "Cannot update a dispatched challan", "status_code": 400}

        data.pop("id", None)
        data.pop("challan_no", None)  # don't allow changing number
        data.pop("is_dispatched", None)  # use dispatch endpoint
        data["updated_at"] = _now()

        resp = sb.table("challan_details").update(data).eq("id", challan_id).execute()
        if not resp.data:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        return {"status": "success", "data": resp.data[0], "message": "Challan updated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── DISPATCH ──────────────────────────────────────────────────

def dispatch_challan(challan_id: str, user_id: str = None) -> dict:
    """Mark challan as dispatched. Sets dispatch_date and locks it."""
    try:
        sb = get_supabase()

        existing = sb.table("challan_details").select("id, is_dispatched, total_bilty_count").eq("id", challan_id).single().execute()
        if not existing.data:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        if existing.data.get("is_dispatched"):
            return {"status": "error", "message": "Challan is already dispatched", "status_code": 400}
        if existing.data.get("total_bilty_count", 0) == 0:
            return {"status": "error", "message": "Cannot dispatch an empty challan (0 bilties)", "status_code": 400}

        now = _now()
        resp = sb.table("challan_details").update({
            "is_dispatched": True,
            "dispatch_date": now,
            "updated_at": now,
        }).eq("id", challan_id).execute()

        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Challan dispatched"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── UNDISPATCH (reopen) ──────────────────────────────────────

def undispatch_challan(challan_id: str) -> dict:
    """Reopen a dispatched challan (only if not received at hub yet)."""
    try:
        sb = get_supabase()
        existing = sb.table("challan_details").select(
            "id, is_dispatched, is_received_at_hub"
        ).eq("id", challan_id).single().execute()

        if not existing.data:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        if not existing.data.get("is_dispatched"):
            return {"status": "error", "message": "Challan is not dispatched", "status_code": 400}
        if existing.data.get("is_received_at_hub"):
            return {"status": "error", "message": "Cannot undispatch — already received at hub", "status_code": 400}

        resp = sb.table("challan_details").update({
            "is_dispatched": False,
            "dispatch_date": None,
            "updated_at": _now(),
        }).eq("id", challan_id).execute()

        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Challan reopened"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── HUB RECEIPT ───────────────────────────────────────────────

def mark_hub_received(challan_id: str, user_id: str = None) -> dict:
    """Mark challan as received at hub."""
    try:
        sb = get_supabase()
        existing = sb.table("challan_details").select(
            "id, is_dispatched, is_received_at_hub"
        ).eq("id", challan_id).single().execute()

        if not existing.data:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        if not existing.data.get("is_dispatched"):
            return {"status": "error", "message": "Challan must be dispatched first", "status_code": 400}
        if existing.data.get("is_received_at_hub"):
            return {"status": "error", "message": "Challan already received at hub", "status_code": 400}

        now = _now()
        resp = sb.table("challan_details").update({
            "is_received_at_hub": True,
            "received_at_hub_timing": now,
            "received_by_user": user_id,
            "updated_at": now,
        }).eq("id", challan_id).execute()

        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Challan marked as received at hub"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── DELETE (soft) ─────────────────────────────────────────────

def delete_challan(challan_id: str) -> dict:
    """Soft-delete a challan. Cannot delete if dispatched."""
    try:
        sb = get_supabase()
        existing = sb.table("challan_details").select("id, is_dispatched").eq("id", challan_id).single().execute()
        if not existing.data:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        if existing.data.get("is_dispatched"):
            return {"status": "error", "message": "Cannot delete a dispatched challan", "status_code": 400}

        # Remove any transit_details linked to this challan
        challan_resp = sb.table("challan_details").select("challan_no").eq("id", challan_id).single().execute()
        challan_no = challan_resp.data.get("challan_no") if challan_resp.data else None
        if challan_no:
            sb.table("transit_details").delete().eq("challan_no", challan_no).execute()

        resp = sb.table("challan_details").update({
            "is_active": False,
            "updated_at": _now(),
        }).eq("id", challan_id).execute()

        return {"status": "success", "message": "Challan deleted"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── INIT (combined page-load fetch) ──────────────────────────

def get_challan_init(branch_id: str) -> dict:
    """
    Single RPC replaces 3 API calls (init + list + available).
    Returns: branches, cities, permanent_details, challan_books,
             ALL challans (lightweight, name-resolved),
             available bilties (regular + station, ALL branches — no branch filter).
    Frontend only needs 1 more call: transit bilties for selected challan.
    """
    try:
        sb = get_supabase()

        rpc_resp = sb.rpc("get_challan_page_init", {"p_branch_id": branch_id}).execute()
        rpc_data = rpc_resp.data or {}

        branches = rpc_data.get("branches") or []
        user_branch = next((b for b in branches if b["id"] == branch_id), None)

        regular = rpc_data.get("available_regular") or []
        station = rpc_data.get("available_station") or []
        for r in regular:
            r["source_table"] = "bilty"
            r["bilty_type"] = "reg"
        for s in station:
            s["source_table"] = "station_bilty_summary"
            s["bilty_type"] = "mnl"

        return {
            "status": "success",
            "data": {
                "user_branch": user_branch,
                "branches": branches,
                "cities": rpc_data.get("cities") or [],
                "permanent_details": rpc_data.get("permanent_details") or [],
                "challan_books": rpc_data.get("challan_books") or [],
                "challans": rpc_data.get("challans") or [],
                "available_bilties": regular + station,
                "regular_count": len(regular),
                "station_count": len(station),
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
