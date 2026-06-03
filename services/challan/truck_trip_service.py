"""
Truck Trip Service
Manages truck_trips — one record per physical truck movement.
One trip can carry multiple challan numbers.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

TRIP_COLS = (
    "id, trip_no, truck_id, driver_id, owner_id, branch_id, "
    "dispatch_date, received_date, received_by, "
    "status, total_challan_count, remarks, "
    "created_by, created_at, updated_at, is_active"
)

CHALLAN_COLS = (
    "id, challan_no, branch_id, truck_id, driver_id, owner_id, date, "
    "total_bilty_count, is_dispatched, dispatch_date, "
    "is_received_at_hub, received_at_hub_timing, truck_trip_id"
)

PAGE_SIZE = 40


def _now():
    return datetime.now(timezone.utc).isoformat()


def _resolve_names(rows: list) -> list:
    if not rows:
        return rows
    sb = get_supabase()
    truck_ids = {r["truck_id"] for r in rows if r.get("truck_id")}
    staff_ids = {r.get("driver_id") for r in rows} | {r.get("owner_id") for r in rows}
    staff_ids.discard(None)
    user_ids = {r.get("created_by") for r in rows} | {r.get("received_by") for r in rows}
    user_ids.discard(None)

    truck_map = staff_map = user_map = {}
    try:
        if truck_ids:
            res = sb.table("trucks").select("id, truck_number").in_("id", list(truck_ids)).execute()
            truck_map = {t["id"]: t["truck_number"] for t in (res.data or [])}
        if staff_ids:
            res = sb.table("staff").select("id, name").in_("id", list(staff_ids)).execute()
            staff_map = {s["id"]: s["name"] for s in (res.data or [])}
        if user_ids:
            res = sb.table("users").select("id, name").in_("id", list(user_ids)).execute()
            user_map = {u["id"]: u["name"] for u in (res.data or [])}
    except Exception:
        pass

    for r in rows:
        r["truck_number"] = truck_map.get(r.get("truck_id"))
        r["driver_name"]  = staff_map.get(r.get("driver_id"))
        r["owner_name"]   = staff_map.get(r.get("owner_id"))
        r["received_by_name"] = user_map.get(r.get("received_by"))
    return rows


# ── LIST ──────────────────────────────────────────────────────────────────────

def list_trips(
    branch_id: str = None,
    truck_id: str = None,
    status: str = None,
    page: int = 1,
    page_size: int = PAGE_SIZE,
    search: str = None,
) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("truck_trips").select(TRIP_COLS, count="exact").eq("is_active", True)

        if branch_id:
            q = q.eq("branch_id", branch_id)
        if truck_id:
            q = q.eq("truck_id", truck_id)
        if status:
            q = q.eq("status", status)
        if search:
            q = q.ilike("trip_no", f"%{search}%")

        offset = (page - 1) * page_size
        q = q.order("dispatch_date", desc=True).range(offset, offset + page_size - 1)
        resp = q.execute()
        rows = _resolve_names(resp.data or [])
        total = resp.count if resp.count is not None else len(rows)

        return {
            "status": "success",
            "data": {
                "rows": rows,
                "page": page,
                "page_size": page_size,
                "total": total,
                "has_more": (offset + page_size) < total,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── GET ONE (with its challans) ───────────────────────────────────────────────

def get_trip(trip_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("truck_trips").select(TRIP_COLS).eq("id", trip_id).single().execute()
        row = resp.data
        if not row:
            return {"status": "error", "message": "Trip not found", "status_code": 404}

        _resolve_names([row])

        # Fetch linked challans
        challans_resp = (
            sb.table("challan_details")
            .select(CHALLAN_COLS)
            .eq("truck_trip_id", trip_id)
            .eq("is_active", True)
            .order("date")
            .execute()
        )
        row["challans"] = challans_resp.data or []
        row["challan_nos"] = [c["challan_no"] for c in row["challans"]]

        return {"status": "success", "data": row}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CREATE TRIP ────────────────────────────────────────────────────────────────

def create_trip(data: dict) -> dict:
    """
    Create a new truck trip. Optionally links existing challans immediately.

    Required: truck_id, created_by
    Optional: driver_id, owner_id, branch_id, remarks
              challan_ids  list[uuid]  — challans to link on creation
    """
    try:
        sb = get_supabase()

        required = ["truck_id", "created_by"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return {"status": "error",
                    "message": f"Missing fields: {', '.join(missing)}", "status_code": 400}

        # Generate trip_no: TR-YYYYMMDD-seq
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        prefix = f"TR-{today}-"
        existing = (
            sb.table("truck_trips")
            .select("trip_no")
            .ilike("trip_no", f"{prefix}%")
            .order("trip_no", desc=True)
            .limit(1)
            .execute()
        )
        last_seq = 0
        if existing.data:
            try:
                last_seq = int(existing.data[0]["trip_no"].split("-")[-1])
            except (ValueError, IndexError):
                last_seq = 0
        trip_no = f"{prefix}{str(last_seq + 1).zfill(4)}"

        record = {
            "trip_no":    trip_no,
            "truck_id":   data["truck_id"],
            "driver_id":  data.get("driver_id"),
            "owner_id":   data.get("owner_id"),
            "branch_id":  data.get("branch_id"),
            "status":     "pending",
            "remarks":    data.get("remarks"),
            "created_by": data["created_by"],
            "is_active":  True,
            "total_challan_count": 0,
        }

        resp = sb.table("truck_trips").insert(record).execute()
        trip = (resp.data or [{}])[0]
        if not trip:
            return {"status": "error", "message": "Insert failed", "status_code": 500}

        trip_id = trip["id"]

        # Link provided challans
        challan_ids = data.get("challan_ids") or []
        if challan_ids:
            result = _link_challans(sb, trip_id, challan_ids)
            if result.get("status") == "error":
                return result
            trip["total_challan_count"] = result["linked_count"]

        _resolve_names([trip])
        return {"status": "success", "data": trip,
                "message": f"Trip {trip_no} created"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── LINK CHALLANS TO TRIP ─────────────────────────────────────────────────────

def link_challans(trip_id: str, challan_ids: list, user_id: str = None) -> dict:
    """Add one or more challans to an existing trip."""
    try:
        sb = get_supabase()

        trip = sb.table("truck_trips").select("id, status, is_active").eq("id", trip_id).single().execute().data
        if not trip:
            return {"status": "error", "message": "Trip not found", "status_code": 404}
        if not trip.get("is_active"):
            return {"status": "error", "message": "Trip is inactive", "status_code": 400}
        if trip.get("status") == "received":
            return {"status": "error", "message": "Cannot add challans to a received trip", "status_code": 400}

        result = _link_challans(sb, trip_id, challan_ids)
        return result
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def _link_challans(sb, trip_id: str, challan_ids: list) -> dict:
    """Internal: link challans to trip, update count."""
    if not challan_ids:
        return {"status": "success", "linked_count": 0, "already_linked": []}

    # Fetch challans to validate
    challans = (
        sb.table("challan_details")
        .select("id, challan_no, truck_trip_id, is_active")
        .in_("id", challan_ids)
        .execute()
    ).data or []

    linked = []
    already = []
    not_found = list(set(challan_ids) - {c["id"] for c in challans})

    for c in challans:
        if not c.get("is_active"):
            continue
        if c.get("truck_trip_id") and c["truck_trip_id"] != trip_id:
            already.append({"challan_no": c["challan_no"], "existing_trip_id": c["truck_trip_id"]})
            continue
        sb.table("challan_details").update({"truck_trip_id": trip_id}).eq("id", c["id"]).execute()
        linked.append(c["challan_no"])

    # Update count on trip
    count_res = (
        sb.table("challan_details")
        .select("id", count="exact")
        .eq("truck_trip_id", trip_id)
        .eq("is_active", True)
        .execute()
    )
    new_count = count_res.count or 0
    sb.table("truck_trips").update({"total_challan_count": new_count}).eq("id", trip_id).execute()

    return {
        "status": "success",
        "linked_count": new_count,
        "newly_linked": linked,
        "already_in_another_trip": already,
        "not_found": not_found,
    }


# ── UNLINK CHALLAN FROM TRIP ──────────────────────────────────────────────────

def unlink_challan(trip_id: str, challan_id: str) -> dict:
    """Remove a challan from a trip (sets truck_trip_id = NULL)."""
    try:
        sb = get_supabase()

        trip = sb.table("truck_trips").select("id, status").eq("id", trip_id).single().execute().data
        if not trip:
            return {"status": "error", "message": "Trip not found", "status_code": 404}
        if trip.get("status") == "received":
            return {"status": "error", "message": "Cannot unlink from a received trip", "status_code": 400}

        sb.table("challan_details").update({"truck_trip_id": None}).eq("id", challan_id).eq("truck_trip_id", trip_id).execute()

        count_res = sb.table("challan_details").select("id", count="exact").eq("truck_trip_id", trip_id).eq("is_active", True).execute()
        new_count = count_res.count or 0
        sb.table("truck_trips").update({"total_challan_count": new_count}).eq("id", trip_id).execute()

        return {"status": "success", "message": "Challan unlinked from trip", "remaining_count": new_count}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── DISPATCH TRIP ─────────────────────────────────────────────────────────────

def dispatch_trip(trip_id: str, user_id: str = None) -> dict:
    """Mark a trip as dispatched. Sets dispatch_date."""
    try:
        sb = get_supabase()

        trip = sb.table("truck_trips").select("id, status, total_challan_count").eq("id", trip_id).single().execute().data
        if not trip:
            return {"status": "error", "message": "Trip not found", "status_code": 404}
        if trip["status"] != "pending":
            return {"status": "error", "message": f"Trip is already {trip['status']}", "status_code": 400}
        if trip.get("total_challan_count", 0) == 0:
            return {"status": "error", "message": "Cannot dispatch an empty trip (no challans linked)", "status_code": 400}

        now = _now()
        resp = sb.table("truck_trips").update({
            "status": "dispatched",
            "dispatch_date": now,
            "updated_at": now,
        }).eq("id", trip_id).execute()

        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Trip dispatched"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── MARK RECEIVED ─────────────────────────────────────────────────────────────

def receive_trip(trip_id: str, user_id: str = None) -> dict:
    """Mark a trip as received at hub."""
    try:
        sb = get_supabase()

        trip = sb.table("truck_trips").select("id, status").eq("id", trip_id).single().execute().data
        if not trip:
            return {"status": "error", "message": "Trip not found", "status_code": 404}
        if trip["status"] == "pending":
            return {"status": "error", "message": "Trip must be dispatched before receiving", "status_code": 400}
        if trip["status"] == "received":
            return {"status": "error", "message": "Trip already received", "status_code": 400}

        now = _now()
        resp = sb.table("truck_trips").update({
            "status": "received",
            "received_date": now,
            "received_by": user_id,
            "updated_at": now,
        }).eq("id", trip_id).execute()

        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Trip marked as received at hub"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── UPDATE ────────────────────────────────────────────────────────────────────

def update_trip(trip_id: str, data: dict) -> dict:
    """Update trip details (driver, remarks). Cannot update received trips."""
    try:
        sb = get_supabase()

        trip = sb.table("truck_trips").select("id, status").eq("id", trip_id).single().execute().data
        if not trip:
            return {"status": "error", "message": "Trip not found", "status_code": 404}
        if trip["status"] == "received":
            return {"status": "error", "message": "Cannot update a received trip", "status_code": 400}

        allowed = {"driver_id", "owner_id", "branch_id", "remarks", "truck_id"}
        update = {k: v for k, v in data.items() if k in allowed}
        update["updated_at"] = _now()

        resp = sb.table("truck_trips").update(update).eq("id", trip_id).execute()
        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Trip updated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── SOFT DELETE ───────────────────────────────────────────────────────────────

def delete_trip(trip_id: str) -> dict:
    """Soft-delete a pending trip. Unlinks its challans."""
    try:
        sb = get_supabase()

        trip = sb.table("truck_trips").select("id, status").eq("id", trip_id).single().execute().data
        if not trip:
            return {"status": "error", "message": "Trip not found", "status_code": 404}
        if trip["status"] != "pending":
            return {"status": "error", "message": "Only pending trips can be deleted", "status_code": 400}

        sb.table("challan_details").update({"truck_trip_id": None}).eq("truck_trip_id", trip_id).execute()
        sb.table("truck_trips").update({"is_active": False, "updated_at": _now()}).eq("id", trip_id).execute()

        return {"status": "success", "message": "Trip deleted and challans unlinked"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
