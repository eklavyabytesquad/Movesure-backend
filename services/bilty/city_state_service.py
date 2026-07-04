"""
City-State Assignment Service
Handles linking cities to states (single + bulk), with denormalization of
state_code / state_name directly onto the cities row for query convenience.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase


def _now():
    return datetime.now(timezone.utc).isoformat()


def _fetch_state(sb, state_id: str) -> dict | None:
    resp = sb.table("states").select("id, state_code, state_name").eq("id", state_id).single().execute()
    return resp.data


def assign_state_to_city(city_id: str, state_id: str, user_id: str = None) -> dict:
    """Assign a state to a single city; denormalises state_code / state_name."""
    try:
        sb = get_supabase()

        state = _fetch_state(sb, state_id)
        if not state:
            return {"status": "error", "message": f"State not found: {state_id}", "status_code": 404}

        payload = {
            "state_id": state_id,
            "state_code": state["state_code"],
            "state_name": state["state_name"],
            "updated_at": _now(),
        }
        if user_id:
            payload["updated_by"] = user_id

        resp = sb.table("cities").update(payload).eq("id", city_id).execute()
        if not resp.data:
            return {"status": "error", "message": "City not found", "status_code": 404}

        return {
            "status": "success",
            "message": "State assigned to city",
            "data": resp.data[0],
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def bulk_assign_state_to_cities(updates: list, user_id: str = None) -> dict:
    """
    Bulk assign states to cities.

    updates = [
        { "city_id": "<uuid>", "state_id": "<uuid>" },
        ...
    ]
    Resolves each unique state once, then updates each city row.
    """
    if not updates or not isinstance(updates, list):
        return {"status": "error", "message": "updates must be a non-empty array", "status_code": 400}

    try:
        sb = get_supabase()
        now = _now()

        # Resolve unique state ids in one query
        unique_state_ids = list({u["state_id"] for u in updates if u.get("state_id")})
        if not unique_state_ids:
            return {"status": "error", "message": "No valid state_id provided in updates", "status_code": 400}

        state_resp = sb.table("states").select("id, state_code, state_name").in_("id", unique_state_ids).execute()
        state_map = {s["id"]: s for s in (state_resp.data or [])}

        success, failed = 0, []

        for item in updates:
            city_id = item.get("city_id")
            state_id = item.get("state_id")

            if not city_id:
                failed.append({"item": item, "error": "Missing city_id"})
                continue
            if not state_id:
                failed.append({"city_id": city_id, "error": "Missing state_id"})
                continue

            state = state_map.get(state_id)
            if not state:
                failed.append({"city_id": city_id, "error": f"State not found: {state_id}"})
                continue

            payload = {
                "state_id": state_id,
                "state_code": state["state_code"],
                "state_name": state["state_name"],
                "updated_at": now,
            }
            if user_id:
                payload["updated_by"] = user_id

            try:
                resp = sb.table("cities").update(payload).eq("id", city_id).execute()
                if resp.data:
                    success += 1
                else:
                    failed.append({"city_id": city_id, "error": "City not found"})
            except Exception as e:
                failed.append({"city_id": city_id, "error": str(e)})

        return {
            "status": "success",
            "message": f"Bulk state assignment: {success} updated, {len(failed)} failed",
            "data": {
                "success_count": success,
                "failed_count": len(failed),
                "failed": failed,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
