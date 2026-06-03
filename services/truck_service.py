"""
Truck Service — list and get trucks.
"""
from services.supabase_client import get_supabase

TRUCK_COLS = (
    "id, truck_number, truck_type, tyre_count, brand, "
    "year_of_manufacturing, rc_number, insurance_number, permit_number, "
    "fuel_type, loading_capacity, is_active, is_available, current_location, "
    "owner_id, created_at"
)


def list_trucks(
    active_only: bool = True,
    available_only: bool = False,
    search: str = None,
    page: int = 1,
    page_size: int = 100,
) -> dict:
    try:
        sb = get_supabase()
        q = sb.table("trucks").select(TRUCK_COLS, count="exact")
        if active_only:
            q = q.eq("is_active", True)
        if available_only:
            q = q.eq("is_available", True)
        if search:
            q = q.ilike("truck_number", f"%{search}%")
        offset = (page - 1) * page_size
        q = q.order("truck_number").range(offset, offset + page_size - 1)
        resp = q.execute()
        total = resp.count if resp.count is not None else len(resp.data or [])
        return {
            "status": "success",
            "data": {
                "rows": resp.data or [],
                "total": total,
                "page": page,
                "page_size": page_size,
                "has_more": (offset + page_size) < total,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def get_truck(truck_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("trucks").select(TRUCK_COLS).eq("id", truck_id).single().execute()
        if not resp.data:
            return {"status": "error", "message": "Truck not found", "status_code": 404}
        return {"status": "success", "data": resp.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
