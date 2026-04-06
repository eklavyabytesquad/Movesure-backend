"""
Challan Book Service
CRUD for challan_books — number series management for challans.
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

COLS = (
    "id, prefix, from_number, to_number, digits, postfix, "
    "current_number, from_branch_id, to_branch_id, "
    "branch_1, branch_2, branch_3, "
    "is_active, is_completed, is_fixed, auto_continue, "
    "created_by, created_at, updated_at"
)

PAGE_SIZE = 40


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── LIST ──────────────────────────────────────────────────────

def list_challan_books(branch_id: str = None, active_only: bool = False,
                       page: int = 1, page_size: int = PAGE_SIZE) -> dict:
    """List challan books, optionally filtered by branch and active status."""
    try:
        sb = get_supabase()
        q = sb.table("challan_books").select(COLS, count="exact")

        if branch_id:
            # Books where this branch is any of branch_1/2/3
            q = q.or_(
                f"branch_1.eq.{branch_id},"
                f"branch_2.eq.{branch_id},"
                f"branch_3.eq.{branch_id}"
            )

        if active_only:
            q = q.eq("is_active", True).eq("is_completed", False)

        offset = (page - 1) * page_size
        q = q.order("created_at", desc=True).range(offset, offset + page_size - 1)
        resp = q.execute()
        rows = resp.data or []
        total = resp.count if resp.count is not None else len(rows)

        return {
            "status": "success",
            "data": {"rows": rows, "page": page, "page_size": page_size,
                     "total": total, "has_more": (offset + page_size) < total},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── GET ONE ───────────────────────────────────────────────────

def get_challan_book(book_id: str) -> dict:
    try:
        sb = get_supabase()
        resp = sb.table("challan_books").select(COLS).eq("id", book_id).single().execute()
        return {"status": "success", "data": resp.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CREATE ────────────────────────────────────────────────────

def create_challan_book(data: dict) -> dict:
    try:
        sb = get_supabase()
        now = _now()
        data.pop("id", None)

        # Validate required fields
        required = ["from_number", "to_number", "digits", "from_branch_id", "to_branch_id", "branch_1", "created_by"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return {"status": "error", "message": f"Missing fields: {', '.join(missing)}", "status_code": 400}

        data["current_number"] = data.get("current_number", data["from_number"])
        data["is_active"] = True
        data["is_completed"] = False
        data["created_at"] = now
        data["updated_at"] = now

        resp = sb.table("challan_books").insert(data).execute()
        return {"status": "success", "data": resp.data[0] if resp.data else None,
                "message": "Challan book created"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── UPDATE ────────────────────────────────────────────────────

def update_challan_book(book_id: str, data: dict) -> dict:
    try:
        sb = get_supabase()
        data.pop("id", None)
        data["updated_at"] = _now()

        resp = sb.table("challan_books").update(data).eq("id", book_id).execute()
        if not resp.data:
            return {"status": "error", "message": "Challan book not found", "status_code": 404}
        return {"status": "success", "data": resp.data[0], "message": "Challan book updated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
