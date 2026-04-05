"""
Master Data CRUD Service
Handles List (paginated), Create, Update, Delete, Bulk-Update
for: cities, transports, consignors, consignees, rates
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

PAGE_SIZE = 40  # default rows per page


# ── Table config ──────────────────────────────────────────────
# Maps entity name → { table, columns (for select), search_cols, order }

TABLE_CONFIG = {
    "cities": {
        "table": "cities",
        "columns": "id, city_code, city_name, created_by, updated_by, created_at, updated_at",
        "search_cols": ["city_code", "city_name"],
        "order": "city_name",
    },
    "transports": {
        "table": "transports",
        "columns": "id, transport_name, city_id, city_name, address, gst_number, mob_number, branch_owner_name, website, transport_admin_id, is_prior, created_by, updated_by, created_at, updated_at",
        "search_cols": ["transport_name", "city_name", "gst_number"],
        "order": "transport_name",
    },
    "consignors": {
        "table": "consignors",
        "columns": "id, company_name, company_add, number, gst_num, adhar, pan, created_by, updated_by, created_at, updated_at",
        "search_cols": ["company_name", "gst_num", "number"],
        "order": "company_name",
    },
    "consignees": {
        "table": "consignees",
        "columns": "id, company_name, company_add, number, gst_num, adhar, pan, created_by, updated_by, created_at, updated_at",
        "search_cols": ["company_name", "gst_num", "number"],
        "order": "company_name",
    },
    "rates": {
        "table": "rates",
        "columns": "id, branch_id, city_id, consignor_id, rate, is_default, created_by, updated_by, created_at, updated_at",
        "search_cols": [],
        "order": "rate",
    },
}

VALID_ENTITIES = set(TABLE_CONFIG.keys())


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── LIST (paginated) ──────────────────────────────────────────

def list_records(entity: str, page: int = 1, page_size: int = PAGE_SIZE,
                 search: str = None, filters: dict = None) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}

    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()

        query = sb.table(cfg["table"]).select(cfg["columns"], count="exact")

        # Apply search — ilike on searchable columns
        if search and cfg["search_cols"]:
            # Supabase doesn't have multi-column OR via chaining,
            # so we use .or_() with ilike on each search col
            or_parts = ",".join(f"{col}.ilike.%{search}%" for col in cfg["search_cols"])
            query = query.or_(or_parts)

        # Apply exact-match filters (e.g. branch_id, city_id, consignor_id)
        if filters:
            for col, val in filters.items():
                if val is not None:
                    query = query.eq(col, val)

        # Pagination
        offset = (page - 1) * page_size
        query = query.order(cfg["order"]).range(offset, offset + page_size - 1)

        resp = query.execute()
        rows = resp.data or []
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


# ── GET ONE ───────────────────────────────────────────────────

def get_record(entity: str, record_id: str) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()
        resp = sb.table(cfg["table"]).select(cfg["columns"]).eq("id", record_id).single().execute()
        return {"status": "success", "data": resp.data}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CREATE ────────────────────────────────────────────────────

def create_record(entity: str, data: dict, user_id: str = None) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()

        now = _now()
        data["created_at"] = now
        data["updated_at"] = now
        if user_id:
            data["created_by"] = user_id
            data["updated_by"] = user_id

        # Remove id if present (let DB generate)
        data.pop("id", None)

        resp = sb.table(cfg["table"]).insert(data).execute()
        return {"status": "success", "data": resp.data[0] if resp.data else None, "message": f"{entity[:-1].title()} created"}
    except Exception as e:
        msg = str(e)
        if "duplicate" in msg.lower() or "unique" in msg.lower():
            return {"status": "error", "message": f"Duplicate entry: {msg}", "status_code": 409}
        return {"status": "error", "message": msg, "status_code": 500}


# ── UPDATE ────────────────────────────────────────────────────

def update_record(entity: str, record_id: str, data: dict, user_id: str = None) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()

        data["updated_at"] = _now()
        if user_id:
            data["updated_by"] = user_id

        # Don't allow changing id
        data.pop("id", None)

        resp = sb.table(cfg["table"]).update(data).eq("id", record_id).execute()
        if not resp.data:
            return {"status": "error", "message": "Record not found", "status_code": 404}
        return {"status": "success", "data": resp.data[0], "message": f"{entity[:-1].title()} updated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── DELETE ────────────────────────────────────────────────────

def delete_record(entity: str, record_id: str) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()
        resp = sb.table(cfg["table"]).delete().eq("id", record_id).execute()
        if not resp.data:
            return {"status": "error", "message": "Record not found", "status_code": 404}
        return {"status": "success", "message": f"{entity[:-1].title()} deleted"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── BULK UPDATE ───────────────────────────────────────────────

def bulk_update(entity: str, updates: list, user_id: str = None) -> dict:
    """
    updates = [ { "id": "uuid", "field1": "val1", ... }, ... ]
    Each item MUST have an 'id'.
    """
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    if not updates or not isinstance(updates, list):
        return {"status": "error", "message": "updates must be a non-empty array", "status_code": 400}

    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()
        now = _now()

        success = 0
        failed = []
        for item in updates:
            rid = item.get("id")
            if not rid:
                failed.append({"error": "Missing id", "item": item})
                continue
            payload = {k: v for k, v in item.items() if k != "id"}
            payload["updated_at"] = now
            if user_id:
                payload["updated_by"] = user_id
            try:
                sb.table(cfg["table"]).update(payload).eq("id", rid).execute()
                success += 1
            except Exception as e:
                failed.append({"id": rid, "error": str(e)})

        return {
            "status": "success",
            "message": f"Bulk update: {success} updated, {len(failed)} failed",
            "data": {"success_count": success, "failed_count": len(failed), "failed": failed},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── BULK CREATE ───────────────────────────────────────────────

def bulk_create(entity: str, records: list, user_id: str = None) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    if not records or not isinstance(records, list):
        return {"status": "error", "message": "records must be a non-empty array", "status_code": 400}

    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()
        now = _now()

        for rec in records:
            rec.pop("id", None)
            rec["created_at"] = now
            rec["updated_at"] = now
            if user_id:
                rec["created_by"] = user_id
                rec["updated_by"] = user_id

        resp = sb.table(cfg["table"]).insert(records).execute()
        return {
            "status": "success",
            "message": f"{len(resp.data)} {entity} created",
            "data": resp.data,
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── BULK DELETE ───────────────────────────────────────────────

def bulk_delete(entity: str, ids: list) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    if not ids or not isinstance(ids, list):
        return {"status": "error", "message": "ids must be a non-empty array", "status_code": 400}

    try:
        cfg = TABLE_CONFIG[entity]
        sb = get_supabase()
        resp = sb.table(cfg["table"]).delete().in_("id", ids).execute()
        deleted = len(resp.data) if resp.data else 0
        return {
            "status": "success",
            "message": f"{deleted} {entity} deleted",
            "data": {"deleted_count": deleted},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
