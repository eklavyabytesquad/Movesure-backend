"""
Master Data CRUD Service
Handles List (paginated), Create, Update, Delete, Bulk-Update
for: cities, transports, consignors, consignees, rates
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

PAGE_SIZE = 40  # default rows per page


# ── Table config ──────────────────────────────────────────────
# Maps entity name → { table, columns (for select), search_cols, order, pk }

TABLE_CONFIG = {
    "cities": {
        "table": "cities",
        "columns": "id, city_code, city_name, state_id, state_code, state_name, created_by, updated_by, created_at, updated_at",
        "search_cols": ["city_code", "city_name", "state_name"],
        "order": "city_name",
        "pk": "id",
    },
    "states": {
        "table": "states",
        "columns": "id, state_code, state_name, created_by, updated_by, created_at, updated_at",
        "search_cols": ["state_code", "state_name"],
        "order": "state_name",
        "pk": "id",
    },
    "transports": {
        "table": "transports",
        "columns": "id, transport_name, city_id, city_name, address, gst_number, mob_number, branch_owner_name, website, transport_admin_id, is_prior, created_by, updated_by, created_at, updated_at",
        "search_cols": ["transport_name", "city_name", "gst_number"],
        "order": "transport_name",
        "pk": "id",
    },
    "transport_admin": {
        "table": "transport_admin",
        "columns": "transport_id, transport_name, gstin, hub_mobile_number, owner_name, website, address, sample_ref_image, sample_challan_image, created_by, updated_by, created_at, updated_at",
        "search_cols": ["transport_name", "gstin", "owner_name"],
        "order": "transport_name",
        "pk": "transport_id",
    },
    "consignors": {
        "table": "consignors",
        "columns": "id, company_name, company_add, number, gst_num, adhar, pan, created_by, updated_by, created_at, updated_at",
        "search_cols": ["company_name", "gst_num", "number"],
        "order": "company_name",
        "pk": "id",
    },
    "consignees": {
        "table": "consignees",
        "columns": "id, company_name, company_add, number, gst_num, adhar, pan, created_by, updated_by, created_at, updated_at",
        "search_cols": ["company_name", "gst_num", "number"],
        "order": "company_name",
        "pk": "id",
    },
    "rates": {
        "table": "rates",
        "columns": "id, branch_id, city_id, consignor_id, rate, is_default, created_by, updated_by, created_at, updated_at",
        "search_cols": [],
        "order": "rate",
        "pk": "id",
    },
}

VALID_ENTITIES = set(TABLE_CONFIG.keys())

DIRECTORY_LIMIT = 200  # dropdown/autocomplete use — not paginated, just capped


# ── DIRECTORY (lightweight lookup for dropdowns/autocomplete) ──

# Priority order when ranking matches: a city code hit outranks a city name
# hit, which outranks a transport name hit, which outranks a phone number
# hit — so searching "KNP" surfaces the city Kanpur (code match) above any
# transport, and searching "GORAKHPUR" surfaces the city itself above a
# transport merely named "... GORAKHPUR TRANSPORT".
_FIELD_TIER = {"city_code": 0, "city_name": 1, "transport_name": 2, "mob_number": 3}
_FETCH_CAP = 1000  # DB already filters by ilike; this just bounds the worst case before we rank+slice


def _field_rank(value, q: str):
    """0 = exact match, 1 = starts-with, 2 = contains, None = no match."""
    if not value:
        return None
    v = str(value).strip().lower()
    if v == q:
        return 0
    if v.startswith(q):
        return 1
    if q in v:
        return 2
    return None


def _best_score(fields: dict, q: str):
    """Lowest (field tier * 3 + match quality) across the given {field_name: value} pairs."""
    best = None
    for field, value in fields.items():
        r = _field_rank(value, q)
        if r is None:
            continue
        score = _FIELD_TIER[field] * 3 + r
        if best is None or score < best:
            best = score
    return best


def get_directory(search: str = None, limit: int = DIRECTORY_LIMIT) -> dict:
    """
    One fast call for populating city / transport / phone-number pickers —
    no pagination, just the fields a dropdown needs. Pass `search` to
    filter+rank all of them by the same text (city code, city name,
    transport name, or mobile number); omit it to just list alphabetically.

    Ranking priority: city code > city name > transport name > mobile
    number, and on a tie the city always sorts above the transport — so
    "KNP" puts Kanpur first, and "GORAKHPUR" puts the city Gorakhpur above
    a transport that merely has "Gorakhpur" in its name.
    """
    try:
        sb = get_supabase()
        q = (search or "").strip().lower()

        cities_q = sb.table("cities").select("id, city_code, city_name, state_name")
        transports_q = sb.table("transports").select("id, transport_name, city_id, city_name, mob_number")

        if q:
            cities_q = cities_q.or_(f"city_code.ilike.%{search}%,city_name.ilike.%{search}%").limit(_FETCH_CAP)
            transports_q = transports_q.or_(
                f"transport_name.ilike.%{search}%,city_name.ilike.%{search}%,mob_number.ilike.%{search}%"
            ).limit(_FETCH_CAP)
        else:
            cities_q = cities_q.order("city_name").limit(limit)
            transports_q = transports_q.order("transport_name").limit(limit)

        cities = cities_q.execute().data or []
        transports = transports_q.execute().data or []

        if q:
            for c in cities:
                c["_score"] = _best_score({"city_code": c.get("city_code"), "city_name": c.get("city_name")}, q)
            for t in transports:
                t["_score"] = _best_score(
                    {"transport_name": t.get("transport_name"), "city_name": t.get("city_name"), "mob_number": t.get("mob_number")}, q
                )
            cities.sort(key=lambda r: (r["_score"], r.get("city_name") or ""))
            transports.sort(key=lambda r: (r["_score"], r.get("transport_name") or ""))
            for r in cities + transports:
                r.pop("_score", None)

        cities = cities[:limit]
        transports = transports[:limit]

        numbers = [
            {"transport_id": t["id"], "transport_name": t["transport_name"], "mob_number": t["mob_number"]}
            for t in transports
            if t.get("mob_number")
        ]

        # `results` is cities + transports pre-merged in final display order
        # (cities always first on a tie) — use this directly for a single
        # combined search box; `cities`/`transports`/`numbers` stay separate
        # for pickers that only need one type.
        results = (
            [{"type": "city", **c} for c in cities]
            + [{"type": "transport", **t} for t in transports]
        )[:limit]

        return {
            "status": "success",
            "data": {"results": results, "cities": cities, "transports": transports, "numbers": numbers},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── Resolve user UUIDs → names ────────────────────────────────

def _resolve_user_names(rows: list) -> list:
    """Replace created_by / updated_by UUIDs with user names."""
    if not rows:
        return rows
    user_ids = set()
    for r in rows:
        for field in ("created_by", "updated_by"):
            val = r.get(field)
            if val and isinstance(val, str) and len(val) > 20:
                user_ids.add(val)
    if not user_ids:
        return rows
    try:
        sb = get_supabase()
        resp = sb.table("users").select("id, name").in_("id", list(user_ids)).execute()
        name_map = {u["id"]: u["name"] or u["id"] for u in (resp.data or [])}
    except Exception:
        return rows
    for r in rows:
        for field in ("created_by", "updated_by"):
            val = r.get(field)
            if val and val in name_map:
                r[field] = name_map[val]
    return rows


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
        rows = _resolve_user_names(resp.data or [])
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
        pk = cfg["pk"]
        sb = get_supabase()
        resp = sb.table(cfg["table"]).select(cfg["columns"]).eq(pk, record_id).single().execute()
        row = resp.data
        if row:
            _resolve_user_names([row])
        return {"status": "success", "data": row}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CREATE ────────────────────────────────────────────────────

def create_record(entity: str, data: dict, user_id: str = None) -> dict:
    if entity not in VALID_ENTITIES:
        return {"status": "error", "message": f"Invalid entity: {entity}", "status_code": 400}
    try:
        cfg = TABLE_CONFIG[entity]
        pk = cfg["pk"]
        sb = get_supabase()

        now = _now()
        data["created_at"] = now
        data["updated_at"] = now
        if user_id:
            data["created_by"] = user_id
            data["updated_by"] = user_id

        # Remove pk if present (let DB generate)
        data.pop(pk, None)
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
        pk = cfg["pk"]
        sb = get_supabase()

        data["updated_at"] = _now()
        if user_id:
            data["updated_by"] = user_id

        # Don't allow changing pk
        data.pop(pk, None)
        data.pop("id", None)

        resp = sb.table(cfg["table"]).update(data).eq(pk, record_id).execute()
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
        pk = cfg["pk"]
        sb = get_supabase()
        resp = sb.table(cfg["table"]).delete().eq(pk, record_id).execute()
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
        pk = cfg["pk"]
        for item in updates:
            rid = item.get(pk) or item.get("id")
            if not rid:
                failed.append({"error": f"Missing {pk}", "item": item})
                continue
            payload = {k: v for k, v in item.items() if k not in (pk, "id")}
            payload["updated_at"] = now
            if user_id:
                payload["updated_by"] = user_id
            try:
                sb.table(cfg["table"]).update(payload).eq(pk, rid).execute()
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

        pk = cfg["pk"]
        for rec in records:
            rec.pop(pk, None)
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
        pk = cfg["pk"]
        sb = get_supabase()
        resp = sb.table(cfg["table"]).delete().in_(pk, ids).execute()
        deleted = len(resp.data) if resp.data else 0
        return {
            "status": "success",
            "message": f"{deleted} {entity} deleted",
            "data": {"deleted_count": deleted},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
