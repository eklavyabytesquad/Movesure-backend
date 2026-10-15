"""
Station (Manual) Bilty Meta Service
=====================================
Extra fields on station_bilty_summary (see migrations/020):

  small_bilties : JSONB array of small slips merged into this manual bilty
                  [{id, small_gr_no, no_of_packets, weight, amount, pvt_marks,
                    booked_at, remark, created_at}]
  group_ref     : optional group name (e.g. 'MRM', 'RONKEY METAL'), stored UPPER-cased
  short_packages_count / is_advance_bilty / advance_amount / remaining_amount
                : same meaning as on the bilty table

Rows are addressed by station_bilty_summary.id — gr_no is NOT unique in this
table (the same manual GR can exist under two stations).
"""
import uuid
from datetime import datetime, timezone
from services.supabase_client import get_supabase

TABLE = "station_bilty_summary"
META_COLS = (
    "id, station, gr_no, consignor, consignee, no_of_packets, weight, amount, payment_status, "
    "small_bilties, group_ref, short_packages_count, is_advance_bilty, advance_amount, remaining_amount, "
    "created_at, updated_at"
)
_SMALL_FIELDS = ("small_gr_no", "no_of_packets", "weight", "amount", "pvt_marks", "booked_at", "remark")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400) -> dict:
    return {"status": "error", "message": msg, "status_code": code}


def _fetch(sb, sbs_id: str):
    r = sb.table(TABLE).select(META_COLS).eq("id", sbs_id).limit(1).execute()
    return r.data[0] if r.data else None


def _with_summary(row: dict) -> dict:
    smalls = row.get("small_bilties") or []
    row["small_bilty_summary"] = {
        "count": len(smalls),
        "total_packets": sum(int(s.get("no_of_packets") or 0) for s in smalls),
        "total_weight": round(sum(float(s.get("weight") or 0) for s in smalls), 2),
        "total_amount": round(sum(float(s.get("amount") or 0) for s in smalls), 2),
    }
    return row


def _clean_small(data: dict, existing: dict | None = None) -> tuple[dict | None, str | None]:
    item = dict(existing or {})
    for f in _SMALL_FIELDS:
        if f in data:
            item[f] = data[f]
    item["small_gr_no"] = str(item.get("small_gr_no") or "").strip().upper()
    if not item["small_gr_no"]:
        return None, "small_gr_no is required"
    try:
        for f, cast in (("no_of_packets", int), ("weight", float), ("amount", float)):
            if item.get(f) not in (None, ""):
                item[f] = cast(item[f])
                if item[f] < 0:
                    return None, f"{f} cannot be negative"
            else:
                item[f] = 0
    except (TypeError, ValueError):
        return None, "no_of_packets / weight / amount must be numbers"
    return item, None


def _save_smalls(sb, sbs_id: str, smalls: list, updated_by: str | None) -> dict:
    payload = {"small_bilties": smalls, "updated_at": _now()}
    if updated_by:
        payload["updated_by"] = updated_by
    sb.table(TABLE).update(payload).eq("id", sbs_id).execute()
    return {"status": "success", "data": _with_summary(_fetch(sb, sbs_id))}


# ── Read ──────────────────────────────────────────────────────────────────────
def get_station_bilty_meta(sbs_id: str) -> dict:
    row = _fetch(get_supabase(), sbs_id)
    if not row:
        return _err("Station bilty not found", 404)
    return {"status": "success", "data": _with_summary(row)}


def lookup_station_bilty(gr_no: str, station: str | None = None) -> dict:
    """All station bilty rows for a manual GR no (optionally one station)."""
    q = get_supabase().table(TABLE).select(META_COLS).eq("gr_no", gr_no.strip())
    if station:
        q = q.eq("station", station.strip())
    rows = q.execute().data or []
    return {"status": "success", "data": [_with_summary(r) for r in rows], "count": len(rows)}


def find_by_small_gr(small_gr_no: str) -> dict:
    """Which manual bilty(s) a small slip GR no was merged into."""
    needle = [{"small_gr_no": small_gr_no.strip().upper()}]
    rows = get_supabase().table(TABLE).select(META_COLS).contains("small_bilties", needle).execute().data or []
    return {"status": "success", "data": [_with_summary(r) for r in rows], "count": len(rows)}


# ── Meta (group_ref / short / advance) ────────────────────────────────────────
def update_station_bilty_meta(sbs_id: str, data: dict) -> dict:
    """data: any of group_ref, short_packages_count, is_advance_bilty,
    advance_amount, remaining_amount, updated_by. Omitted keys are untouched;
    group_ref = null / "" clears the group."""
    sb = get_supabase()
    row = _fetch(sb, sbs_id)
    if not row:
        return _err("Station bilty not found", 404)

    payload = {}
    if "group_ref" in data:
        g = (data.get("group_ref") or "").strip().upper()
        if len(g) > 100:
            return _err("group_ref max length is 100")
        payload["group_ref"] = g or None
    if "short_packages_count" in data:
        try:
            n = int(data.get("short_packages_count") or 0)
        except (TypeError, ValueError):
            return _err("short_packages_count must be an integer")
        if n < 0:
            return _err("short_packages_count cannot be negative")
        if n > int(row.get("no_of_packets") or 0):
            return _err(f"short_packages_count ({n}) cannot exceed no_of_packets ({row.get('no_of_packets')})")
        payload["short_packages_count"] = n
    if "is_advance_bilty" in data:
        payload["is_advance_bilty"] = bool(data.get("is_advance_bilty"))
    for f in ("advance_amount", "remaining_amount"):
        if f in data:
            try:
                payload[f] = None if data[f] in (None, "") else float(data[f])
            except (TypeError, ValueError):
                return _err(f"{f} must be a number")
    # advance given without remaining -> remaining = amount - advance
    if payload.get("advance_amount") is not None and "remaining_amount" not in data:
        payload["remaining_amount"] = round(float(row.get("amount") or 0) - payload["advance_amount"], 2)

    if not payload:
        return _err("Nothing to update")
    payload["updated_at"] = _now()
    if data.get("updated_by"):
        payload["updated_by"] = data["updated_by"]
    sb.table(TABLE).update(payload).eq("id", sbs_id).execute()
    return {"status": "success", "message": "Updated", "data": _with_summary(_fetch(sb, sbs_id))}


# ── Small bilties ─────────────────────────────────────────────────────────────
def add_small_bilty(sbs_id: str, data: dict) -> dict:
    sb = get_supabase()
    row = _fetch(sb, sbs_id)
    if not row:
        return _err("Station bilty not found", 404)
    item, err = _clean_small(data)
    if err:
        return _err(err)
    smalls = row.get("small_bilties") or []
    if any(s.get("small_gr_no") == item["small_gr_no"] for s in smalls):
        return _err(f"Small bilty {item['small_gr_no']} already added to this bilty", 409)
    item["id"] = str(uuid.uuid4())
    item["created_at"] = _now()
    return _save_smalls(sb, sbs_id, smalls + [item], data.get("updated_by"))


def update_small_bilty(sbs_id: str, item_id: str, data: dict) -> dict:
    sb = get_supabase()
    row = _fetch(sb, sbs_id)
    if not row:
        return _err("Station bilty not found", 404)
    smalls = row.get("small_bilties") or []
    idx = next((i for i, s in enumerate(smalls) if s.get("id") == item_id), None)
    if idx is None:
        return _err("Small bilty not found", 404)
    item, err = _clean_small(data, smalls[idx])
    if err:
        return _err(err)
    if any(i != idx and s.get("small_gr_no") == item["small_gr_no"] for i, s in enumerate(smalls)):
        return _err(f"Small bilty {item['small_gr_no']} already added to this bilty", 409)
    item["updated_at"] = _now()
    smalls[idx] = item
    return _save_smalls(sb, sbs_id, smalls, data.get("updated_by"))


def remove_small_bilty(sbs_id: str, item_id: str, updated_by: str | None = None) -> dict:
    sb = get_supabase()
    row = _fetch(sb, sbs_id)
    if not row:
        return _err("Station bilty not found", 404)
    smalls = row.get("small_bilties") or []
    kept = [s for s in smalls if s.get("id") != item_id]
    if len(kept) == len(smalls):
        return _err("Small bilty not found", 404)
    return _save_smalls(sb, sbs_id, kept, updated_by)


# ── Groups ────────────────────────────────────────────────────────────────────
def list_groups(q: str | None = None) -> dict:
    """Distinct group_refs with bilty counts — for the group dropdown/autocomplete."""
    query = get_supabase().table(TABLE).select("group_ref").not_.is_("group_ref", "null")
    if q:
        query = query.ilike("group_ref", f"%{q.strip()}%")
    counts: dict[str, int] = {}
    off = 0
    while True:
        rows = query.range(off, off + 999).execute().data or []
        for r in rows:
            counts[r["group_ref"]] = counts.get(r["group_ref"], 0) + 1
        if len(rows) < 1000:
            break
        off += 1000
    data = [{"group_ref": g, "bilty_count": n} for g, n in sorted(counts.items())]
    return {"status": "success", "data": data, "count": len(data)}


def list_bilties_by_group(group_ref: str, page: int = 1, page_size: int = 50) -> dict:
    g = (group_ref or "").strip().upper()
    if not g:
        return _err("group_ref is required")
    page, page_size = max(1, page), min(max(1, page_size), 200)
    start = (page - 1) * page_size
    r = (get_supabase().table(TABLE).select(META_COLS, count="exact").eq("group_ref", g)
         .order("created_at", desc=True).range(start, start + page_size - 1).execute())
    return {"status": "success", "data": [_with_summary(x) for x in r.data or []],
            "total": r.count or 0, "page": page, "page_size": page_size}
