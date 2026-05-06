"""
Kaat Update Service
===================
Handles bulk and single-GR updates to the bilty_wise_kaat table.

Key rules:
  - kaat      = weight * kaat_rate   (recalculated whenever kaat_rate changes)
  - pf        = total - kaat         (always recalculated from bilty total)
  - kaat_dd   = stored as dd_chrg
  - kaat_rate = stored as actual_kaat_rate

Bulk update filters by:
  transport_gstin + date range + destination city (partial, case-insensitive)
  Bilties are sourced from both `bilty` and `station_bilty_summary` tables.
"""

from __future__ import annotations
from datetime import date, timedelta
from typing import Optional
from services.supabase_client import get_supabase

PAGE_SIZE = 1000


def _next_day(date_str: str) -> str:
    return str(date.fromisoformat(date_str) + timedelta(days=1))


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ---------------------------------------------------------------------------
# Helpers to fetch city IDs matching a station name
# ---------------------------------------------------------------------------

def _resolve_city_ids(sb, station_name: str) -> list[str]:
    """Return all city IDs whose name matches station_name (case-insensitive partial)."""
    res = (
        sb.table("cities")
        .select("id, city_name")
        .ilike("city_name", f"%{station_name.strip()}%")
        .execute()
    )
    return [r["id"] for r in (res.data or [])]


# ---------------------------------------------------------------------------
# Fetch bilties for a transport + date range, optionally filtered by city IDs
# ---------------------------------------------------------------------------

def _fetch_bilty_gr_info(sb, transport_gstin: str, from_date: str, to_date: str,
                          city_ids: Optional[list] = None) -> list[dict]:
    """
    Returns list of {gr_no, wt, total, to_city_id} from bilty table.
    Optionally restricted to specific city_ids.
    """
    rows, page = [], 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("bilty")
            .select("gr_no, wt, total, to_city_id")
            .eq("is_active", True)
            .eq("transport_gst", transport_gstin.strip().upper())
            .gte("bilty_date", from_date)
            .lte("bilty_date", to_date)
            .range(lo, hi)
        )
        if city_ids:
            q = q.in_("to_city_id", city_ids)
        batch = q.execute().data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return rows


def _fetch_sbs_gr_info(sb, transport_gstin: str, from_date: str, to_date_exclusive: str,
                        city_ids: Optional[list] = None) -> list[dict]:
    """
    Returns list of {gr_no, weight as wt, amount as total, city_id as to_city_id}
    from station_bilty_summary table.
    """
    rows, page = [], 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("station_bilty_summary")
            .select("gr_no, weight, amount, city_id")
            .eq("transport_gst", transport_gstin.strip().upper())
            .gte("created_at", from_date)
            .lt("created_at", to_date_exclusive)
            .range(lo, hi)
        )
        if city_ids:
            q = q.in_("city_id", city_ids)
        batch = q.execute().data or []
        # normalise keys
        rows.extend({
            "gr_no": r["gr_no"],
            "wt": r.get("weight") or 0,
            "total": r.get("amount") or 0,
            "to_city_id": r.get("city_id"),
        } for r in batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return rows


# ---------------------------------------------------------------------------
# 1. Bulk update kaat_rate by station
# ---------------------------------------------------------------------------

def bulk_update_kaat_rate(
    transport_gstin: str,
    from_date: str,
    to_date: str,
    station_name: str,
    new_kaat_rate: float,
    new_kaat_dd: Optional[float] = None,
) -> dict:
    """
    Update actual_kaat_rate (and optionally dd_chrg) for all bilties of a
    transport in a date range whose destination matches station_name.
    Recalculates kaat = weight * new_kaat_rate and pf = total - kaat.

    Returns a summary with updated count and per-bilty details.
    """
    if not transport_gstin:
        return {"status": "error", "message": "transport_gstin is required", "status_code": 400}
    if not from_date or not to_date or not station_name:
        return {"status": "error", "message": "from_date, to_date and station_name are required", "status_code": 400}
    try:
        date.fromisoformat(from_date)
        date.fromisoformat(to_date)
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD", "status_code": 400}
    if new_kaat_rate < 0:
        return {"status": "error", "message": "new_kaat_rate must be >= 0", "status_code": 400}

    sb = get_supabase()
    to_date_excl = _next_day(to_date)

    city_ids = _resolve_city_ids(sb, station_name)
    if not city_ids:
        return {
            "status": "error",
            "message": f"No city found matching '{station_name}'",
            "status_code": 404,
        }

    # Fetch bilties from both tables
    bilty_rows = _fetch_bilty_gr_info(sb, transport_gstin, from_date, to_date, city_ids)
    sbs_rows   = _fetch_sbs_gr_info(sb, transport_gstin, from_date, to_date_excl, city_ids)

    seen: set[str] = set()
    gr_info: dict[str, dict] = {}
    for r in bilty_rows:
        gr = r["gr_no"]
        if gr not in seen:
            seen.add(gr)
            gr_info[gr] = {"wt": r.get("wt") or 0, "total": r.get("total") or 0}
    for r in sbs_rows:
        gr = r["gr_no"]
        if gr not in seen:
            seen.add(gr)
            gr_info[gr] = {"wt": r.get("wt") or 0, "total": r.get("total") or 0}

    if not gr_info:
        return {
            "status": "success",
            "message": f"No bilties found for station '{station_name}' in the given range",
            "updated_count": 0,
            "updated": [],
        }

    # Recalculate and update each GR in bilty_wise_kaat
    updated = []
    not_in_kaat = []

    for gr_no, info in gr_info.items():
        wt    = info["wt"]
        total = info["total"]
        kaat  = round(wt * new_kaat_rate, 2)
        pf    = round(total - kaat, 2)

        payload: dict = {
            "actual_kaat_rate": new_kaat_rate,
            "kaat": kaat,
            "pf": pf,
        }
        if new_kaat_dd is not None:
            payload["dd_chrg"] = new_kaat_dd

        res = (
            sb.table("bilty_wise_kaat")
            .update(payload)
            .eq("gr_no", gr_no)
            .execute()
        )
        if res.data:
            updated.append({
                "gr_no": gr_no,
                "wt": wt,
                "total": total,
                "kaat_rate": new_kaat_rate,
                "kaat": kaat,
                "pf": pf,
                **({"kaat_dd": new_kaat_dd} if new_kaat_dd is not None else {}),
            })
        else:
            not_in_kaat.append(gr_no)

    return {
        "status": "success",
        "transport_gstin": transport_gstin.upper(),
        "station_name": station_name,
        "city_ids_matched": city_ids,
        "from_date": from_date,
        "to_date": to_date,
        "new_kaat_rate": new_kaat_rate,
        **({"new_kaat_dd": new_kaat_dd} if new_kaat_dd is not None else {}),
        "updated_count": len(updated),
        "skipped_count": len(not_in_kaat),
        "skipped_gr_nos": not_in_kaat,
        "updated": updated,
    }


# ---------------------------------------------------------------------------
# 2. Update a single GR
# ---------------------------------------------------------------------------

def update_single_gr_kaat(
    gr_no: str,
    kaat_rate: Optional[float] = None,
    kaat: Optional[float] = None,
    kaat_dd: Optional[float] = None,
    pf_override: Optional[float] = None,
) -> dict:
    """
    Update kaat fields for a single GR number in bilty_wise_kaat.

    Priority logic:
      - If kaat_rate is given → kaat = weight * kaat_rate (fetched from bilty/sbs)
                                pf   = total - kaat
      - If kaat is given directly (no kaat_rate) → use it as-is,
                                pf = total - kaat (fetched from bilty/sbs)
      - If only kaat_dd is given → update only dd_chrg
      - pf_override: if explicitly provided, overrides the calculated pf
    """
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    if kaat_rate is None and kaat is None and kaat_dd is None and pf_override is None:
        return {"status": "error", "message": "At least one of kaat_rate, kaat, kaat_dd, pf must be provided", "status_code": 400}

    sb = get_supabase()

    # Fetch current kaat row
    kaat_res = (
        sb.table("bilty_wise_kaat")
        .select("gr_no, kaat, pf, actual_kaat_rate, dd_chrg")
        .eq("gr_no", gr_no)
        .execute()
    )
    if not kaat_res.data:
        return {"status": "error", "message": f"GR '{gr_no}' not found in bilty_wise_kaat", "status_code": 404}

    current = kaat_res.data[0]

    # Fetch weight and total from bilty or station_bilty_summary
    wt, total = None, None
    bilty_res = (
        sb.table("bilty")
        .select("wt, total")
        .eq("gr_no", gr_no)
        .eq("is_active", True)
        .limit(1)
        .execute()
    )
    if bilty_res.data:
        wt    = bilty_res.data[0].get("wt") or 0
        total = bilty_res.data[0].get("total") or 0
    else:
        sbs_res = (
            sb.table("station_bilty_summary")
            .select("weight, amount")
            .eq("gr_no", gr_no)
            .limit(1)
            .execute()
        )
        if sbs_res.data:
            wt    = sbs_res.data[0].get("weight") or 0
            total = sbs_res.data[0].get("amount") or 0

    payload: dict = {}

    # Determine new kaat value
    if kaat_rate is not None:
        if wt is None:
            return {"status": "error", "message": f"Could not find weight for GR '{gr_no}'", "status_code": 404}
        new_kaat = round(wt * kaat_rate, 2)
        payload["actual_kaat_rate"] = kaat_rate
        payload["kaat"] = new_kaat
    elif kaat is not None:
        new_kaat = kaat
        payload["kaat"] = new_kaat
    else:
        new_kaat = current.get("kaat") or 0

    # Determine new pf
    if pf_override is not None:
        payload["pf"] = pf_override
    elif "kaat" in payload and total is not None:
        payload["pf"] = round(total - new_kaat, 2)

    if kaat_dd is not None:
        payload["dd_chrg"] = kaat_dd

    res = (
        sb.table("bilty_wise_kaat")
        .update(payload)
        .eq("gr_no", gr_no)
        .execute()
    )
    if not res.data:
        return {"status": "error", "message": "Update failed — row not found or unchanged", "status_code": 500}

    row = res.data[0]
    return {
        "status": "success",
        "gr_no": gr_no,
        "updated": {
            "kaat_rate": row.get("actual_kaat_rate"),
            "kaat": row.get("kaat"),
            "kaat_dd": row.get("dd_chrg"),
            "pf": row.get("pf"),
        },
        "weight": wt,
        "total": total,
    }
