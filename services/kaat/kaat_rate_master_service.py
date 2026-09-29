"""
Kaat Rate Master Service
=========================
CRUD for `transport_hub_rates` — the per-station, per-transport kaat rate
master (migrations/016_kaat_rate_master_transport_gstin.sql). A station
(destination_city_id) can carry rates for any number of different
transports; the DB only blocks the SAME transport getting two active
rates for the SAME station.

Functions:
  list_hub_rates             — list/filter rates (by station name, gstin, active)
  get_hub_rate                — fetch one rate by id
  create_hub_rate              — add a transport rate for a station
                                  (call again with a different transport to
                                  add a 2nd/3rd/... transport for that station)
  update_hub_rate               — edit an existing rate
  delete_hub_rate                — soft-delete (is_active = false)
  apply_hub_rate_to_bilties       — push a rate into bilty_wise_kaat for a
                                     transport + date range (recalculates
                                     kaat/pf), and stamps transport_hub_rate_id
                                     on each updated row
"""
from __future__ import annotations
from typing import Optional
from services.supabase_client import get_supabase
from services.kaat.kaat_update_service import (
    _resolve_city_info, _fetch_bilty_gr_info, _fetch_sbs_gr_info,
    _chunks, _next_day, _sync_pohonch_metadata, PAGE_SIZE,
)


def _resolve_single_city(sb, station_name: str) -> dict:
    """Resolve station_name to exactly one city row. Returns {"city": {...}}
    or {"error": ..., "matches": [...]} when it's ambiguous or not found."""
    res = (
        sb.table("cities")
        .select("id, city_name")
        .ilike("city_name", f"%{station_name.strip()}%")
        .execute()
    )
    rows = res.data or []
    if not rows:
        return {"error": f"No city found matching '{station_name}'"}
    if len(rows) > 1:
        exact = [r for r in rows if r["city_name"].strip().upper() == station_name.strip().upper()]
        if len(exact) == 1:
            return {"city": exact[0]}
        return {"error": f"Multiple cities match '{station_name}' — pass destination_city_id instead", "matches": rows}
    return {"city": rows[0]}


def list_hub_rates(
    station_name: Optional[str] = None,
    transport_gstin: Optional[str] = None,
    is_active: Optional[bool] = None,
) -> dict:
    """List rates, optionally filtered by station (partial name match),
    transport GSTIN, and active status. Multiple rows per station is the
    normal case, not an error."""
    try:
        sb = get_supabase()
        q = sb.table("transport_hub_rates").select("*")
        if transport_gstin:
            q = q.eq("transport_gstin", transport_gstin.strip().upper())
        if is_active is not None:
            q = q.eq("is_active", is_active)
        if station_name:
            city_ids, _ = _resolve_city_info(sb, station_name)
            if not city_ids:
                return {"status": "success", "data": [], "count": 0,
                        "message": f"No city found matching '{station_name}'"}
            q = q.in_("destination_city_id", city_ids)
        rows = q.order("destination_city_id").execute().data or []

        # attach destination_city_name for readability
        city_ids_all = list({r["destination_city_id"] for r in rows if r.get("destination_city_id")})
        city_map = {}
        for chunk in _chunks(city_ids_all, 200):
            for c in sb.table("cities").select("id, city_name").in_("id", chunk).execute().data or []:
                city_map[c["id"]] = c["city_name"]
        for r in rows:
            r["destination_city_name"] = city_map.get(r.get("destination_city_id"), "")

        return {"status": "success", "data": rows, "count": len(rows)}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def get_hub_rate(rate_id: str) -> dict:
    try:
        sb = get_supabase()
        res = sb.table("transport_hub_rates").select("*").eq("id", rate_id).execute()
        if not res.data:
            return {"status": "error", "message": "Rate not found", "status_code": 404}
        row = res.data[0]
        if row.get("destination_city_id"):
            city = sb.table("cities").select("city_name").eq("id", row["destination_city_id"]).execute()
            row["destination_city_name"] = (city.data or [{}])[0].get("city_name", "") if city.data else ""
        return {"status": "success", "data": row}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def create_hub_rate(
    transport_name: str,
    transport_gstin: Optional[str] = None,
    transport_id: Optional[str] = None,
    destination_city_id: Optional[str] = None,
    station_name: Optional[str] = None,
    rate_per_kg: Optional[float] = None,
    rate_per_pkg: Optional[float] = None,
    pricing_mode: str = "per_kg",
    min_charge: float = 0,
    goods_type: Optional[str] = None,
    bilty_chrg: Optional[float] = None,
    ewb_chrg: Optional[float] = None,
    other_chrg: Optional[float] = None,
    labour_chrg: Optional[float] = None,
    created_by: Optional[str] = None,
) -> dict:
    """Add a transport rate for a station. To add a 2nd, 3rd... transport
    for the SAME station, call this again with a different
    transport_name/transport_gstin/transport_id — the station keeps every
    active rate it's given, one row each."""
    if not transport_name:
        return {"status": "error", "message": "transport_name is required", "status_code": 400}
    if not destination_city_id and not station_name:
        return {"status": "error", "message": "destination_city_id or station_name is required", "status_code": 400}
    if pricing_mode == "per_kg" and rate_per_kg is None:
        return {"status": "error", "message": "rate_per_kg is required for pricing_mode='per_kg'", "status_code": 400}
    if pricing_mode == "per_pkg" and rate_per_pkg is None:
        return {"status": "error", "message": "rate_per_pkg is required for pricing_mode='per_pkg'", "status_code": 400}

    sb = get_supabase()

    if not destination_city_id:
        resolved = _resolve_single_city(sb, station_name)
        if "error" in resolved:
            status_code = 409 if "matches" in resolved else 400
            return {"status": "error", "message": resolved["error"],
                    "matches": resolved.get("matches"), "status_code": status_code}
        destination_city_id = resolved["city"]["id"]

    # Same transport already has an active rate for this station?
    if transport_id:
        dup = (
            sb.table("transport_hub_rates")
            .select("id")
            .eq("destination_city_id", destination_city_id)
            .eq("transport_id", transport_id)
            .eq("is_active", True)
            .execute()
        )
        if dup.data:
            return {"status": "error",
                    "message": "This transport already has an active rate for this station — update it instead",
                    "status_code": 409}

    record = {
        "transport_id": transport_id,
        "transport_name": transport_name.strip(),
        "transport_gstin": (transport_gstin or "").strip().upper() or None,
        "destination_city_id": destination_city_id,
        "goods_type": goods_type,
        "pricing_mode": pricing_mode,
        "rate_per_kg": rate_per_kg,
        "rate_per_pkg": rate_per_pkg,
        "min_charge": min_charge or 0,
        "bilty_chrg": bilty_chrg,
        "ewb_chrg": ewb_chrg,
        "other_chrg": other_chrg,
        "labour_chrg": labour_chrg,
        "is_active": True,
        "created_by": created_by,
        "updated_by": created_by,
    }
    res = sb.table("transport_hub_rates").insert(record).execute()
    if not res.data:
        return {"status": "error", "message": "Insert failed", "status_code": 500}
    return {"status": "success", "message": "Rate added", "data": res.data[0]}


_EDITABLE_FIELDS = {
    "transport_name", "transport_gstin", "transport_id", "goods_type",
    "pricing_mode", "rate_per_kg", "rate_per_pkg", "min_charge",
    "bilty_chrg", "ewb_chrg", "other_chrg", "labour_chrg", "is_active",
}


def update_hub_rate(rate_id: str, updates: dict, updated_by: Optional[str] = None) -> dict:
    if not rate_id:
        return {"status": "error", "message": "rate_id is required", "status_code": 400}
    payload = {k: v for k, v in (updates or {}).items() if k in _EDITABLE_FIELDS}
    if not payload:
        return {"status": "error", "message": "No editable fields supplied", "status_code": 400}
    if payload.get("transport_gstin"):
        payload["transport_gstin"] = payload["transport_gstin"].strip().upper()
    payload["updated_by"] = updated_by

    try:
        sb = get_supabase()
        existing = sb.table("transport_hub_rates").select("id").eq("id", rate_id).execute()
        if not existing.data:
            return {"status": "error", "message": "Rate not found", "status_code": 404}

        res = sb.table("transport_hub_rates").update(payload).eq("id", rate_id).execute()
        return {"status": "success", "message": "Rate updated", "data": (res.data or [None])[0]}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def delete_hub_rate(rate_id: str) -> dict:
    """Soft-delete — sets is_active = false, freeing the station+transport
    slot for a fresh active rate later."""
    try:
        sb = get_supabase()
        existing = sb.table("transport_hub_rates").select("id").eq("id", rate_id).execute()
        if not existing.data:
            return {"status": "error", "message": "Rate not found", "status_code": 404}
        sb.table("transport_hub_rates").update({"is_active": False}).eq("id", rate_id).execute()
        return {"status": "success", "message": "Rate deactivated"}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


def apply_hub_rate_to_bilties(
    rate_id: str,
    from_date: str,
    to_date: str,
    new_kaat_dd: Optional[float] = None,
) -> dict:
    """
    Push a master rate into bilty_wise_kaat: recalculates kaat = weight *
    rate_per_kg and pf = total - kaat - dd for every bilty of that rate's
    transport, to that rate's station, in the date range (same formula as
    /api/kaat/bulk-update) — and additionally stamps transport_hub_rate_id
    (+ transport_id, if set) on each updated row so it stays linked back
    to this master rate.
    """
    if not rate_id or not from_date or not to_date:
        return {"status": "error", "message": "rate_id, from_date and to_date are required", "status_code": 400}

    sb = get_supabase()
    rate_res = sb.table("transport_hub_rates").select("*").eq("id", rate_id).eq("is_active", True).execute()
    if not rate_res.data:
        return {"status": "error", "message": "Active rate not found for this id", "status_code": 404}
    rate = rate_res.data[0]

    if rate.get("pricing_mode") != "per_kg" or rate.get("rate_per_kg") is None:
        return {"status": "error", "message": "apply currently supports pricing_mode='per_kg' rates only", "status_code": 400}
    if not rate.get("transport_gstin"):
        return {"status": "error", "message": "This rate has no transport_gstin set — update the rate with one before applying", "status_code": 400}

    transport_gstin = rate["transport_gstin"]
    new_kaat_rate = rate["rate_per_kg"]
    city_ids = [rate["destination_city_id"]]

    to_date_excl = _next_day(to_date)
    bilty_rows = _fetch_bilty_gr_info(sb, transport_gstin, from_date, to_date, city_ids)
    sbs_rows = _fetch_sbs_gr_info(sb, transport_gstin, from_date, to_date_excl, city_ids, None)

    gr_info: dict[str, dict] = {}
    for r in bilty_rows + sbs_rows:
        gr = r["gr_no"]
        if gr not in gr_info:
            gr_info[gr] = {"wt": r.get("wt") or 0, "total": r.get("total") or 0,
                            "payment_mode": r.get("payment_mode") or "to-pay"}

    if not gr_info:
        return {"status": "success", "message": "No bilties found for this rate's transport/station/date range",
                "updated_count": 0, "updated": []}

    existing_dd: dict[str, float] = {}
    for chunk in _chunks(list(gr_info.keys()), PAGE_SIZE):
        dd_rows = sb.table("bilty_wise_kaat").select("gr_no, dd_chrg").in_("gr_no", chunk).execute()
        for r in (dd_rows.data or []):
            existing_dd[r["gr_no"]] = r.get("dd_chrg") or 0

    updated, skipped = [], []
    for gr_no, info in gr_info.items():
        wt, total, payment_mode = info["wt"], info["total"], info["payment_mode"]
        dd = new_kaat_dd if new_kaat_dd is not None else existing_dd.get(gr_no, 0)
        kaat = round(wt * new_kaat_rate, 2)
        pf = round(-kaat, 2) if payment_mode == "paid" else round(total - kaat - dd, 2)

        payload = {
            "actual_kaat_rate": new_kaat_rate,
            "kaat": kaat,
            "pf": pf,
            "transport_hub_rate_id": rate_id,
        }
        if rate.get("transport_id"):
            payload["transport_id"] = rate["transport_id"]
        if new_kaat_dd is not None:
            payload["dd_chrg"] = new_kaat_dd

        res = sb.table("bilty_wise_kaat").update(payload).eq("gr_no", gr_no).execute()
        if res.data:
            updated.append({"gr_no": gr_no, "wt": wt, "total": total, "kaat_rate": new_kaat_rate,
                             "kaat": kaat, "pf": pf, "kaat_dd": dd})
        else:
            skipped.append(gr_no)

    pohonch_touched = _sync_pohonch_metadata(sb, updated)

    return {
        "status": "success",
        "rate_id": rate_id,
        "transport_gstin": transport_gstin,
        "station_city_id": rate["destination_city_id"],
        "new_kaat_rate": new_kaat_rate,
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "skipped_gr_nos": skipped,
        "pohonch_rows_synced": pohonch_touched,
        "updated": updated,
    }
