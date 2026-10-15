"""
Bulk Rate Service
Lets a user set ONE rate (+ optional payment mode / labour / DD / other
charges) for a consignor or consignee across EVERY station in the system in
a single call, optionally excluding a handful of cities (e.g. "100/nag for
every station except Kanpur").

Also provides a plain "list every station + whether this consignor/consignee
already has a profile for it" helper, so the frontend can show a full
station picker before applying the bulk rate.
"""
from services.supabase_client import get_supabase

RATE_UNITS = ("PER_KG", "PER_NAG")
LABOUR_UNITS = ("PER_KG", "PER_NAG", "PER_BILTY")
PAYMENT_MODES = ("to-pay", "paid")

# Fields a caller may bulk-apply on top of rate/rate_unit (all optional).
OPTIONAL_BULK_FIELDS = (
    "labour_rate", "labour_unit", "minimum_weight_kg", "freight_minimum_amount",
    "dd_charge_per_kg", "dd_charge_per_nag", "dd_print_charge_per_kg", "dd_print_charge_per_nag",
    "receiving_slip_charge", "bilty_charge", "local_charge_per_nag",
    "is_toll_tax_applicable", "toll_tax_amount", "is_no_charge",
    "transport_name", "transport_gst", "default_payment_mode",
    "effective_from", "effective_to", "is_active",
)


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _fetch_all_cities(sb, exclude_city_ids, exclude_city_names):
    exclude_ids = {c.strip() for c in (exclude_city_ids or []) if c and c.strip()}
    exclude_names = {c.strip().upper() for c in (exclude_city_names or []) if c and c.strip()}

    rows = []
    page = 0
    page_size = 1000
    while True:
        lo, hi = page * page_size, page * page_size + page_size - 1
        batch = (
            sb.table("cities")
            .select("id, city_code, city_name")
            .range(lo, hi)
            .execute()
        ).data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        page += 1

    excluded = []
    kept = []
    for c in rows:
        if c["id"] in exclude_ids or (c.get("city_name") or "").strip().upper() in exclude_names:
            excluded.append(c)
        else:
            kept.append(c)
    return kept, excluded


def _fetch_worked_cities(sb, table: str, owner_id: str) -> list:
    """
    Cities this consignor/consignee has ACTUALLY sent/received consignments
    to — not every city in the system. Matched against real shipment
    history (bilty + station_bilty_summary), not just the master-record
    name, because that's the only reliable signal of "worked with".

    table="consignor_bilty_profile" -> matches bilty.consignor_gst /
          consignor_name against the `consignors` master row for owner_id.
    table="consignee_bilty_profile" -> same, consignee side.
    """
    is_consignor = table == "consignor_bilty_profile"
    master_table = "consignors" if is_consignor else "consignees"
    gst_col = "consignor_gst" if is_consignor else "consignee_gst"
    name_col = "consignor_name" if is_consignor else "consignee_name"
    sbs_name_col = "consignor" if is_consignor else "consignee"  # station_bilty_summary's column names differ

    owner_res = sb.table(master_table).select("company_name, gst_num").eq("id", owner_id).execute().data
    if not owner_res:
        return []
    company_name = (owner_res[0].get("company_name") or "").strip()
    gst_num = (owner_res[0].get("gst_num") or "").strip().upper() or None

    city_ids = set()

    # bilty table — match by GSTIN first (exact, reliable), name as fallback
    b_query = sb.table("bilty").select("to_city_id").eq("is_active", True)
    if gst_num:
        rows = b_query.eq(gst_col, gst_num).execute().data or []
    elif company_name:
        rows = b_query.ilike(name_col, company_name).execute().data or []
    else:
        rows = []
    city_ids |= {r["to_city_id"] for r in rows if r.get("to_city_id")}

    # station_bilty_summary — no GSTIN column for consignor/consignee there, match by name only
    if company_name:
        rows = sb.table("station_bilty_summary").select("city_id").ilike(sbs_name_col, company_name).execute().data or []
        city_ids |= {r["city_id"] for r in rows if r.get("city_id")}

    if not city_ids:
        return []

    cities = []
    for chunk in _chunks(list(city_ids), 200):
        cities.extend(sb.table("cities").select("id, city_code, city_name").in_("id", chunk).execute().data or [])
    return cities


def _fetch_existing_profiles(sb, table, owner_col, owner_id):
    profiles = {}
    page = 0
    page_size = 1000
    while True:
        lo, hi = page * page_size, page * page_size + page_size - 1
        batch = (
            sb.table(table)
            .select("id, destination_station_id")
            .eq(owner_col, owner_id)
            .range(lo, hi)
            .execute()
        ).data or []
        for row in batch:
            dest = row.get("destination_station_id")
            if dest:
                profiles[dest] = row["id"]
        if len(batch) < page_size:
            break
        page += 1
    return profiles


def bulk_set_rate(
    table: str,
    owner_col: str,
    owner_id: str,
    rate: float,
    rate_unit: str = "PER_NAG",
    exclude_city_ids: list = None,
    exclude_city_names: list = None,
    extra_fields: dict = None,
    created_by: str = None,
    scope: str = "all",
) -> dict:
    """
    Generic bulk-rate setter shared by consignor and consignee profiles.

    `table` is "consignor_bilty_profile" or "consignee_bilty_profile",
    `owner_col` is "consignor_id" or "consignee_id".

    scope="all" (default, unchanged from before): every city in the
          system except the excluded ones.
    scope="worked_with": ONLY cities this consignor/consignee has actual
          shipment history to/from (bilty + station_bilty_summary,
          matched by GSTIN where available, else name) — what you
          actually want for "set my rate for the stations I ship to",
          not a rate row manufactured for every station in the system
          whether or not this party has ever used it.

    Applies `rate` + `rate_unit` (+ any of OPTIONAL_BULK_FIELDS passed in
    `extra_fields`) to the scoped set of cities: updates the profile row
    where one already exists for that city, and creates a new active
    profile row where one doesn't.
    """
    try:
        if not owner_id:
            return {"status": "error", "message": f"{owner_col} is required", "status_code": 400}
        if rate is None or rate < 0:
            return {"status": "error", "message": "rate is required and must be >= 0", "status_code": 400}
        rate_unit = (rate_unit or "PER_NAG").upper()
        if rate_unit not in RATE_UNITS:
            return {"status": "error", "message": f"rate_unit must be one of {RATE_UNITS}", "status_code": 400}
        scope = (scope or "all").lower()
        if scope not in ("all", "worked_with"):
            return {"status": "error", "message": "scope must be 'all' or 'worked_with'", "status_code": 400}

        extra_fields = dict(extra_fields or {})
        unknown = set(extra_fields) - set(OPTIONAL_BULK_FIELDS)
        if unknown:
            return {"status": "error", "message": f"Unsupported field(s): {sorted(unknown)}", "status_code": 400}
        if "labour_unit" in extra_fields and extra_fields["labour_unit"] not in LABOUR_UNITS:
            return {"status": "error", "message": f"labour_unit must be one of {LABOUR_UNITS}", "status_code": 400}
        if "default_payment_mode" in extra_fields and extra_fields["default_payment_mode"] not in PAYMENT_MODES:
            return {"status": "error", "message": f"default_payment_mode must be one of {PAYMENT_MODES}", "status_code": 400}

        sb = get_supabase()

        if scope == "worked_with":
            worked_cities = _fetch_worked_cities(sb, table, owner_id)
            if not worked_cities:
                return {"status": "error",
                        "message": "No shipment history found for this consignor/consignee — nothing to scope to. Use scope='all' to apply everywhere instead.",
                        "status_code": 404}
            exclude_ids = {c.strip() for c in (exclude_city_ids or []) if c and c.strip()}
            exclude_names = {c.strip().upper() for c in (exclude_city_names or []) if c and c.strip()}
            kept_cities = [c for c in worked_cities
                           if c["id"] not in exclude_ids and (c.get("city_name") or "").strip().upper() not in exclude_names]
            excluded_cities = [c for c in worked_cities if c not in kept_cities]
        else:
            kept_cities, excluded_cities = _fetch_all_cities(sb, exclude_city_ids, exclude_city_names)
        if not kept_cities:
            return {"status": "error", "message": "No cities left to apply after exclusions", "status_code": 400}

        existing = _fetch_existing_profiles(sb, table, owner_col, owner_id)

        common_payload = {"rate": rate, "rate_unit": rate_unit, **extra_fields}

        to_update_ids = []
        to_insert_rows = []
        for c in kept_cities:
            city_id = c["id"]
            if city_id in existing:
                to_update_ids.append(existing[city_id])
            else:
                to_insert_rows.append({
                    owner_col: owner_id,
                    "destination_station_id": city_id,
                    "city_code": c.get("city_code"),
                    "city_name": c.get("city_name"),
                    "created_by": created_by,
                    **common_payload,
                })

        updated_count = 0
        if to_update_ids:
            for chunk in _chunks(to_update_ids, 200):
                sb.table(table).update({**common_payload, "updated_by": created_by}).in_("id", chunk).execute()
                updated_count += len(chunk)

        inserted_count = 0
        if to_insert_rows:
            for chunk in _chunks(to_insert_rows, 200):
                sb.table(table).insert(chunk).execute()
                inserted_count += len(chunk)

        return {
            "status": "success",
            "data": {
                owner_col: owner_id,
                "scope": scope,
                "rate": rate,
                "rate_unit": rate_unit,
                "applied_fields": common_payload,
                "total_cities": len(kept_cities) + len(excluded_cities),
                "applied_cities": len(kept_cities),
                "excluded_cities": [c["city_name"] for c in excluded_cities],
                "updated_count": updated_count,
                "inserted_count": inserted_count,
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to bulk-set rate: {str(e)}",
            "status_code": 500,
        }


def list_stations_for_owner(table: str, owner_col: str, owner_id: str) -> dict:
    """
    Every station in the system, flagged with whether `owner_id` already has
    an active profile row for it (and that row's current rate/payment mode
    if so) — lets the frontend show a full picker before bulk-applying.
    """
    try:
        if not owner_id:
            return {"status": "error", "message": f"{owner_col} is required", "status_code": 400}

        sb = get_supabase()

        cities = (
            sb.table("cities").select("id, city_code, city_name").order("city_name").execute()
        ).data or []

        profiles = (
            sb.table(table)
            .select("id, destination_station_id, rate, rate_unit, default_payment_mode, is_active")
            .eq(owner_col, owner_id)
            .execute()
        ).data or []
        profile_map = {p["destination_station_id"]: p for p in profiles if p.get("destination_station_id")}

        rows = []
        for c in cities:
            p = profile_map.get(c["id"])
            rows.append({
                "city_id": c["id"],
                "city_code": c.get("city_code"),
                "city_name": c.get("city_name"),
                "has_profile": bool(p),
                "profile_id": p["id"] if p else None,
                "rate": p.get("rate") if p else None,
                "rate_unit": p.get("rate_unit") if p else None,
                "default_payment_mode": p.get("default_payment_mode") if p else None,
                "is_active": p.get("is_active") if p else None,
            })

        return {
            "status": "success",
            "data": {
                owner_col: owner_id,
                "total_stations": len(rows),
                "stations_with_profile": sum(1 for r in rows if r["has_profile"]),
                "stations": rows,
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list stations: {str(e)}",
            "status_code": 500,
        }
