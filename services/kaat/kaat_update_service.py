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
# Sync updated kaat values back into pohonch.bilty_metadata
# ---------------------------------------------------------------------------

def _sync_pohonch_metadata(sb, updates: list[dict]) -> int:
    """
    For each updated gr_no, find every pohonch row whose bilty_metadata
    contains that gr_no and patch kaat, pf, dd, kaat_rate in-place.

    `updates` is a list of dicts with keys:
        gr_no, kaat, pf, kaat_rate (optional), kaat_dd (optional)

    Returns number of pohonch rows touched.
    """
    if not updates:
        return 0

    # Build a lookup: gr_no → updated values
    update_map: dict[str, dict] = {}
    for u in updates:
        update_map[u["gr_no"]] = u

    gr_nos = list(update_map.keys())

    # Fetch all pohonch rows that might contain any of these gr_nos.
    # We check every pohonch row (pohonch table is small) for simplicity.
    all_pohonch = sb.table("pohonch").select("id, pohonch_number, bilty_metadata").execute()

    touched = 0
    for row in (all_pohonch.data or []):
        meta = row.get("bilty_metadata") or []
        changed = False
        new_meta = []
        for entry in meta:
            gr = entry.get("gr_no")
            if gr in update_map:
                upd = update_map[gr]
                new_entry = dict(entry)
                new_entry["kaat"]  = upd.get("kaat",  entry.get("kaat"))
                new_entry["pf"]    = upd.get("pf",    entry.get("pf"))
                # amount in pohonch metadata = pf + kaat + dd
                dd = upd.get("kaat_dd") if upd.get("kaat_dd") is not None else entry.get("dd", 0)
                new_entry["dd"]    = dd
                new_entry["amount"] = round(
                    (new_entry["pf"] or 0) + (new_entry["kaat"] or 0) + (dd or 0), 2
                )
                if upd.get("kaat_rate") is not None:
                    new_entry["kaat_rate"] = upd["kaat_rate"]
                changed = True
                new_meta.append(new_entry)
            else:
                new_meta.append(entry)

        if changed:
            sb.table("pohonch").update({"bilty_metadata": new_meta}).eq("id", row["id"]).execute()
            touched += 1

    return touched


# ---------------------------------------------------------------------------
# Helpers to fetch city IDs matching a station name
# ---------------------------------------------------------------------------

def _resolve_city_info(sb, station_name: str) -> tuple[list[str], list[str]]:
    """
    Return (city_ids, city_codes) for cities whose name matches station_name
    (case-insensitive partial match).
    city_codes are the short codes stored in station_bilty_summary.station column.
    """
    res = (
        sb.table("cities")
        .select("id, city_name, city_code")
        .ilike("city_name", f"%{station_name.strip()}%")
        .execute()
    )
    rows = res.data or []
    city_ids   = [r["id"]        for r in rows]
    city_codes = [r["city_code"] for r in rows if r.get("city_code")]
    return city_ids, city_codes


def _resolve_city_ids(sb, station_name: str) -> list[str]:
    """Return all city IDs whose name matches station_name (case-insensitive partial)."""
    ids, _ = _resolve_city_info(sb, station_name)
    return ids


# ---------------------------------------------------------------------------
# Fetch bilties for a transport + date range, optionally filtered by city IDs
# ---------------------------------------------------------------------------

def _fetch_bilty_gr_info(sb, transport_gstin: str, from_date: str, to_date: str,
                          city_ids: Optional[list] = None) -> list[dict]:
    """
    Returns list of {gr_no, wt, total, to_city_id, payment_mode} from bilty table.
    Optionally restricted to specific city_ids.
    """
    rows, page = [], 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("bilty")
            .select("gr_no, wt, total, to_city_id, payment_mode")
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
                        city_ids: Optional[list] = None,
                        city_codes: Optional[list] = None) -> list[dict]:
    """
    Returns list of {gr_no, wt, total, to_city_id, payment_mode}
    from station_bilty_summary table (payment_status normalised to payment_mode).

    Destination filtering uses city_id OR station (city code) because some
    station bilties are created with city_id=NULL and only a short city code
    stored in the station column.
    """
    rows, page = [], 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("station_bilty_summary")
            .select("gr_no, weight, amount, city_id, payment_status")
            .eq("transport_gst", transport_gstin.strip().upper())
            .gte("created_at", from_date)
            .lt("created_at", to_date_exclusive)
            .range(lo, hi)
        )
        if city_ids and city_codes:
            # OR: match by UUID city_id (when set) or by city code in station column.
            ids_str   = ",".join(str(cid) for cid in city_ids)
            codes_str = ",".join(str(c)   for c   in city_codes)
            q = q.or_(f"city_id.in.({ids_str}),station.in.({codes_str})")
        elif city_ids:
            q = q.in_("city_id", city_ids)
        elif city_codes:
            q = q.in_("station", city_codes)
        batch = q.execute().data or []
        # normalise keys
        rows.extend({
            "gr_no": r["gr_no"],
            "wt": r.get("weight") or 0,
            "total": r.get("amount") or 0,
            "to_city_id": r.get("city_id"),
            "payment_mode": r.get("payment_status") or "to-pay",
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

    city_ids, city_codes = _resolve_city_info(sb, station_name)

    # Fetch bilties from both tables.
    # bilty table uses to_city_id (UUID FK) — needs city_ids.
    # station_bilty_summary uses city_id (UUID FK) OR station (city code) — needs both.
    bilty_rows = _fetch_bilty_gr_info(sb, transport_gstin, from_date, to_date, city_ids) if city_ids else []
    sbs_rows   = _fetch_sbs_gr_info(sb, transport_gstin, from_date, to_date_excl, city_ids, city_codes)

    seen: set[str] = set()
    gr_info: dict[str, dict] = {}
    for r in bilty_rows:
        gr = r["gr_no"]
        if gr not in seen:
            seen.add(gr)
            gr_info[gr] = {
                "wt": r.get("wt") or 0,
                "total": r.get("total") or 0,
                "payment_mode": r.get("payment_mode") or "to-pay",
            }
    for r in sbs_rows:
        gr = r["gr_no"]
        if gr not in seen:
            seen.add(gr)
            gr_info[gr] = {
                "wt": r.get("wt") or 0,
                "total": r.get("total") or 0,
                "payment_mode": r.get("payment_mode") or "to-pay",
            }

    if not gr_info:
        return {
            "status": "success",
            "message": f"No bilties found for station '{station_name}' in the given range",
            "updated_count": 0,
            "updated": [],
        }

    # Fetch existing dd_chrg for all gr_nos so pf = total - kaat - dd is correct
    existing_dd: dict[str, float] = {}
    for chunk in _chunks(list(gr_info.keys()), PAGE_SIZE):
        dd_rows = sb.table("bilty_wise_kaat").select("gr_no, dd_chrg").in_("gr_no", chunk).execute()
        for r in (dd_rows.data or []):
            existing_dd[r["gr_no"]] = r.get("dd_chrg") or 0

    # Recalculate and update each GR in bilty_wise_kaat
    updated = []
    not_in_kaat = []

    for gr_no, info in gr_info.items():
        wt           = info["wt"]
        total        = info["total"]
        payment_mode = info.get("payment_mode", "to-pay")
        # dd: use new value if caller provided it, else keep existing
        dd    = new_kaat_dd if new_kaat_dd is not None else existing_dd.get(gr_no, 0)
        kaat  = round(wt * new_kaat_rate, 2)
        # paid bilties: pf is negative (transport owes the consignor)
        # paid: transport already collected freight at source, so pf = -kaat only
        # to-pay/foc: transport collects at destination, keeps freight-kaat-dd
        pf = round(-kaat, 2) if payment_mode == "paid" else round(total - kaat - dd, 2)

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
                "kaat_dd": dd,
            })
        else:
            not_in_kaat.append(gr_no)

    # Sync updated values into pohonch bilty_metadata
    pohonch_touched = _sync_pohonch_metadata(sb, updated)

    return {
        "status": "success",
        "transport_gstin": transport_gstin.upper(),
        "station_name": station_name,
        "city_ids_matched": city_ids,
        "city_codes_matched": city_codes,
        "from_date": from_date,
        "to_date": to_date,
        "new_kaat_rate": new_kaat_rate,
        **({"new_kaat_dd": new_kaat_dd} if new_kaat_dd is not None else {}),
        "updated_count": len(updated),
        "skipped_count": len(not_in_kaat),
        "skipped_gr_nos": not_in_kaat,
        "pohonch_rows_synced": pohonch_touched,
        "updated": updated,
    }


# ---------------------------------------------------------------------------
# 2. Bulk update kaat by explicit GR list
# ---------------------------------------------------------------------------

def bulk_update_kaat_by_gr_nos(
    gr_nos: list[str],
    new_kaat_rate: float,
    new_kaat_dd: Optional[float] = None,
) -> dict:
    """
    Update kaat for an explicit list of GR numbers.
    Fetches weight/total/payment_mode from bilty or station_bilty_summary,
    recalculates kaat = weight * new_kaat_rate, pf = total - kaat - dd,
    then writes to bilty_wise_kaat and syncs pohonch.bilty_metadata.

    Returns a summary with per-GR details.
    """
    if not gr_nos:
        return {"status": "error", "message": "gr_nos list is required", "status_code": 400}
    if new_kaat_rate < 0:
        return {"status": "error", "message": "new_kaat_rate must be >= 0", "status_code": 400}

    # Deduplicate preserving order
    seen_input: set[str] = set()
    unique_gr_nos = [g for g in gr_nos if g and not seen_input.add(g)]  # type: ignore[func-returns-value]

    sb = get_supabase()

    # ── 1. Fetch weight, total, payment_mode from bilty table ─────────────
    gr_info: dict[str, dict] = {}
    for chunk in _chunks(unique_gr_nos, PAGE_SIZE):
        res = (
            sb.table("bilty")
            .select("gr_no, wt, total, payment_mode")
            .eq("is_active", True)
            .in_("gr_no", chunk)
            .execute()
        )
        for r in (res.data or []):
            gr = r["gr_no"]
            if gr not in gr_info:
                gr_info[gr] = {
                    "wt": r.get("wt") or 0,
                    "total": r.get("total") or 0,
                    "payment_mode": r.get("payment_mode") or "to-pay",
                }

    # ── 2. Fallback to station_bilty_summary for missing GRs ──────────────
    missing = [g for g in unique_gr_nos if g not in gr_info]
    if missing:
        for chunk in _chunks(missing, PAGE_SIZE):
            res = (
                sb.table("station_bilty_summary")
                .select("gr_no, weight, amount, payment_status")
                .in_("gr_no", chunk)
                .execute()
            )
            for r in (res.data or []):
                gr = r["gr_no"]
                if gr not in gr_info:
                    gr_info[gr] = {
                        "wt": r.get("weight") or 0,
                        "total": r.get("amount") or 0,
                        "payment_mode": r.get("payment_status") or "to-pay",
                    }

    # GRs not found in either table
    not_found = [g for g in unique_gr_nos if g not in gr_info]

    # ── 3. Fetch existing dd_chrg so pf stays correct when dd not supplied ─
    existing_dd: dict[str, float] = {}
    for chunk in _chunks(unique_gr_nos, PAGE_SIZE):
        dd_rows = (
            sb.table("bilty_wise_kaat")
            .select("gr_no, dd_chrg")
            .in_("gr_no", chunk)
            .execute()
        )
        for r in (dd_rows.data or []):
            existing_dd[r["gr_no"]] = r.get("dd_chrg") or 0

    # ── 4. Recalculate and update bilty_wise_kaat ─────────────────────────
    updated = []
    not_in_kaat = []

    for gr_no in unique_gr_nos:
        if gr_no not in gr_info:
            continue
        info         = gr_info[gr_no]
        wt           = info["wt"]
        total        = info["total"]
        payment_mode = info["payment_mode"]
        dd           = new_kaat_dd if new_kaat_dd is not None else existing_dd.get(gr_no, 0)
        kaat         = round(wt * new_kaat_rate, 2)
        pf           = round(-kaat, 2) if payment_mode == "paid" else round(total - kaat - dd, 2)

        payload: dict = {"actual_kaat_rate": new_kaat_rate, "kaat": kaat, "pf": pf}
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
                "kaat_dd": dd,
            })
        else:
            not_in_kaat.append(gr_no)

    # ── 5. Sync updated values into pohonch bilty_metadata ────────────────
    pohonch_touched = _sync_pohonch_metadata(sb, updated)

    return {
        "status": "success",
        "new_kaat_rate": new_kaat_rate,
        **({"new_kaat_dd": new_kaat_dd} if new_kaat_dd is not None else {}),
        "requested_count": len(unique_gr_nos),
        "updated_count": len(updated),
        "skipped_count": len(not_in_kaat),
        "not_found_count": len(not_found),
        "skipped_gr_nos": not_in_kaat,
        "not_found_gr_nos": not_found,
        "pohonch_rows_synced": pohonch_touched,
        "updated": updated,
    }


# ---------------------------------------------------------------------------
# 3. Update a single GR
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
                                pf   = total - kaat - dd
      - If kaat is given directly (no kaat_rate) → use it as-is,
                                pf = total - kaat - dd (fetched from bilty/sbs)
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

    # Fetch weight, total and payment_mode from bilty or station_bilty_summary
    wt, total, payment_mode = None, None, "to-pay"
    bilty_res = (
        sb.table("bilty")
        .select("wt, total, payment_mode")
        .eq("gr_no", gr_no)
        .eq("is_active", True)
        .limit(1)
        .execute()
    )
    if bilty_res.data:
        wt           = bilty_res.data[0].get("wt") or 0
        total        = bilty_res.data[0].get("total") or 0
        payment_mode = bilty_res.data[0].get("payment_mode") or "to-pay"
    else:
        sbs_res = (
            sb.table("station_bilty_summary")
            .select("weight, amount, payment_status")
            .eq("gr_no", gr_no)
            .limit(1)
            .execute()
        )
        if sbs_res.data:
            wt           = sbs_res.data[0].get("weight") or 0
            total        = sbs_res.data[0].get("amount") or 0
            payment_mode = sbs_res.data[0].get("payment_status") or "to-pay"

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

    # Determine new pf: pf = total - kaat - dd (negative for paid bilties)
    if pf_override is not None:
        payload["pf"] = pf_override
    elif "kaat" in payload and total is not None:
        # use incoming kaat_dd if provided, otherwise keep existing dd_chrg
        dd_for_pf = kaat_dd if kaat_dd is not None else (current.get("dd_chrg") or 0)
        payload["pf"] = round(-new_kaat, 2) if payment_mode == "paid" else round(total - new_kaat - dd_for_pf, 2)

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
    result = {
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

    # Sync into pohonch bilty_metadata
    _sync_pohonch_metadata(sb, [{
        "gr_no": gr_no,
        "kaat": row.get("kaat"),
        "pf": row.get("pf"),
        "kaat_rate": row.get("actual_kaat_rate"),
        "kaat_dd": row.get("dd_chrg"),
    }])

    return result
