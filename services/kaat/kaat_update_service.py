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

    # Every kaat edit (single, bulk by GR list, bulk by station, apply station rate) ends here —
    # keep transport ledgers in step with the new kaat / pf (background, never blocks or fails this)
    try:
        from services.thread_pool import shared_pool
        from services.ledger.transport_ledger_service import sync_transport_bilties
        shared_pool.submit(sync_transport_bilties, [u["gr_no"] for u in updates if u.get("gr_no")])
    except Exception as exc:
        print(f"transport ledger sync not scheduled: {exc}")

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
        # paid: pf = -kaat (even when kaat=0 → pf=0)
        # to-pay/foc: pf = freight - kaat - dd
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
    """Old signature, kept for callers. Goes through update_gr_kaat (charges included)."""
    return update_gr_kaat(gr_no, {"kaat_rate": kaat_rate, "kaat": kaat, "kaat_dd": kaat_dd, "pf": pf_override})


# ════════════════════════════════════════════════════════════════════════════
# Full per-bilty kaat edit: rate type (per kg / per pkg), rate, bilty charge,
# labour charge, ewb / other charge, DD, and kaat / pf overrides.
#
#   base  = weight x rate_per_kg            (rate_type = per_kg)
#         = packages x rate_per_pkg         (rate_type = per_pkg)
#   kaat  = base + bilty_chrg + labour_chrg + ewb_chrg + other_chrg
#   pf    = total - kaat - dd               (to-pay / foc)
#         = -kaat                           (paid)
# Same formula the stored data already follows (e.g. A07478: 420 kg x 2 + 20 = 860).
# ════════════════════════════════════════════════════════════════════════════
KAAT_SELECT = ("gr_no, rate_type, rate_per_kg, rate_per_pkg, actual_kaat_rate, kaat, pf, dd_chrg, "
               "bilty_chrg, labour_chrg, ewb_chrg, other_chrg")
CHARGE_FIELDS = ("bilty_chrg", "labour_chrg", "ewb_chrg", "other_chrg")
EDIT_FIELDS = ("rate_type", "kaat_rate", "rate_per_kg", "rate_per_pkg", "labour_per_pkg",
               "kaat_dd", "dd_chrg", "kaat", "pf") + CHARGE_FIELDS
_CALC_FIELDS = ("rate_type", "kaat_rate", "rate_per_kg", "rate_per_pkg", "labour_per_pkg") + CHARGE_FIELDS


def _f(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _given(data: dict, k: str) -> bool:
    return data.get(k) is not None and data.get(k) != ""


def fetch_gr_info(sb, gr_no: str) -> dict | None:
    """Weight, packages, freight total and payment mode from bilty, else station_bilty_summary."""
    b = (sb.table("bilty").select("wt, no_of_pkg, total, payment_mode").eq("gr_no", gr_no)
         .eq("is_active", True).limit(1).execute().data)
    if b:
        b = b[0]
        return {"weight": _f(b.get("wt")), "packages": _f(b.get("no_of_pkg")), "total": _f(b.get("total")),
                "payment_mode": (b.get("payment_mode") or "to-pay").strip().lower(), "source_table": "bilty"}
    s = (sb.table("station_bilty_summary").select("weight, no_of_packets, amount, payment_status")
         .eq("gr_no", gr_no).limit(1).execute().data)
    if s:
        s = s[0]
        return {"weight": _f(s.get("weight")), "packages": _f(s.get("no_of_packets")), "total": _f(s.get("amount")),
                "payment_mode": (s.get("payment_status") or "to-pay").strip().lower(),
                "source_table": "station_bilty_summary"}
    return None


def kaat_detail(row: dict | None, info: dict | None) -> dict:
    """The breakdown the edit modal shows (current values)."""
    row, info = row or {}, info or {}
    rate_type = row.get("rate_type") or "per_kg"
    per_kg = _f(row.get("actual_kaat_rate")) or _f(row.get("rate_per_kg"))
    return {
        "rate_type": rate_type,
        "rate": _f(row.get("rate_per_pkg")) if rate_type == "per_pkg" else per_kg,
        "rate_per_kg": per_kg,
        "rate_per_pkg": _f(row.get("rate_per_pkg")),
        **{k: _f(row.get(k)) for k in CHARGE_FIELDS},
        "dd_chrg": _f(row.get("dd_chrg")),
        "kaat": _f(row.get("kaat")),
        "pf": _f(row.get("pf")),
        "weight": info.get("weight"),
        "packages": info.get("packages"),
        "total": info.get("total"),
        "payment_mode": info.get("payment_mode"),
        "has_kaat_row": bool(row),
    }


def compute_kaat(current: dict | None, info: dict | None, data: dict) -> tuple[dict, dict]:
    """Merge the edit onto the current row and recalculate.
    Returns (payload for bilty_wise_kaat, breakdown with the new values). Raises ValueError on bad input."""
    current, info, data = current or {}, info or {}, data or {}
    if not any(_given(data, k) for k in EDIT_FIELDS):
        raise ValueError("Send at least one of: " + ", ".join(EDIT_FIELDS))

    rate_type = data.get("rate_type") if _given(data, "rate_type") else None
    if rate_type is None and _given(data, "kaat_rate"):
        rate_type = "per_kg"                    # old callers: kaat_rate alone always meant per kg
    rate_type = str(rate_type or current.get("rate_type") or "per_kg").strip().lower().replace("-", "_")
    rate_type = {"kg": "per_kg", "perkg": "per_kg", "pkg": "per_pkg", "perpkg": "per_pkg",
                 "per_package": "per_pkg", "package": "per_pkg"}.get(rate_type, rate_type)
    if rate_type not in ("per_kg", "per_pkg"):
        raise ValueError("rate_type must be 'per_kg' or 'per_pkg'")

    cur = kaat_detail(current, info)
    per_kg, per_pkg = cur["rate_per_kg"], cur["rate_per_pkg"]
    if _given(data, "rate_per_kg"):
        per_kg = _f(data["rate_per_kg"])
    if _given(data, "rate_per_pkg"):
        per_pkg = _f(data["rate_per_pkg"])
    if _given(data, "kaat_rate"):               # "the rate" for the chosen rate_type
        if rate_type == "per_kg":
            per_kg = _f(data["kaat_rate"])
        else:
            per_pkg = _f(data["kaat_rate"])

    charges = {k: (_f(data[k]) if _given(data, k) else cur[k]) for k in CHARGE_FIELDS}
    if _given(data, "labour_per_pkg"):          # labour charged per package
        charges["labour_chrg"] = round(_f(data["labour_per_pkg"]) * _f(info.get("packages")), 2)
    dd_key = "kaat_dd" if _given(data, "kaat_dd") else ("dd_chrg" if _given(data, "dd_chrg") else None)
    dd = _f(data[dd_key]) if dd_key else cur["dd_chrg"]

    recalc = any(_given(data, k) for k in _CALC_FIELDS)
    if recalc and not info:
        raise ValueError("Could not find weight / packages for this GR")
    wt, pkgs = _f(info.get("weight")), _f(info.get("packages"))
    base = round(wt * per_kg if rate_type == "per_kg" else pkgs * per_pkg, 2)

    if _given(data, "kaat"):                    # typed kaat wins over the formula
        kaat = round(_f(data["kaat"]), 2)
    elif recalc:
        kaat = round(base + sum(charges.values()), 2)
    else:
        kaat = cur["kaat"]

    if _given(data, "pf"):                      # typed pf wins
        pf = round(_f(data["pf"]), 2)
    elif recalc or _given(data, "kaat") or dd_key:
        mode = info.get("payment_mode") or "to-pay"
        pf = round(-kaat, 2) if mode == "paid" else round(_f(info.get("total")) - kaat - dd, 2)
    else:
        pf = cur["pf"]

    payload = {"rate_type": rate_type, "rate_per_pkg": per_pkg, "kaat": kaat, "pf": pf, "dd_chrg": dd, **charges}
    if rate_type == "per_kg":
        payload["actual_kaat_rate"] = per_kg
        payload["rate_per_kg"] = per_kg
    extra = "".join(f" + {charges[k]:g} {k.replace('_chrg', '')}" for k in CHARGE_FIELDS if charges[k])
    unit = f"{wt:g} kg x {per_kg:g}" if rate_type == "per_kg" else f"{pkgs:g} pkg x {per_pkg:g}"
    breakdown = {
        **kaat_detail({**current, **payload}, info),
        "base_kaat": base,
        "formula": f"{unit}{extra} = {round(base + sum(charges.values()), 2):g}"
                   + (f"  (kaat typed: {kaat:g})" if _given(data, "kaat") else ""),
    }
    return payload, breakdown


def get_gr_kaat(gr_no: str) -> dict:
    """Current kaat breakdown for one GR (prefills the edit modal)."""
    gr_no = (gr_no or "").strip()
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    sb = get_supabase()
    row = (sb.table("bilty_wise_kaat").select(KAAT_SELECT).eq("gr_no", gr_no).limit(1).execute().data or [None])[0]
    info = fetch_gr_info(sb, gr_no)
    if not row and not info:
        return {"status": "error", "message": f"GR '{gr_no}' not found", "status_code": 404}
    return {"status": "success", "gr_no": gr_no, "data": kaat_detail(row, info)}


def update_gr_kaat(gr_no: str, data: dict) -> dict:
    """Edit one GR's kaat in bilty_wise_kaat (formula above), then sync pohonch + transport ledgers."""
    gr_no = (gr_no or "").strip()
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    sb = get_supabase()
    row = (sb.table("bilty_wise_kaat").select(KAAT_SELECT).eq("gr_no", gr_no).limit(1).execute().data or [None])[0]
    if not row:
        return {"status": "error", "message": f"GR '{gr_no}' not found in bilty_wise_kaat", "status_code": 404}
    info = fetch_gr_info(sb, gr_no)
    try:
        payload, breakdown = compute_kaat(row, info, data or {})
    except ValueError as e:
        return {"status": "error", "message": str(e), "status_code": 400}
    if (data or {}).get("updated_by"):
        payload["updated_by"] = data["updated_by"]
    res = sb.table("bilty_wise_kaat").update(payload).eq("gr_no", gr_no).execute()
    if not res.data:
        return {"status": "error", "message": "Update failed — row not found or unchanged", "status_code": 500}
    _sync_pohonch_metadata(sb, [{"gr_no": gr_no, "kaat": payload["kaat"], "pf": payload["pf"],
                                 "kaat_rate": breakdown["rate"], "kaat_dd": payload["dd_chrg"]}])
    return {
        "status": "success", "gr_no": gr_no,
        # same keys as before, so the existing Kaat Update screen keeps working
        "updated": {"kaat_rate": breakdown["rate"], "kaat": payload["kaat"],
                    "kaat_dd": payload["dd_chrg"], "pf": payload["pf"]},
        "weight": (info or {}).get("weight"), "total": (info or {}).get("total"),
        "data": breakdown,
    }
