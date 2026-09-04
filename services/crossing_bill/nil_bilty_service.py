"""
Nil Bilty Service
==================
Finds bilties for a transport that were DISPATCHED within a date range but
have NO pohonch (crossing-challan) proof at all — the "NILL" bucket that
needs a fresh pohonch created for them (e.g. "NILL-AUG-KBT").

Critical rule: "dispatched in a date range" is judged by the CARRYING
CHALLAN's dispatch_date (challan_details.dispatch_date), NOT by bilty_date
or created_at. A bilty made on 30 July but dispatched on 1 Aug belongs to
August. An August bilty dispatched only in September belongs to September,
not August. Bilties never dispatched (no challan, or challan not yet
dispatched) fall in neither bucket and are not returned here.

Flow:
  1. Find challans dispatched in [from_date, to_date].
  2. Find every GR carried by those challans (transit_details).
  3. Keep only GRs belonging to the given transport (transport_gst match,
     looked up in `bilty` or `station_bilty_summary` — whichever table the
     GR came from), optionally restricted to one destination station/city.
  4. Cross-reference every pohonch's bilty_metadata to see which of those
     GRs already have pohonch proof, and whether that pohonch is already
     rolled into a crossing bill.
  5. Return three buckets: no_pohonch (the "NILL" list), pohonch_unbilled,
     pohonch_billed — plus a ready-to-post payload for
     POST /api/pohonch/create for the no_pohonch bucket.
"""
from __future__ import annotations
from datetime import date, timedelta
from services.supabase_client import get_supabase

PAGE_SIZE = 1000


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _next_day(date_str: str) -> str:
    return str(date.fromisoformat(date_str) + timedelta(days=1))


def _resolve_city_info(sb, station_name: str) -> tuple[list[str], list[str]]:
    """Cities whose name partially matches station_name → (city_ids, city_codes)."""
    res = (
        sb.table("cities")
        .select("id, city_name, city_code")
        .ilike("city_name", f"%{station_name.strip()}%")
        .execute()
    )
    rows = res.data or []
    city_ids = [r["id"] for r in rows]
    city_codes = [r["city_code"] for r in rows if r.get("city_code")]
    return city_ids, city_codes


def _empty_result(transport_gstin: str, from_date: str, to_date: str, station_name: str | None) -> dict:
    gstin = (transport_gstin or "").strip().upper()
    return {
        "transport_gstin": gstin,
        "from_date": from_date,
        "to_date": to_date,
        "station_name": station_name,
        "matched_city_ids": [],
        "totals": {"dispatched_matching": 0, "no_pohonch": 0, "pohonch_unbilled": 0, "pohonch_billed": 0},
        "no_pohonch": {
            "bilties": [], "total_weight": 0, "total_amount": 0, "total_packages": 0,
            "suggested_pohonch_payload": {
                "transport_name": None, "transport_gstin": gstin,
                "challan_nos": [], "gr_items": [], "pohonch_prefix": None,
            },
        },
        "pohonch_unbilled": [],
        "pohonch_billed": [],
    }


def find_nil_bilties(
    transport_gstin: str,
    from_date: str,
    to_date: str,
    station_name: str | None = None,
) -> dict:
    if not transport_gstin:
        return {"status": "error", "message": "transport_gstin is required", "status_code": 400}
    if not from_date or not to_date:
        return {"status": "error", "message": "from_date and to_date are required (YYYY-MM-DD)", "status_code": 400}
    try:
        date.fromisoformat(from_date)
        date.fromisoformat(to_date)
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD", "status_code": 400}

    sb = get_supabase()
    gstin = transport_gstin.strip().upper()
    to_date_excl = _next_day(to_date)

    # ── Optional destination filter ────────────────────────────────────────
    city_ids: list[str] = []
    city_codes: list[str] = []
    if station_name:
        city_ids, city_codes = _resolve_city_info(sb, station_name)
        if not city_ids:
            return {"status": "error", "message": f"No city found matching '{station_name}'", "status_code": 404}

    # ── 1. Challans DISPATCHED within the window ───────────────────────────
    challan_nos: list[str] = []
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        res = (
            sb.table("challan_details")
            .select("challan_no")
            .eq("is_active", True)
            .eq("is_dispatched", True)
            .gte("dispatch_date", from_date)
            .lt("dispatch_date", to_date_excl)
            .range(lo, hi)
            .execute()
        )
        batch = res.data or []
        challan_nos.extend(c["challan_no"] for c in batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1

    if not challan_nos:
        return {"status": "success", "message": "No challans dispatched in this date range",
                "data": _empty_result(transport_gstin, from_date, to_date, station_name)}

    # ── 2. Every GR carried by those challans ───────────────────────────────
    transit_rows = []
    for chunk in _chunks(challan_nos, 200):
        res = (
            sb.table("transit_details")
            .select("gr_no, bilty_id, challan_no")
            .in_("challan_no", chunk)
            .execute()
        )
        transit_rows.extend(res.data or [])

    if not transit_rows:
        return {"status": "success", "message": "No bilties found on those dispatched challans",
                "data": _empty_result(transport_gstin, from_date, to_date, station_name)}

    gr_challan_map = {r["gr_no"]: r["challan_no"] for r in transit_rows}
    bilty_ids = [r["bilty_id"] for r in transit_rows if r.get("bilty_id")]
    station_grs = [r["gr_no"] for r in transit_rows if not r.get("bilty_id")]

    # ── 3. Keep only GRs belonging to this transport (+ optional station) ──
    matched: dict[str, dict] = {}

    BILTY_COLS = (
        "id, gr_no, transport_gst, transport_name, to_city_id, "
        "consignor_name, consignee_name, wt, total, no_of_pkg, "
        "bilty_date, e_way_bill, pvt_marks"
    )
    for chunk in _chunks(bilty_ids, 200):
        q = (
            sb.table("bilty")
            .select(BILTY_COLS)
            .in_("id", chunk)
            .eq("is_active", True)
            .eq("transport_gst", gstin)
        )
        if city_ids:
            q = q.in_("to_city_id", city_ids)
        for b in q.execute().data or []:
            gr = b["gr_no"]
            matched[gr] = {
                "gr_no": gr,
                "source_table": "bilty",
                "challan_no": gr_challan_map.get(gr),
                "transport_name": b.get("transport_name") or "",
                "consignor_name": b.get("consignor_name") or "",
                "consignee_name": b.get("consignee_name") or "",
                "weight": b.get("wt") or 0,
                "amount": b.get("total") or 0,
                "packages": b.get("no_of_pkg") or 0,
                "bilty_date": (b.get("bilty_date") or "")[:10],
                "e_way_bill": b.get("e_way_bill") or "",
                "pvt_marks": b.get("pvt_marks") or "",
                "city_id": b.get("to_city_id"),
            }

    SBS_COLS = (
        "gr_no, transport_gst, transport_name, city_id, station, "
        "consignor, consignee, weight, amount, no_of_packets, "
        "created_at, e_way_bill, pvt_marks"
    )
    for chunk in _chunks(station_grs, 200):
        q = (
            sb.table("station_bilty_summary")
            .select(SBS_COLS)
            .in_("gr_no", chunk)
            .eq("transport_gst", gstin)
        )
        if city_ids and city_codes:
            ids_str = ",".join(str(c) for c in city_ids)
            codes_str = ",".join(str(c) for c in city_codes)
            q = q.or_(f"city_id.in.({ids_str}),station.in.({codes_str})")
        elif city_ids:
            q = q.in_("city_id", city_ids)
        for s in q.execute().data or []:
            gr = s["gr_no"]
            matched[gr] = {
                "gr_no": gr,
                "source_table": "station_bilty_summary",
                "challan_no": gr_challan_map.get(gr),
                "transport_name": s.get("transport_name") or "",
                "consignor_name": s.get("consignor") or "",
                "consignee_name": s.get("consignee") or "",
                "weight": s.get("weight") or 0,
                "amount": s.get("amount") or 0,
                "packages": s.get("no_of_packets") or 0,
                "bilty_date": (s.get("created_at") or "")[:10],
                "e_way_bill": s.get("e_way_bill") or "",
                "pvt_marks": s.get("pvt_marks") or "",
                "city_id": s.get("city_id"),
            }

    if not matched:
        return {
            "status": "success",
            "message": "No matching bilties for this transport (and station) on those dispatched challans",
            "data": _empty_result(transport_gstin, from_date, to_date, station_name),
        }

    # ── 4. Cross-reference pohonch coverage (pohonch table is small — full scan,
    #        same accepted pattern used by kaat_update_service._sync_pohonch_metadata) ──
    all_pohonch = sb.table("pohonch").select("id, pohonch_number, bilty_metadata, crossing_bill_id").execute()

    gr_pohonch_map: dict[str, dict] = {}
    for p in all_pohonch.data or []:
        for entry in (p.get("bilty_metadata") or []):
            gr = entry.get("gr_no")
            if gr and gr not in gr_pohonch_map:
                gr_pohonch_map[gr] = {
                    "pohonch_number": p.get("pohonch_number"),
                    "crossing_bill_id": p.get("crossing_bill_id"),
                }

    billed_ids = {v["crossing_bill_id"] for v in gr_pohonch_map.values() if v.get("crossing_bill_id")}
    bill_no_map: dict[str, str] = {}
    if billed_ids:
        res = sb.table("crossing_bill").select("id, bill_no").in_("id", list(billed_ids)).execute()
        bill_no_map = {b["id"]: b["bill_no"] for b in (res.data or [])}

    no_pohonch, pohonch_unbilled, pohonch_billed = [], [], []
    for gr, row in matched.items():
        info = gr_pohonch_map.get(gr)
        if not info:
            no_pohonch.append(row)
        elif info.get("crossing_bill_id"):
            r = dict(row)
            r["pohonch_number"] = info["pohonch_number"]
            r["crossing_bill_id"] = info["crossing_bill_id"]
            r["bill_no"] = bill_no_map.get(info["crossing_bill_id"])
            pohonch_billed.append(r)
        else:
            r = dict(row)
            r["pohonch_number"] = info["pohonch_number"]
            pohonch_unbilled.append(r)

    no_pohonch.sort(key=lambda r: (r.get("bilty_date") or "", r["gr_no"]))
    pohonch_unbilled.sort(key=lambda r: (r.get("bilty_date") or "", r["gr_no"]))
    pohonch_billed.sort(key=lambda r: (r.get("bilty_date") or "", r["gr_no"]))

    # ── 5. Ready-to-post payload for POST /api/pohonch/create ──────────────
    gr_items = [{"gr_no": r["gr_no"], "pohonch_bilty": str(i + 1)} for i, r in enumerate(no_pohonch)]
    nil_challan_nos = sorted({r["challan_no"] for r in no_pohonch if r.get("challan_no")})

    return {
        "status": "success",
        "data": {
            "transport_gstin": gstin,
            "from_date": from_date,
            "to_date": to_date,
            "station_name": station_name,
            "matched_city_ids": city_ids,
            "totals": {
                "dispatched_matching": len(matched),
                "no_pohonch": len(no_pohonch),
                "pohonch_unbilled": len(pohonch_unbilled),
                "pohonch_billed": len(pohonch_billed),
            },
            "no_pohonch": {
                "bilties": no_pohonch,
                "total_weight": round(sum(r["weight"] for r in no_pohonch), 2),
                "total_amount": round(sum(r["amount"] for r in no_pohonch), 2),
                "total_packages": sum(r["packages"] for r in no_pohonch),
                "suggested_pohonch_payload": {
                    "transport_name": no_pohonch[0]["transport_name"] if no_pohonch else None,
                    "transport_gstin": gstin,
                    "challan_nos": nil_challan_nos,
                    "gr_items": gr_items,
                    "pohonch_prefix": None,
                },
            },
            "pohonch_unbilled": pohonch_unbilled,
            "pohonch_billed": pohonch_billed,
        },
    }
