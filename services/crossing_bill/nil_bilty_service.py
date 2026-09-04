"""
Nil Bilty Service
==================
Finds bilties for a transport that were DISPATCHED within a date range but
have NO pohonch (crossing-challan) proof at all — the "NILL" bucket — and
can create a single catch-up pohonch for them.

Critical rule: "dispatched in a date range" is judged by the CARRYING
CHALLAN's dispatch_date (challan_details.dispatch_date), NOT by bilty_date
or created_at — AND with a 1-day transit-lag shift, because the truck
takes a day to actually reach the transport after dispatch. So a bilty's
"arrival month" = dispatch_date + 1 day, not dispatch_date itself:
  - Dispatched 31 July → arrives 1 Aug → counts as AUGUST (not July).
  - Dispatched 31 Aug   → arrives 1 Sep → counts as SEPTEMBER (not August).
  - A bilty made on 30 July but dispatched 1 Aug (arrives 2 Aug) → August.
Bilties never dispatched (no challan, or challan not yet dispatched) fall
in neither bucket and are not returned here.

The catch-up pohonch itself is created with the TRANSPORT'S OWN normal
prefix and numbering series (e.g. "KBF0103", continuing wherever that
transport's series already is) — never a special "NILL-..." pohonch
number. The "NILL-<MON>-<PREFIX>-<YY>" marker (e.g. "NILL-AUG-KBF-26") is
instead written into bilty_wise_kaat.pohonch_no AND pohonch_bilty for every
covered GR, purely as an audit tag distinguishing "retroactively caught
up" GRs from ones that got a pohonch through the normal daily flow. The
2-digit year keeps markers from different years distinct.

IMPORTANT — station_name is a destination filter, not a scoping unit for
catch-up creation: one transport's challans typically carry GRs to SEVERAL
destinations at once. Filtering by station_name and then creating a
catch-up pohonch from that PARTIAL result leaves every other destination's
nil bilties on the same challans uncovered. Omit station_name when the
goal is "give this transport full proof for this month" — use it only for
inspecting one destination's bilties. The API returns
`partial_scope_warning` whenever station_name is set, precisely to flag
this.

Flow (read, GET /api/crossing-bill/nil-bilties):
  1. Find challans dispatched in [from_date, to_date] (+ their dispatch_date).
  2. Find every GR carried by those challans (transit_details).
  3. Keep only GRs belonging to the given transport (transport_gst match on
     `bilty` or `station_bilty_summary`, whichever the GR came from), and
     optionally only a given destination station/city.
  4. Cross-reference every pohonch's bilty_metadata to see which of those
     GRs already have pohonch proof, and if so whether that pohonch is
     already rolled into a crossing bill.
  5. Also sample up to 5 bilties dispatched just BEFORE from_date and 5 just
     AFTER to_date (same transport/station) as visible boundary proof that
     the month-cutoff logic is dispatch-date driven, not bilty-date driven.

Flow (write, POST /api/crossing-bill/nil-bilties):
  1. Re-run the read flow to get the no_pohonch bucket.
  2. Resolve the transport's ESTABLISHED prefix from its own pohonch
     history (see _resolve_established_prefix) — falling back to a fresh
     name-derived guess only for a genuinely brand-new transport — and
     create one pohonch via create_pohonch_from_gr_items with that exact
     prefix, continuing its existing series.
  3. Tag bilty_wise_kaat.pohonch_no = "NILL-<MON>-<PREFIX>-<YY>" for every
     GR in the bucket (update-only — this backend never inserts kaat rows,
     same convention as kaat_update_service).
"""
from __future__ import annotations
import re
from collections import Counter
from datetime import date, timedelta
from services.supabase_client import get_supabase
from services.pohonch.pohonch_create_service import create_pohonch_from_gr_items, _make_prefix

PAGE_SIZE = 1000
BOUNDARY_LIMIT = 5
BOUNDARY_WINDOW_DAYS = 45

BILTY_COLS = (
    "id, gr_no, transport_gst, transport_name, to_city_id, "
    "consignor_name, consignee_name, wt, total, no_of_pkg, "
    "bilty_date, e_way_bill, pvt_marks"
)
SBS_COLS = (
    "gr_no, transport_gst, transport_name, city_id, station, "
    "consignor, consignee, weight, amount, no_of_packets, "
    "created_at, e_way_bill, pvt_marks"
)


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _next_day(date_str: str) -> str:
    return str(date.fromisoformat(date_str) + timedelta(days=1))


def _prev_day(date_str: str) -> str:
    return str(date.fromisoformat(date_str) - timedelta(days=1))


def _resolve_established_prefix(sb, transport_gstin: str, transport_name: str) -> str:
    """
    The prefix to use for a NEW pohonch for this transport. ALWAYS prefer
    the transport's own established prefix — extracted from the alphabetic
    lead-in of its existing pohonch_number history for this exact GSTIN —
    over a fresh name-derived guess.

    Why this matters: a transport's real prefix is often a chosen
    abbreviation/brand code that word-initials extraction cannot reliably
    reproduce. E.g. transport_name "CKT FRIEGHT CARRIER" (their established
    prefix is "CKT", 35 existing pohonch CKT0001..CKT0035) naively reduces
    to "C"+"F" = "CF" via _make_prefix (CARRIER is a skip-word, FRIEGHT
    isn't) — a completely disconnected series. Checking history first and
    only falling back to _make_prefix for a genuinely brand-new transport
    (no pohonch yet) avoids ever fragmenting an established series again.
    """
    res = (
        sb.table("pohonch")
        .select("pohonch_number")
        .eq("transport_gstin", transport_gstin)
        .execute()
    )
    counts: Counter[str] = Counter()
    for r in res.data or []:
        m = re.match(r"^([A-Za-z.]+)", r.get("pohonch_number") or "")
        if m:
            counts[m.group(1)] += 1
    if counts:
        return counts.most_common(1)[0][0]
    return _make_prefix(transport_name)


def _nill_marker(prefix: str, from_date: str) -> str:
    """'NILL-<MON>-<PREFIX>-<YY>' e.g. 'NILL-AUG-CKT-26' — the same audit
    marker used both as bilty_wise_kaat.pohonch_no AND as the pohonch_bilty
    ("P/B No.") value for every GR in a catch-up pohonch, so every place
    the GR shows up displays the same tag. The 2-digit year keeps markers
    from different years (e.g. Aug 2026 vs Aug 2027) distinct. `prefix`
    must be the transport's ESTABLISHED prefix — see
    _resolve_established_prefix — never a fresh name-derived guess."""
    d = date.fromisoformat(from_date)
    return f"NILL-{d.strftime('%b').upper()}-{prefix}-{d.strftime('%y')}"


def _arrival_date(dispatch_date_str: str | None) -> str | None:
    """dispatch_date + 1 day (truck transit lag) → the calendar date this
    bilty actually counts under."""
    if not dispatch_date_str:
        return None
    try:
        return str(date.fromisoformat(dispatch_date_str[:10]) + timedelta(days=1))
    except ValueError:
        return None


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


def _fetch_dispatched_challans(sb, lo_date: str, hi_date_excl: str) -> dict[str, str]:
    """{challan_no: dispatch_date} for active, dispatched challans in [lo_date, hi_date_excl)."""
    out: dict[str, str] = {}
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        res = (
            sb.table("challan_details")
            .select("challan_no, dispatch_date")
            .eq("is_active", True)
            .eq("is_dispatched", True)
            .gte("dispatch_date", lo_date)
            .lt("dispatch_date", hi_date_excl)
            .range(lo, hi)
            .execute()
        )
        batch = res.data or []
        for c in batch:
            out[c["challan_no"]] = c.get("dispatch_date")
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return out


def _match_bilties_for_challans(
    sb, challan_dispatch_map: dict[str, str], gstin: str,
    city_ids: list[str], city_codes: list[str],
) -> tuple[dict[str, dict], list[dict]]:
    """Given {challan_no: dispatch_date}, return ({gr_no: enriched_row}, transit_rows)
    for GRs on those challans belonging to this transport (+ optional city filter).
    transit_rows is returned too so callers can check for orphaned rows (see
    _find_orphaned_transit_grs)."""
    challan_nos = list(challan_dispatch_map.keys())
    if not challan_nos:
        return {}, []

    transit_rows = []
    for chunk in _chunks(challan_nos, 200):
        res = sb.table("transit_details").select("gr_no, bilty_id, challan_no").in_("challan_no", chunk).execute()
        transit_rows.extend(res.data or [])
    if not transit_rows:
        return {}, []

    gr_challan_map = {r["gr_no"]: r["challan_no"] for r in transit_rows}
    # IMPORTANT: join on the transit_details row's OWN gr_no, never on its
    # bilty_id FK value. Seen live: a transit_details row for a dispatched
    # challan whose bilty_id pointed at a COMPLETELY UNRELATED bilty (a
    # different GR, on a different, undispatched challan). Trusting that FK
    # for the join silently mislabeled results with the wrong GR's identity
    # while dropping the real one. bilty_id's presence/absence only decides
    # which source table to look in — gr_no is always the actual join key.
    bilty_grs = [r["gr_no"] for r in transit_rows if r.get("bilty_id")]
    station_grs = [r["gr_no"] for r in transit_rows if not r.get("bilty_id")]

    matched: dict[str, dict] = {}

    for chunk in _chunks(bilty_grs, 200):
        q = (
            sb.table("bilty").select(BILTY_COLS)
            .in_("gr_no", chunk).eq("is_active", True).eq("transport_gst", gstin)
        )
        if city_ids:
            q = q.in_("to_city_id", city_ids)
        for b in q.execute().data or []:
            gr = b["gr_no"]
            cno = gr_challan_map.get(gr)
            dispatch_date = challan_dispatch_map.get(cno)
            matched[gr] = {
                "gr_no": gr,
                "source_table": "bilty",
                "challan_no": cno,
                "dispatch_date": dispatch_date,
                "arrival_date": _arrival_date(dispatch_date),
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

    for chunk in _chunks(station_grs, 200):
        q = (
            sb.table("station_bilty_summary").select(SBS_COLS)
            .in_("gr_no", chunk).eq("transport_gst", gstin)
        )
        if city_ids and city_codes:
            ids_str = ",".join(str(c) for c in city_ids)
            codes_str = ",".join(str(c) for c in city_codes)
            q = q.or_(f"city_id.in.({ids_str}),station.in.({codes_str})")
        elif city_ids:
            q = q.in_("city_id", city_ids)
        for s in q.execute().data or []:
            gr = s["gr_no"]
            cno = gr_challan_map.get(gr)
            dispatch_date = challan_dispatch_map.get(cno)
            matched[gr] = {
                "gr_no": gr,
                "source_table": "station_bilty_summary",
                "challan_no": cno,
                "dispatch_date": dispatch_date,
                "arrival_date": _arrival_date(dispatch_date),
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

    return matched, transit_rows


def _find_orphaned_transit_grs(sb, transit_rows: list[dict]) -> list[str]:
    """
    GR numbers on a dispatched challan (i.e. present in transit_details)
    that have NO matching row in `bilty` OR `station_bilty_summary` at all
    — regardless of transport. This means the transit_details row itself
    is corrupted/orphaned (e.g. a bilty_id FK pointing at a completely
    unrelated bilty — the exact case that caused wrong GRs to appear in
    this report before the gr_no-based join fix above). Cheap: only checks
    the small set of GRs actually on dispatched challans in this window.
    """
    all_grs = [r["gr_no"] for r in transit_rows]
    if not all_grs:
        return []
    found: set[str] = set()
    for chunk in _chunks(all_grs, 200):
        b = sb.table("bilty").select("gr_no").in_("gr_no", chunk).execute()
        found.update(r["gr_no"] for r in (b.data or []))
        s = sb.table("station_bilty_summary").select("gr_no").in_("gr_no", chunk).execute()
        found.update(r["gr_no"] for r in (s.data or []))
    return sorted(set(all_grs) - found)


def _boundary_sample(
    sb, gstin: str, city_ids: list[str], city_codes: list[str],
    edge_date: str, direction: str,
    limit: int = BOUNDARY_LIMIT, window_days: int = BOUNDARY_WINDOW_DAYS,
) -> list[dict]:
    """
    direction='before': bilties whose challan dispatched in
        [edge_date - window_days, edge_date)  → closest to the boundary first.
    direction='after' : bilties whose challan dispatched in
        [edge_date, edge_date + window_days)  → closest to the boundary first.
    """
    if direction == "before":
        lo = str(date.fromisoformat(edge_date) - timedelta(days=window_days))
        hi = edge_date
    else:
        lo = edge_date
        hi = str(date.fromisoformat(edge_date) + timedelta(days=window_days))

    challan_map = _fetch_dispatched_challans(sb, lo, hi)
    matched, _ = _match_bilties_for_challans(sb, challan_map, gstin, city_ids, city_codes)
    rows = list(matched.values())
    rows.sort(key=lambda r: r.get("dispatch_date") or "", reverse=(direction == "before"))
    return rows[:limit]


def _empty_result(transport_gstin: str, from_date: str, to_date: str, station_name: str | None) -> dict:
    gstin = (transport_gstin or "").strip().upper()
    return {
        "transport_gstin": gstin,
        "from_date": from_date,
        "to_date": to_date,
        "station_name": station_name,
        "matched_city_ids": [],
        "orphaned_transit_gr_nos": [],
        "partial_scope_warning": (
            f"station_name='{station_name}' restricts results to that destination only. "
            "Omit station_name to catch a transport's full nil-bilty set." if station_name else None
        ),
        "totals": {"dispatched_matching": 0, "no_pohonch": 0, "pohonch_unbilled": 0, "pohonch_billed": 0},
        "no_pohonch": {
            "bilties": [], "total_weight": 0, "total_amount": 0, "total_packages": 0,
            "suggested_pohonch_payload": {
                "transport_name": None, "transport_gstin": gstin,
                "challan_nos": [], "gr_items": [], "pohonch_prefix": None, "nill_marker": None,
            },
        },
        "pohonch_unbilled": [],
        "pohonch_billed": [],
        "boundary_proof": {
            "before": {"window_days": BOUNDARY_WINDOW_DAYS, "edge_date": _prev_day(from_date), "bilties": []},
            "after": {"window_days": BOUNDARY_WINDOW_DAYS, "edge_date": to_date, "bilties": []},
        },
    }


# ─────────────────────────────────────────────────────────────────────────
# READ: GET /api/crossing-bill/nil-bilties
# ─────────────────────────────────────────────────────────────────────────

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

    # ── 1-day transit-lag shift: a bilty's "arrival month" = dispatch_date + 1
    #    day, so the DISPATCH-date window is the calendar window shifted back
    #    by one day. E.g. for August (01..31): dispatch_lo=31 Jul (inclusive,
    #    arrives 1 Aug), dispatch_hi_excl=31 Aug (exclusive — a 31 Aug
    #    dispatch arrives 1 Sep, so it belongs to September, not August). ──
    dispatch_lo = _prev_day(from_date)
    dispatch_hi_excl = to_date

    # ── Optional destination filter ────────────────────────────────────────
    city_ids: list[str] = []
    city_codes: list[str] = []
    if station_name:
        city_ids, city_codes = _resolve_city_info(sb, station_name)
        if not city_ids:
            return {"status": "error", "message": f"No city found matching '{station_name}'", "status_code": 404}

    # ── 1+2+3. Challans dispatched in-window → GRs → filtered to transport ──
    challan_map = _fetch_dispatched_challans(sb, dispatch_lo, dispatch_hi_excl)
    matched, transit_rows = _match_bilties_for_challans(sb, challan_map, gstin, city_ids, city_codes)

    # GRs on a dispatched challan with no underlying bilty/station_bilty_summary
    # row at all, for ANY transport — a corrupted/orphaned transit_details row
    # (see _find_orphaned_transit_grs). Surfaced so this doesn't silently
    # disappear from every report that touches these challans.
    orphaned_gr_nos = _find_orphaned_transit_grs(sb, transit_rows)

    # ── Boundary proof: closest 5 bilties just outside the window each side ─
    boundary_before = _boundary_sample(sb, gstin, city_ids, city_codes, dispatch_lo, "before")
    boundary_after = _boundary_sample(sb, gstin, city_ids, city_codes, dispatch_hi_excl, "after")

    if not matched:
        result = _empty_result(transport_gstin, from_date, to_date, station_name)
        result["boundary_proof"]["before"]["bilties"] = boundary_before
        result["boundary_proof"]["after"]["bilties"] = boundary_after
        result["orphaned_transit_gr_nos"] = orphaned_gr_nos
        return {"status": "success", "message": "No matching dispatched bilties for this transport (and station) in this window", "data": result}

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

    # ── 5. Ready-to-post payload for POST /api/crossing-bill/nil-bilties ───
    # pohonch_prefix is the transport's ESTABLISHED prefix (from its own
    # pohonch history — see _resolve_established_prefix), so
    # create_pohonch_from_gr_items continues its existing series exactly
    # (e.g. next after CKT0035 is CKT0036) — never a fresh, potentially
    # different name-derived guess, and never a "NILL-..." number.
    # pohonch_bilty ("P/B No.") is the SAME NILL-<MON>-<PREFIX>-<YY> marker
    # for every GR — not a running 1,2,3... count — so it matches what gets
    # written to bilty_wise_kaat.pohonch_no and shows up identically
    # wherever this GR is displayed.
    resolved_prefix = _resolve_established_prefix(sb, gstin, no_pohonch[0]["transport_name"]) if no_pohonch else None
    marker = _nill_marker(resolved_prefix, from_date) if no_pohonch else None
    gr_items = [{"gr_no": r["gr_no"], "pohonch_bilty": marker} for r in no_pohonch]
    nil_challan_nos = sorted({r["challan_no"] for r in no_pohonch if r.get("challan_no")})

    return {
        "status": "success",
        "data": {
            "transport_gstin": gstin,
            "from_date": from_date,
            "to_date": to_date,
            "station_name": station_name,
            "matched_city_ids": city_ids,
            "orphaned_transit_gr_nos": orphaned_gr_nos,
            "partial_scope_warning": (
                f"station_name='{station_name}' restricts results to that destination only. "
                "A challan/transport typically carries GRs to SEVERAL destinations at once — "
                "creating a catch-up pohonch from a station-scoped result will NOT cover the "
                "rest of that transport's nil bilties. Omit station_name to catch everything."
                if station_name else None
            ),
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
                    "pohonch_prefix": resolved_prefix,
                    "nill_marker": marker,
                },
            },
            "pohonch_unbilled": pohonch_unbilled,
            "pohonch_billed": pohonch_billed,
            "boundary_proof": {
                "before": {
                    "window_days": BOUNDARY_WINDOW_DAYS,
                    "edge_date": dispatch_lo,
                    "note": (
                        f"dispatched BEFORE {dispatch_lo} → arrives before {from_date} "
                        f"→ correctly excluded from this window"
                    ),
                    "bilties": boundary_before,
                },
                "after": {
                    "window_days": BOUNDARY_WINDOW_DAYS,
                    "edge_date": dispatch_hi_excl,
                    "note": (
                        f"dispatched ON/AFTER {dispatch_hi_excl} → arrives on/after {_next_day(to_date)} "
                        f"→ correctly excluded from this window"
                    ),
                    "bilties": boundary_after,
                },
            },
        },
    }


# ─────────────────────────────────────────────────────────────────────────
# WRITE: POST /api/crossing-bill/nil-bilties
# ─────────────────────────────────────────────────────────────────────────

def create_nil_catchup_pohonch(
    transport_gstin: str,
    from_date: str,
    to_date: str,
    station_name: str | None = None,
    created_by: str | None = None,
) -> dict:
    """
    Creates ONE catch-up pohonch covering every bilty in the no_pohonch
    bucket for [from_date, to_date] (+ station_name filter, if given).

    - The pohonch itself uses the transport's own normal auto-derived
      prefix and continues its existing numbering series (e.g. KBF0103) —
      never a "NILL-..." pohonch number.
    - Every GR's pohonch_bilty ("P/B No.") inside that pohonch's
      bilty_metadata is set to "NILL-<MON>-<PREFIX>" (e.g. "NILL-AUG-KBF")
      — not a running 1,2,3... count — so the marker is visible wherever
      that GR is shown.
    - Every covered GR's bilty_wise_kaat.pohonch_no is then tagged with the
      SAME marker. Update-only: GRs with no existing bilty_wise_kaat row
      are reported, not created (this backend never inserts kaat rows).
    """
    lookup = find_nil_bilties(transport_gstin, from_date, to_date, station_name)
    if lookup["status"] != "success":
        return lookup

    bucket = lookup["data"]["no_pohonch"]
    bilties = bucket["bilties"]
    if not bilties:
        return {
            "status": "error",
            "message": "No nil bilties found for the given criteria — nothing to create",
            "status_code": 404,
        }

    gstin = transport_gstin.strip().upper()
    transport_name = bilties[0]["transport_name"]
    payload = bucket["suggested_pohonch_payload"]

    created = create_pohonch_from_gr_items(
        transport_name=transport_name,
        transport_gstin=gstin,
        challan_nos=payload["challan_nos"],
        gr_items=payload["gr_items"],
        pohonch_prefix=payload["pohonch_prefix"],  # transport's ESTABLISHED prefix — never guessed fresh
        created_by=created_by,
    )
    if created.get("status") != "success":
        return created

    marker = payload["nill_marker"]  # same marker already used as pohonch_bilty for every GR

    sb = get_supabase()
    marked, missing = [], []
    for row in bilties:
        gr = row["gr_no"]
        res = sb.table("bilty_wise_kaat").update({"pohonch_no": marker}).eq("gr_no", gr).execute()
        (marked if res.data else missing).append(gr)

    return {
        "status": "success",
        "message": (
            f"Created pohonch {created['pohonch_number']} for {len(bilties)} nil bilties; "
            f"tagged bilty_wise_kaat.pohonch_no = '{marker}' on {len(marked)}"
            + (f" ({len(missing)} had no kaat row yet, skipped)" if missing else "")
        ),
        "pohonch_number": created["pohonch_number"],
        "kaat_marker": marker,
        "bilty_count": len(bilties),
        "kaat_marked_count": len(marked),
        "kaat_marked_gr_nos": marked,
        "kaat_missing_count": len(missing),
        "kaat_missing_gr_nos": missing,
        "data": created["data"],
    }


# ─────────────────────────────────────────────────────────────────────────
# READ (audit report): GET /api/crossing-bill/transport-challan-report
# ─────────────────────────────────────────────────────────────────────────

def get_transport_challan_report(
    transport_gstin: str,
    from_date: str,
    to_date: str,
) -> dict:
    """
    Full audit view: every challan dispatched for this transport in
    [from_date, to_date] (same 1-day transit-lag rule as find_nil_bilties),
    and EVERY bilty carried on those challans — across ALL destination
    stations, unlike find_nil_bilties which is filterable by one station —
    grouped by challan_no. Each bilty row carries:
      - station: destination city name (resolved from to_city_id / city_id)
      - kaat_pohonch_no: bilty_wise_kaat.pohonch_no AS-IS (whatever tag is
        stored there right now — e.g. a "NILL-AUG-KBF-26" catch-up marker,
        or null if nothing has ever been written there)
      - has_crossing_challan: whether REAL pohonch proof exists (the GR
        appears in some pohonch.bilty_metadata) — this is the authoritative
        proof-exists check, independent of whatever kaat_pohonch_no says
      - pohonch_number / is_billed / bill_no: detail on that real proof
      - dispatch_date / arrival_date for the carrying challan

    This is the "inspect by station" companion to find_nil_bilties: it
    never filters by station_name (there's no create action attached to
    this endpoint, so the station-scoping trap from find_nil_bilties does
    not apply here — this is read-only, for humans to look through).
    """
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
    dispatch_lo = _prev_day(from_date)
    dispatch_hi_excl = to_date

    challan_map = _fetch_dispatched_challans(sb, dispatch_lo, dispatch_hi_excl)
    matched, transit_rows = _match_bilties_for_challans(sb, challan_map, gstin, [], [])  # no station filter — full report
    orphaned_gr_nos = _find_orphaned_transit_grs(sb, transit_rows)

    empty = {
        "transport_gstin": gstin,
        "transport_name": None,
        "from_date": from_date,
        "to_date": to_date,
        "totals": {"challans": 0, "bilties": 0, "with_pohonch": 0, "without_pohonch": 0},
        "challans": [],
        "orphaned_transit_gr_nos": orphaned_gr_nos,
    }
    if not matched:
        return {"status": "success", "message": "No dispatched bilties for this transport in this window", "data": empty}

    # ── Destination city names ──────────────────────────────────────────────
    city_ids = list({r["city_id"] for r in matched.values() if r.get("city_id")})
    city_map: dict[str, str] = {}
    for chunk in _chunks(city_ids, 200):
        res = sb.table("cities").select("id, city_name, city_code").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = c.get("city_name") or c.get("city_code") or ""

    # ── bilty_wise_kaat.pohonch_no, as-is, per GR ───────────────────────────
    gr_nos = list(matched.keys())
    kaat_pohonch_no: dict[str, str | None] = {}
    for chunk in _chunks(gr_nos, 200):
        res = sb.table("bilty_wise_kaat").select("gr_no, pohonch_no").in_("gr_no", chunk).execute()
        for r in res.data or []:
            kaat_pohonch_no[r["gr_no"]] = r.get("pohonch_no")

    # ── Real crossing-proof status (pohonch table is small — full scan,
    #     same accepted pattern used throughout this file) ──────────────────
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

    # ── Build enriched, per-GR rows ──────────────────────────────────────────
    rows = []
    for gr, r in matched.items():
        info = gr_pohonch_map.get(gr)
        rows.append({
            "gr_no": gr,
            "source_table": r["source_table"],
            "challan_no": r["challan_no"],
            "dispatch_date": r["dispatch_date"],
            "arrival_date": r["arrival_date"],
            "bilty_date": r["bilty_date"],
            "station": city_map.get(r.get("city_id"), ""),
            "consignor_name": r["consignor_name"],
            "consignee_name": r["consignee_name"],
            "weight": r["weight"],
            "amount": r["amount"],
            "packages": r["packages"],
            "kaat_pohonch_no": kaat_pohonch_no.get(gr),
            "has_crossing_challan": bool(info),
            "pohonch_number": info["pohonch_number"] if info else None,
            "is_billed": bool(info and info.get("crossing_bill_id")),
            "bill_no": bill_no_map.get(info["crossing_bill_id"]) if info and info.get("crossing_bill_id") else None,
        })

    rows.sort(key=lambda r: (r["challan_no"] or "", r["bilty_date"] or "", r["gr_no"]))

    # ── Group by challan ──────────────────────────────────────────────────
    challans: dict[str, dict] = {}
    for r in rows:
        cno = r["challan_no"]
        if cno not in challans:
            challans[cno] = {
                "challan_no": cno,
                "dispatch_date": r["dispatch_date"],
                "arrival_date": r["arrival_date"],
                "bilty_count": 0,
                "with_pohonch": 0,
                "without_pohonch": 0,
                "bilties": [],
            }
        c = challans[cno]
        c["bilties"].append(r)
        c["bilty_count"] += 1
        c["with_pohonch" if r["has_crossing_challan"] else "without_pohonch"] += 1

    challan_list = sorted(challans.values(), key=lambda c: c["dispatch_date"] or "")
    total_with = sum(1 for r in rows if r["has_crossing_challan"])

    return {
        "status": "success",
        "data": {
            "transport_gstin": gstin,
            "transport_name": next((r["transport_name"] for r in matched.values() if r.get("transport_name")), None),
            "from_date": from_date,
            "to_date": to_date,
            "totals": {
                "challans": len(challan_list),
                "bilties": len(rows),
                "with_pohonch": total_with,
                "without_pohonch": len(rows) - total_with,
            },
            "challans": challan_list,
            "orphaned_transit_gr_nos": orphaned_gr_nos,
        },
    }
