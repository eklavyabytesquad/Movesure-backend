"""
Company-wise Kaat Report Service
Public read-only report: every GR (bilty) belonging to a given company name
(matched against consignor/consignee, e.g. "RGT" -> "RGT Logistics"),
across BOTH the regular `bilty` table and manually-entered `station_bilty_summary`
rows, enriched with:
  - pohonch_no + bilty_number (from bilty_wise_kaat)
  - branch-to-branch transit + delivery details (from transit_details:
    branch1 -> branch2 handoff, out-for-door-delivery, delivered-at-destination)

Supports an additional free-text `search` on top of the company filter,
an optional date range, and pagination.
"""
from datetime import date, timedelta
from services.supabase_client import get_supabase

PAGE_SIZE = 1000  # internal fetch page size (Supabase row cap per request)


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _safe(val, default=""):
    return val if val is not None else default


def _next_day(date_str: str) -> str:
    return str(date.fromisoformat(date_str) + timedelta(days=1))


BILTY_COLS = (
    "id, gr_no, branch_id, bilty_date, from_city_id, to_city_id, "
    "consignor_name, consignor_gst, consignor_number, "
    "consignee_name, consignee_gst, consignee_number, "
    "transport_name, transport_gst, payment_mode, contain, "
    "e_way_bill, no_of_pkg, wt, rate, pvt_marks, freight_amount, "
    "labour_charge, bill_charge, toll_charge, dd_charge, other_charge, "
    "total, remark, is_active"
)

SBS_COLS = (
    "id, gr_no, branch_id, created_at, city_id, "
    "consignor, consignee, transport_name, transport_gst, "
    "payment_status, contents, e_way_bill, no_of_packets, weight, "
    "pvt_marks, amount, delivery_type, remaining_amount, advance_amount"
)


def _fetch_bilty_matches(sb, company, search, from_date, to_date):
    rows = []
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("bilty")
            .select(BILTY_COLS)
            .eq("is_active", True)
            .or_(f"consignor_name.ilike.%{company}%,consignee_name.ilike.%{company}%")
        )
        if search:
            q = q.or_(
                f"gr_no.ilike.%{search}%,consignor_name.ilike.%{search}%,"
                f"consignee_name.ilike.%{search}%,pvt_marks.ilike.%{search}%,"
                f"contain.ilike.%{search}%"
            )
        if from_date:
            q = q.gte("bilty_date", from_date)
        if to_date:
            q = q.lte("bilty_date", to_date)
        batch = q.range(lo, hi).execute().data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return rows


def _fetch_sbs_matches(sb, company, search, from_date, to_date):
    rows = []
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("station_bilty_summary")
            .select(SBS_COLS)
            .or_(f"consignor.ilike.%{company}%,consignee.ilike.%{company}%")
        )
        if search:
            q = q.or_(
                f"gr_no.ilike.%{search}%,consignor.ilike.%{search}%,"
                f"consignee.ilike.%{search}%,pvt_marks.ilike.%{search}%,"
                f"contents.ilike.%{search}%"
            )
        if from_date:
            q = q.gte("created_at", from_date)
        if to_date:
            q = q.lt("created_at", _next_day(to_date))
        batch = q.range(lo, hi).execute().data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return rows


def _fetch_kaat(sb, gr_nos):
    """pohonch_no + bilty_number + the transport recorded on the kaat row."""
    kaat_map = {}
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("bilty_wise_kaat")
            .select("gr_no, pohonch_no, bilty_number, transport_id")
            .in_("gr_no", chunk)
            .execute()
        )
        for row in res.data or []:
            gr = row.get("gr_no")
            if gr:
                kaat_map[gr] = row
    return kaat_map


def _fetch_transports(sb, transport_ids):
    transport_map = {}
    if not transport_ids:
        return transport_map
    for chunk in _chunks(list(transport_ids), 100):
        res = sb.table("transports").select("id, transport_name, gst_number, mob_number").in_("id", chunk).execute()
        for t in res.data or []:
            transport_map[t["id"]] = t
    return transport_map


def _fetch_transit(sb, gr_nos):
    """Branch1 -> branch2 transit + delivery milestones, keyed by gr_no."""
    transit_map = {}
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("transit_details")
            .select(
                "gr_no, challan_no, from_branch_id, to_branch_id, "
                "is_out_of_delivery_from_branch1, out_of_delivery_from_branch1_date, "
                "is_delivered_at_branch2, delivered_at_branch2_date, "
                "is_out_of_delivery_from_branch2, out_of_delivery_from_branch2_date, "
                "is_delivered_at_destination, delivered_at_destination_date, "
                "out_for_door_delivery, out_for_door_delivery_date, "
                "delivery_agent_name, delivery_agent_phone, vehicle_number, remarks"
            )
            .in_("gr_no", chunk)
            .execute()
        )
        for row in res.data or []:
            gr = row.get("gr_no")
            if gr:
                transit_map[gr] = row
    return transit_map


def _fetch_cities(sb, city_ids):
    city_map = {}
    if not city_ids:
        return city_map
    for chunk in _chunks(list(city_ids), 100):
        res = sb.table("cities").select("id, city_name, city_code").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = c
    return city_map


def _fetch_branches(sb, branch_ids):
    branch_map = {}
    if not branch_ids:
        return branch_map
    for chunk in _chunks(list(branch_ids), 100):
        res = sb.table("branches").select("id, branch_name, branch_code").in_("id", chunk).execute()
        for b in res.data or []:
            branch_map[b["id"]] = b
    return branch_map


def get_company_kaat_report(
    company: str = "RGT",
    search: str = None,
    from_date: str = None,
    to_date: str = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """
    Full GR report for every bilty (regular or manual/station) whose
    consignor or consignee name contains `company` (case-insensitive),
    optionally narrowed further by `search`, `from_date` and `to_date`
    (matched against bilty_date / created_at), and paginated.

    Each row includes pohonch_no + bilty_number (from bilty_wise_kaat) and
    the full branch1 -> branch2 transit + delivery timeline (from
    transit_details): which branch it left from, which branch received it,
    out-for-door-delivery, and final delivered-at-destination details.
    """
    try:
        if not company or not company.strip():
            return {"status": "error", "message": "company is required", "status_code": 400}
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 50

        company = company.strip()
        search = search.strip() if search else None

        sb = get_supabase()

        bilty_rows = _fetch_bilty_matches(sb, company, search, from_date, to_date)
        sbs_rows = _fetch_sbs_matches(sb, company, search, from_date, to_date)

        unified = []
        for b in bilty_rows:
            unified.append({
                "source": "regular",
                "gr_no": b["gr_no"],
                "branch_id": b.get("branch_id"),
                "bilty_date": _safe(b.get("bilty_date")),
                "from_city_id": b.get("from_city_id"),
                "to_city_id": b.get("to_city_id"),
                "consignor_name": _safe(b.get("consignor_name")),
                "consignor_gst": _safe(b.get("consignor_gst")),
                "consignor_number": _safe(b.get("consignor_number")),
                "consignee_name": _safe(b.get("consignee_name")),
                "consignee_gst": _safe(b.get("consignee_gst")),
                "consignee_number": _safe(b.get("consignee_number")),
                "transport_name": _safe(b.get("transport_name")),
                "transport_gst": _safe(b.get("transport_gst")),
                "payment_mode": _safe(b.get("payment_mode")),
                "contain": _safe(b.get("contain")),
                "e_way_bill": _safe(b.get("e_way_bill")),
                "no_of_pkg": b.get("no_of_pkg") or 0,
                "wt": b.get("wt") or 0,
                "rate": b.get("rate") or 0,
                "pvt_marks": _safe(b.get("pvt_marks")),
                "freight_amount": b.get("freight_amount") or 0,
                "labour_charge": b.get("labour_charge") or 0,
                "bill_charge": b.get("bill_charge") or 0,
                "toll_charge": b.get("toll_charge") or 0,
                "dd_charge": b.get("dd_charge") or 0,
                "other_charge": b.get("other_charge") or 0,
                "total": b.get("total") or 0,
                "remark": _safe(b.get("remark")),
            })
        for s in sbs_rows:
            created_raw = s.get("created_at") or ""
            unified.append({
                "source": "manual",
                "gr_no": s["gr_no"],
                "branch_id": s.get("branch_id"),
                "bilty_date": created_raw[:10],
                "from_city_id": None,
                "to_city_id": s.get("city_id"),
                "consignor_name": _safe(s.get("consignor")),
                "consignor_gst": "",
                "consignor_number": "",
                "consignee_name": _safe(s.get("consignee")),
                "consignee_gst": "",
                "consignee_number": "",
                "transport_name": _safe(s.get("transport_name")),
                "transport_gst": _safe(s.get("transport_gst")),
                "payment_mode": _safe(s.get("payment_status")),
                "contain": _safe(s.get("contents")),
                "e_way_bill": _safe(s.get("e_way_bill")),
                "no_of_pkg": s.get("no_of_packets") or 0,
                "wt": s.get("weight") or 0,
                "rate": 0,
                "pvt_marks": _safe(s.get("pvt_marks")),
                "freight_amount": s.get("amount") or 0,
                "labour_charge": 0,
                "bill_charge": 0,
                "toll_charge": 0,
                "dd_charge": 0,
                "other_charge": 0,
                "total": s.get("amount") or 0,
                "remark": _safe(s.get("delivery_type")),
            })

        total = len(unified)
        if total == 0:
            return {
                "status": "success",
                "data": {
                    "rows": [], "page": page, "page_size": page_size,
                    "total": 0, "has_more": False,
                },
            }

        # newest first
        unified.sort(key=lambda r: (r["bilty_date"] or "", r["gr_no"]), reverse=True)

        start = (page - 1) * page_size
        page_rows = unified[start:start + page_size]

        gr_nos = [r["gr_no"] for r in page_rows]
        kaat_map = _fetch_kaat(sb, gr_nos)
        transit_map = _fetch_transit(sb, gr_nos)

        city_ids = {r["from_city_id"] for r in page_rows if r.get("from_city_id")} | \
                   {r["to_city_id"] for r in page_rows if r.get("to_city_id")}
        city_map = _fetch_cities(sb, city_ids)

        branch_ids = {r["branch_id"] for r in page_rows if r.get("branch_id")}
        branch_ids |= {t["from_branch_id"] for t in transit_map.values() if t.get("from_branch_id")}
        branch_ids |= {t["to_branch_id"] for t in transit_map.values() if t.get("to_branch_id")}
        branch_map = _fetch_branches(sb, branch_ids)

        transport_ids = {k["transport_id"] for k in kaat_map.values() if k.get("transport_id")}
        transport_map = _fetch_transports(sb, transport_ids)

        def _branch(branch_id):
            b = branch_map.get(branch_id, {}) if branch_id else {}
            return _safe(b.get("branch_name")), _safe(b.get("branch_code"))

        result_rows = []
        for r in page_rows:
            gr = r["gr_no"]
            kaat = kaat_map.get(gr, {})
            transit = transit_map.get(gr, {})
            kaat_transport = transport_map.get(kaat.get("transport_id"), {}) if kaat.get("transport_id") else {}
            from_city = city_map.get(r["from_city_id"], {}) if r.get("from_city_id") else {}
            to_city = city_map.get(r["to_city_id"], {}) if r.get("to_city_id") else {}
            branch_name, branch_code = _branch(r.get("branch_id"))
            branch1_name, branch1_code = _branch(transit.get("from_branch_id"))
            branch2_name, branch2_code = _branch(transit.get("to_branch_id"))

            result_rows.append({
                **{k: v for k, v in r.items() if k not in ("from_city_id", "to_city_id", "branch_id")},
                "branch_name": branch_name,
                "branch_code": branch_code,
                "from_city": _safe(from_city.get("city_name")),
                "to_city": _safe(to_city.get("city_name")),
                # from bilty_wise_kaat
                "pohonch_no": _safe(kaat.get("pohonch_no")),
                "bilty_number": _safe(kaat.get("bilty_number")),
                "kaat_transport_name": _safe(kaat_transport.get("transport_name")),
                "kaat_transport_gst": _safe(kaat_transport.get("gst_number")),
                "kaat_transport_number": _safe(kaat_transport.get("mob_number")),
                # branch-to-branch transit + delivery details (transit_details)
                "challan_no": _safe(transit.get("challan_no")),
                "branch1_name": branch1_name,
                "branch1_code": branch1_code,
                "is_out_of_delivery_from_branch1": transit.get("is_out_of_delivery_from_branch1", False),
                "out_of_delivery_from_branch1_date": transit.get("out_of_delivery_from_branch1_date") or "",
                "branch2_name": branch2_name,
                "branch2_code": branch2_code,
                "is_delivered_at_branch2": transit.get("is_delivered_at_branch2", False),
                "delivered_at_branch2_date": transit.get("delivered_at_branch2_date") or "",
                "is_out_of_delivery_from_branch2": transit.get("is_out_of_delivery_from_branch2", False),
                "out_of_delivery_from_branch2_date": transit.get("out_of_delivery_from_branch2_date") or "",
                "out_for_door_delivery": transit.get("out_for_door_delivery", False),
                "out_for_door_delivery_date": transit.get("out_for_door_delivery_date") or "",
                "is_delivered_at_destination": transit.get("is_delivered_at_destination", False),
                "delivered_at_destination_date": transit.get("delivered_at_destination_date") or "",
                "delivery_agent_name": _safe(transit.get("delivery_agent_name")),
                "delivery_agent_phone": _safe(transit.get("delivery_agent_phone")),
                "vehicle_number": _safe(transit.get("vehicle_number")),
                "transit_remarks": _safe(transit.get("remarks")),
            })

        return {
            "status": "success",
            "data": {
                "rows": result_rows,
                "page": page,
                "page_size": page_size,
                "total": total,
                "has_more": start + page_size < total,
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to fetch company kaat report: {str(e)}",
            "status_code": 500,
        }
