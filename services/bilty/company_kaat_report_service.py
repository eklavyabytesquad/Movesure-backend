"""
Company-wise Kaat Report Service
Public read-only report: every GR (bilty) belonging to a given company name
(matched against consignor/consignee, e.g. "RGT" -> "RGT Logistics"),
across BOTH the regular `bilty` table and manually-entered `station_bilty_summary`
rows, enriched with:
  - kaat details (bilty_wise_kaat)
  - dispatch details (challan_details, via kaat.challan_no)
  - receiving/delivery details (pod_details + transit_details, by gr_no)

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
    kaat_map = {}
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("bilty_wise_kaat")
            .select(
                "gr_no, challan_no, destination_city_id, rate_type, rate_per_kg, "
                "rate_per_pkg, kaat, pf, actual_kaat_rate, dd_chrg, bilty_chrg, "
                "ewb_chrg, labour_chrg, other_chrg, pohonch_no, bilty_number, "
                "crossing_challan_no"
            )
            .in_("gr_no", chunk)
            .execute()
        )
        for row in res.data or []:
            gr = row.get("gr_no")
            if gr:
                kaat_map[gr] = row
    return kaat_map


def _fetch_challan_dispatch(sb, challan_nos):
    dispatch_map = {}
    if not challan_nos:
        return dispatch_map
    for chunk in _chunks(challan_nos, 100):
        res = (
            sb.table("challan_details")
            .select("challan_no, date, is_dispatched, dispatch_date, is_received_at_hub, received_at_hub_timing")
            .in_("challan_no", chunk)
            .execute()
        )
        for row in res.data or []:
            dispatch_map[row.get("challan_no", "")] = {
                "challan_date": _safe(row.get("date")),
                "is_dispatched": row.get("is_dispatched", False),
                "dispatch_date": row.get("dispatch_date") or "",
                "is_received_at_hub": row.get("is_received_at_hub", False),
                "received_at_hub_timing": row.get("received_at_hub_timing") or "",
            }
    return dispatch_map


def _fetch_pod(sb, gr_nos):
    pod_map = {}
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("pod_details")
            .select("gr_no, pod_no, delivered_at, payment_mode, total_amount, amount_given")
            .in_("gr_no", chunk)
            .execute()
        )
        for row in res.data or []:
            gr = row.get("gr_no")
            if gr:
                pod_map[gr] = row
    return pod_map


def _fetch_transit(sb, gr_nos):
    transit_map = {}
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("transit_details")
            .select(
                "gr_no, is_delivered_at_destination, delivered_at_destination_date, "
                "out_for_door_delivery, out_for_door_delivery_date, "
                "delivery_agent_name, delivery_agent_phone, vehicle_number"
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
    Full GR + kaat + dispatch + receiving report for every bilty (regular or
    manual/station) whose consignor or consignee name contains `company`
    (case-insensitive), optionally narrowed further by `search`, `from_date`
    and `to_date` (matched against bilty_date / created_at), and paginated.
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
        challan_nos = list({k["challan_no"] for k in kaat_map.values() if k.get("challan_no")})
        dispatch_map = _fetch_challan_dispatch(sb, challan_nos)
        pod_map = _fetch_pod(sb, gr_nos)
        transit_map = _fetch_transit(sb, gr_nos)

        city_ids = {r["from_city_id"] for r in page_rows if r.get("from_city_id")} | \
                   {r["to_city_id"] for r in page_rows if r.get("to_city_id")}
        city_map = _fetch_cities(sb, city_ids)
        branch_ids = {r["branch_id"] for r in page_rows if r.get("branch_id")}
        branch_map = _fetch_branches(sb, branch_ids)

        result_rows = []
        for r in page_rows:
            gr = r["gr_no"]
            kaat = kaat_map.get(gr, {})
            challan_no = _safe(kaat.get("challan_no"))
            dispatch = dispatch_map.get(challan_no, {}) if challan_no else {}
            pod = pod_map.get(gr, {})
            transit = transit_map.get(gr, {})
            from_city = city_map.get(r["from_city_id"], {}) if r.get("from_city_id") else {}
            to_city = city_map.get(r["to_city_id"], {}) if r.get("to_city_id") else {}
            branch = branch_map.get(r["branch_id"], {}) if r.get("branch_id") else {}

            result_rows.append({
                **{k: v for k, v in r.items() if k not in ("from_city_id", "to_city_id", "branch_id")},
                "branch_name": _safe(branch.get("branch_name")),
                "branch_code": _safe(branch.get("branch_code")),
                "from_city": _safe(from_city.get("city_name")),
                "to_city": _safe(to_city.get("city_name")),
                # kaat details
                "challan_no": challan_no,
                "kaat": kaat.get("kaat", 0),
                "kaat_pf": kaat.get("pf", 0),
                "kaat_dd": kaat.get("dd_chrg", 0),
                "kaat_rate": kaat.get("actual_kaat_rate", 0),
                "rate_type": _safe(kaat.get("rate_type")),
                "rate_per_kg": kaat.get("rate_per_kg", 0),
                "rate_per_pkg": kaat.get("rate_per_pkg", 0),
                "bilty_chrg": kaat.get("bilty_chrg", 0),
                "ewb_chrg": kaat.get("ewb_chrg", 0),
                "labour_chrg": kaat.get("labour_chrg", 0),
                "other_chrg": kaat.get("other_chrg", 0),
                "pohonch_no": _safe(kaat.get("pohonch_no")),
                "bilty_number": _safe(kaat.get("bilty_number")),
                "crossing_challan_no": _safe(kaat.get("crossing_challan_no")),
                # dispatch details (from challan)
                "challan_date": dispatch.get("challan_date", ""),
                "is_dispatched": dispatch.get("is_dispatched", False),
                "dispatch_date": dispatch.get("dispatch_date", ""),
                "is_received_at_hub": dispatch.get("is_received_at_hub", False),
                "received_at_hub_timing": dispatch.get("received_at_hub_timing", ""),
                # receiving / delivery details
                "pod_no": _safe(pod.get("pod_no")),
                "delivered_at": pod.get("delivered_at") or "",
                "pod_payment_mode": _safe(pod.get("payment_mode")),
                "pod_total_amount": pod.get("total_amount", 0),
                "pod_amount_given": pod.get("amount_given", 0),
                "is_delivered_at_destination": transit.get("is_delivered_at_destination", False),
                "delivered_at_destination_date": transit.get("delivered_at_destination_date") or "",
                "out_for_door_delivery": transit.get("out_for_door_delivery", False),
                "out_for_door_delivery_date": transit.get("out_for_door_delivery_date") or "",
                "delivery_agent_name": _safe(transit.get("delivery_agent_name")),
                "delivery_agent_phone": _safe(transit.get("delivery_agent_phone")),
                "vehicle_number": _safe(transit.get("vehicle_number")),
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
