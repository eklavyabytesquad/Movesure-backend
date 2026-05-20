"""
Transport Bilty Report Service
Given a transport GSTIN (or transport_name) and a date range, returns every
bilty across bilty table (regular) and station_bilty_summary (manual).

Response: nested grouped structure:
  with_pohonch: { pohonch_number -> { regular: [...], manual: [...] } }
  no_pohonch:   { challan_no -> [...], "UNKNOWN": [...] }
"""

import re
from datetime import date, timedelta
from collections import defaultdict
from services.supabase_client import get_supabase


def _natural_key(s: str):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', s)]


PAGE_SIZE = 1000


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _safe(val, default=""):
    return val if val is not None else default


def _next_day(date_str: str) -> str:
    return str(date.fromisoformat(date_str) + timedelta(days=1))


def get_transport_bilty_report(
    transport_gstin=None,
    transport_name=None,
    from_date=None,
    to_date=None,
):
    """
    Get comprehensive transport bilty report with all details including content.

    Each bilty includes:
    - gr_no, bilty_number, dest_pohonch_no
    - kaat, kaat_pf (Provider Fee), kaat_dd (DD/Deduction)
    - payment_mode (paid/to-pay/foc)
    - **contain: Goods/Contents description** ✓ INCLUDED
    - All other charges and amounts

    Returns grouped data by pohonch number.
    """
    if not transport_gstin and not transport_name:
        return {"status": "error", "message": "transport_gstin or transport_name is required", "status_code": 400}
    if not from_date or not to_date:
        return {"status": "error", "message": "from_date and to_date are required (YYYY-MM-DD)", "status_code": 400}
    try:
        date.fromisoformat(from_date)
        date.fromisoformat(to_date)
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD", "status_code": 400}

    sb = get_supabase()
    to_date_exclusive = _next_day(to_date)

    bilty_rows = _fetch_bilty(sb, transport_gstin, transport_name, from_date, to_date)
    sbs_rows = _fetch_station_bilty_summary(sb, transport_gstin, transport_name, from_date, to_date_exclusive)

    seen = set()
    unified = []

    for b in bilty_rows:
        gr = b["gr_no"]
        seen.add(gr)
        unified.append({
            "source": "regular",
            "gr_no": gr,
            "bilty_date": _safe(b.get("bilty_date")),
            "transport_name": _safe(b.get("transport_name")),
            "transport_gst": _safe(b.get("transport_gst")),
            "consignor_name": _safe(b.get("consignor_name")),
            "consignee_name": _safe(b.get("consignee_name")),
            "city_id_from": b.get("from_city_id"),
            "city_id_to": b.get("to_city_id"),
            "payment_mode": _safe(b.get("payment_mode")),
            "no_of_pkg": b.get("no_of_pkg") or 0,
            "wt": b.get("wt") or 0,
            "freight_amount": b.get("freight_amount") or 0,
            "pf_charge": b.get("pf_charge") or 0,
            "dd_charge": b.get("dd_charge") or 0,
            "labour_charge": b.get("labour_charge") or 0,
            "bill_charge": b.get("bill_charge") or 0,
            "toll_charge": b.get("toll_charge") or 0,
            "other_charge": b.get("other_charge") or 0,
            "total": b.get("total") or 0,
            "contain": _safe(b.get("contain")),
            "pvt_marks": _safe(b.get("pvt_marks")),
            "remark": _safe(b.get("remark")),
        })

    for s in sbs_rows:
        gr = s["gr_no"]
        if gr in seen:
            continue
        seen.add(gr)
        created_raw = s.get("created_at") or ""
        unified.append({
            "source": "manual",
            "gr_no": gr,
            "bilty_date": created_raw[:10],
            "transport_name": _safe(s.get("transport_name")),
            "transport_gst": _safe(s.get("transport_gst")),
            "consignor_name": _safe(s.get("consignor")),
            "consignee_name": _safe(s.get("consignee")),
            "city_id_from": None,
            "city_id_to": s.get("city_id"),
            "payment_mode": _safe(s.get("payment_status")),
            "no_of_pkg": s.get("no_of_packets") or 0,
            "wt": s.get("weight") or 0,
            "freight_amount": s.get("amount") or 0,
            "pf_charge": 0,
            "dd_charge": 0,
            "labour_charge": 0,
            "bill_charge": 0,
            "toll_charge": 0,
            "other_charge": 0,
            "total": s.get("amount") or 0,
            "contain": _safe(s.get("contents")),
            "pvt_marks": _safe(s.get("pvt_marks")),
            "remark": _safe(s.get("delivery_type")),
        })

    if not unified:
        return {
            "status": "success",
            "from_date": from_date, "to_date": to_date,
            "transport_gstin": transport_gstin or "",
            "transport_name": transport_name or "",
            "sources": {"bilty_table": 0, "station_bilty_summary": 0},
            "summary": {"total": 0, "with_pohonch": 0, "without_pohonch": 0},
            "with_pohonch": {},
            "no_pohonch": {},
        }

    transport_filter = transport_gstin or transport_name
    is_gstin = bool(transport_gstin)
    gr_to_pohonch_number, gr_to_crossing_challans, all_pohonch_gr_nos = _fetch_pohonch_maps(sb, transport_filter, is_gstin)

    # Fetch crossing-challan bilties from other transports referenced in this transport's pohonch
    missing_gr_nos = all_pohonch_gr_nos - seen
    if missing_gr_nos:
        missing_list = list(missing_gr_nos)
        for b in _fetch_bilty_by_gr_nos(sb, missing_list):
            gr = b["gr_no"]
            if gr in seen:
                continue
            seen.add(gr)
            unified.append({
                "source": "regular",
                "gr_no": gr,
                "bilty_date": _safe(b.get("bilty_date")),
                "transport_name": _safe(b.get("transport_name")),
                "transport_gst": _safe(b.get("transport_gst")),
                "consignor_name": _safe(b.get("consignor_name")),
                "consignee_name": _safe(b.get("consignee_name")),
                "city_id_from": b.get("from_city_id"),
                "city_id_to": b.get("to_city_id"),
                "payment_mode": _safe(b.get("payment_mode")),
                "no_of_pkg": b.get("no_of_pkg") or 0,
                "wt": b.get("wt") or 0,
                "freight_amount": b.get("freight_amount") or 0,
                "pf_charge": b.get("pf_charge") or 0,
                "dd_charge": b.get("dd_charge") or 0,
                "labour_charge": b.get("labour_charge") or 0,
                "bill_charge": b.get("bill_charge") or 0,
                "toll_charge": b.get("toll_charge") or 0,
                "other_charge": b.get("other_charge") or 0,
                "total": b.get("total") or 0,
                "contain": _safe(b.get("contain")),
                "pvt_marks": _safe(b.get("pvt_marks")),
                "remark": _safe(b.get("remark")),
            })
        for s in _fetch_sbs_by_gr_nos(sb, missing_list):
            gr = s["gr_no"]
            if gr in seen:
                continue
            seen.add(gr)
            created_raw = s.get("created_at") or ""
            unified.append({
                "source": "manual",
                "gr_no": gr,
                "bilty_date": created_raw[:10],
                "transport_name": _safe(s.get("transport_name")),
                "transport_gst": _safe(s.get("transport_gst")),
                "consignor_name": _safe(s.get("consignor")),
                "consignee_name": _safe(s.get("consignee")),
                "city_id_from": None,
                "city_id_to": s.get("city_id"),
                "payment_mode": _safe(s.get("payment_status")),
                "no_of_pkg": s.get("no_of_packets") or 0,
                "wt": s.get("weight") or 0,
                "freight_amount": s.get("amount") or 0,
                "pf_charge": 0,
                "dd_charge": 0,
                "labour_charge": 0,
                "bill_charge": 0,
                "toll_charge": 0,
                "other_charge": 0,
                "total": s.get("amount") or 0,
                "contain": _safe(s.get("contents")),
                "pvt_marks": _safe(s.get("pvt_marks")),
                "remark": _safe(s.get("delivery_type")),
            })

    gr_nos = [b["gr_no"] for b in unified]
    kaat_map = _fetch_kaat(sb, gr_nos)

    all_challan_nos = list({row["challan_no"] for row in kaat_map.values() if row.get("challan_no")})
    challan_dispatch_map = _fetch_challan_dispatch(sb, all_challan_nos)

    city_ids = {b["city_id_from"] for b in unified if b.get("city_id_from")} | \
               {b["city_id_to"] for b in unified if b.get("city_id_to")}
    city_map = _fetch_cities(sb, city_ids)

    with_pohonch_groups = defaultdict(lambda: {"regular": [], "manual": []})
    no_pohonch_groups = defaultdict(list)
    total_wt = 0
    total_freight = 0

    for b in unified:
        gr = b["gr_no"]
        kaat = kaat_map.get(gr, {})
        pohonch_number = gr_to_pohonch_number.get(gr, "")
        crossing_challans = gr_to_crossing_challans.get(gr, "")
        challan_no = _safe(kaat.get("challan_no"))
        dispatch_date = challan_dispatch_map.get(challan_no, "") if challan_no else ""

        row = {
            "source": b["source"],
            "gr_no": gr,
            "bilty_date": b["bilty_date"],
            "transport_name": b["transport_name"],
            "transport_gst": b["transport_gst"],
            "consignor_name": b["consignor_name"],
            "consignee_name": b["consignee_name"],
            "from_city": city_map.get(b["city_id_from"], "") if b.get("city_id_from") else "",
            "to_city": city_map.get(b["city_id_to"], "") if b.get("city_id_to") else "",
            "payment_mode": b["payment_mode"],
            "no_of_pkg": b["no_of_pkg"],
            "wt": b["wt"],
            "freight_amount": b["freight_amount"],
            "pf_charge": b["pf_charge"],
            "dd_charge": b["dd_charge"],
            "labour_charge": b["labour_charge"],
            "bill_charge": b["bill_charge"],
            "toll_charge": b["toll_charge"],
            "other_charge": b["other_charge"],
            "total": b["total"],
            "contain": b["contain"],
            "pvt_marks": b["pvt_marks"],
            "remark": b["remark"],
            "challan_no": challan_no,
            "challan_dispatch_date": dispatch_date,
            "pohonch_number": pohonch_number,
            "has_crossing_challan": bool(crossing_challans),
            "crossing_challans": crossing_challans,
            "dest_pohonch_no": _safe(kaat.get("pohonch_no")).strip(),
            "bilty_number": _safe(kaat.get("bilty_number")),
            "kaat": kaat.get("kaat"),
            "kaat_pf": kaat.get("pf"),
            "kaat_dd": kaat.get("dd_chrg"),
            "kaat_rate": kaat.get("actual_kaat_rate"),
        }

        total_wt += b["wt"] or 0
        total_freight += b["total"] or 0

        if pohonch_number:
            with_pohonch_groups[pohonch_number][b["source"]].append(row)
        else:
            bilty_number = _safe(kaat.get("bilty_number"))
            key = challan_no if challan_no else (bilty_number if bilty_number else "UNKNOWN")
            no_pohonch_groups[key].append(row)

    for grp in with_pohonch_groups.values():
        grp["regular"].sort(key=lambda x: x["gr_no"])
        grp["manual"].sort(key=lambda x: x["gr_no"])
    for lst in no_pohonch_groups.values():
        lst.sort(key=lambda x: x["gr_no"])

    with_pohonch_sorted = dict(sorted(with_pohonch_groups.items(), key=lambda kv: _natural_key(kv[0])))

    def _ck(kv):
        return (1, []) if kv[0] == "UNKNOWN" else (0, _natural_key(kv[0]))
    no_pohonch_sorted = dict(sorted(no_pohonch_groups.items(), key=_ck))

    with_pohonch_count = sum(len(g["regular"]) + len(g["manual"]) for g in with_pohonch_sorted.values())
    without_pohonch_count = sum(len(v) for v in no_pohonch_sorted.values())

    return {
        "status": "success",
        "from_date": from_date,
        "to_date": to_date,
        "transport_gstin": transport_gstin or "",
        "transport_name": unified[0]["transport_name"] if unified else (transport_name or ""),
        "sources": {"bilty_table": len(bilty_rows), "station_bilty_summary": len(sbs_rows)},
        "summary": {
            "total": len(unified),
            "with_pohonch": with_pohonch_count,
            "without_pohonch": without_pohonch_count,
            "total_weight_kg": round(total_wt, 2),
            "total_freight": round(total_freight, 2),
        },
        "with_pohonch": with_pohonch_sorted,
        "no_pohonch": no_pohonch_sorted,
    }


def _apply_transport_filter(query, transport_gstin, transport_name):
    if transport_gstin:
        return query.eq("transport_gst", transport_gstin.strip().upper())
    return query.ilike("transport_name", f"%{transport_name.strip()}%")


def _fetch_bilty(sb, transport_gstin, transport_name, from_date, to_date):
    rows = []
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("bilty")
            .select(
                "gr_no, bilty_date, transport_name, transport_gst, "
                "consignor_name, consignee_name, from_city_id, to_city_id, "
                "payment_mode, no_of_pkg, wt, freight_amount, pf_charge, "
                "dd_charge, labour_charge, bill_charge, toll_charge, "
                "other_charge, total, contain, pvt_marks, remark"
            )
            .eq("is_active", True)
            .gte("bilty_date", from_date)
            .lte("bilty_date", to_date)
            .range(lo, hi)
        )
        q = _apply_transport_filter(q, transport_gstin, transport_name)
        batch = q.execute().data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return rows


def _fetch_station_bilty_summary(sb, transport_gstin, transport_name, from_date, to_date_exclusive):
    rows = []
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("station_bilty_summary")
            .select(
                "gr_no, created_at, transport_name, transport_gst, "
                "consignor, consignee, city_id, payment_status, "
                "no_of_packets, weight, amount, contents, pvt_marks, delivery_type"
            )
            .gte("created_at", from_date)
            .lt("created_at", to_date_exclusive)
            .range(lo, hi)
        )
        q = _apply_transport_filter(q, transport_gstin, transport_name)
        batch = q.execute().data or []
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
            .select("gr_no, challan_no, pohonch_no, bilty_number, kaat, pf, dd_chrg, actual_kaat_rate")
            .in_("gr_no", chunk)
            .execute()
        )
        for row in res.data or []:
            gr = row.get("gr_no")
            if gr and gr not in kaat_map:
                kaat_map[gr] = row
    return kaat_map


def _fetch_challan_dispatch(sb, challan_nos):
    dispatch_map = {}
    if not challan_nos:
        return dispatch_map
    for chunk in _chunks(challan_nos, 100):
        res = (
            sb.table("challan_details")
            .select(
                "challan_no, date, is_dispatched, dispatch_date, "
                "is_received_at_hub, received_at_hub_timing, remarks, total_bilty_count"
            )
            .in_("challan_no", chunk)
            .execute()
        )
        for row in res.data or []:
            cno = row.get("challan_no", "")
            dispatch_map[cno] = {
                "challan_date": _safe(row.get("date")),
                "is_dispatched": row.get("is_dispatched", False),
                "dispatch_date": row.get("dispatch_date") or "",
                "is_received_at_hub": row.get("is_received_at_hub", False),
                "received_at_hub_timing": row.get("received_at_hub_timing") or "",
                "remarks": _safe(row.get("remarks")),
                "total_bilty_count": row.get("total_bilty_count") or 0,
            }
    return dispatch_map


def _fetch_pohonch_maps(sb, transport_filter, is_gstin):
    gr_to_pohonch_number = {}
    gr_to_crossing_challans = {}
    all_pohonch_gr_nos = set()
    page = 0
    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1
        q = (
            sb.table("pohonch")
            .select("pohonch_number, challan_metadata, bilty_metadata")
            .eq("is_active", True)
            .range(lo, hi)
        )
        if is_gstin:
            q = q.eq("transport_gstin", transport_filter.strip().upper())
        else:
            q = q.ilike("transport_name", f"%{transport_filter.strip()}%")
        batch = q.execute().data or []
        for p in batch:
            pno = p.get("pohonch_number", "")
            challans = p.get("challan_metadata") or []
            crossing = " | ".join(str(c) for c in challans) if challans else ""
            for entry in (p.get("bilty_metadata") or []):
                gr = entry.get("gr_no") if isinstance(entry, dict) else None
                if gr:
                    gr_to_pohonch_number[gr] = pno
                    gr_to_crossing_challans[gr] = crossing
                    all_pohonch_gr_nos.add(gr)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return gr_to_pohonch_number, gr_to_crossing_challans, all_pohonch_gr_nos


def _fetch_cities(sb, city_ids):
    city_map = {}
    if not city_ids:
        return city_map
    for chunk in _chunks(list(city_ids), 100):
        res = sb.table("cities").select("id, city_name").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = c.get("city_name", "")
    return city_map


def _fetch_bilty_by_gr_nos(sb, gr_nos):
    """Fetch bilty rows by gr_no list (no transport filter — used for crossing-challan bilties)."""
    rows = []
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("bilty")
            .select(
                "gr_no, bilty_date, transport_name, transport_gst, "
                "consignor_name, consignee_name, from_city_id, to_city_id, "
                "payment_mode, no_of_pkg, wt, freight_amount, pf_charge, "
                "dd_charge, labour_charge, bill_charge, toll_charge, "
                "other_charge, total, contain, pvt_marks, remark"
            )
            .eq("is_active", True)
            .in_("gr_no", chunk)
            .execute()
        )
        rows.extend(res.data or [])
    return rows


def _fetch_sbs_by_gr_nos(sb, gr_nos):
    """Fetch station_bilty_summary rows by gr_no list (no transport filter)."""
    rows = []
    for chunk in _chunks(gr_nos, 100):
        res = (
            sb.table("station_bilty_summary")
            .select(
                "gr_no, created_at, transport_name, transport_gst, "
                "consignor, consignee, city_id, payment_status, "
                "no_of_packets, weight, amount, contents, pvt_marks, delivery_type"
            )
            .in_("gr_no", chunk)
            .execute()
        )
        rows.extend(res.data or [])
    return rows
