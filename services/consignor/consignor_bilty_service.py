"""
Consignor 360 View
====================
Everything about every bilty booked for one consignor, in one call:
dispatch status, e-way bill numbers + their validation status, Part-B
(transporter update) history, truck/driver on the dispatching challan,
and bilty-wise-kaat/pohonch/crossing details.

Matches by consignor_name (ilike, since neither `bilty` nor
`station_bilty_summary` has a real consignor_id FK — just free-text name/
GST/number fields) — pass consignor_gst too to disambiguate two consignors
with a similar name.
"""
from services.supabase_client import get_supabase


def _split_ewb_numbers(raw) -> list:
    if not raw:
        return []
    return [p.strip() for p in str(raw).split(",") if p.strip()]


def _resolve_cities(sb, city_ids: set) -> dict:
    city_ids = [c for c in city_ids if c]
    if not city_ids:
        return {}
    rows = sb.table("cities").select("id, city_name, city_code").in_("id", city_ids).execute().data or []
    return {r["id"]: r for r in rows}


def get_consignor_bilties(consignor_name: str, consignor_gst: str = None,
                           from_date: str = None, to_date: str = None,
                           page: int = 1, page_size: int = 50) -> dict:
    if not consignor_name:
        return {"status": "error", "message": "consignor_name is required", "status_code": 400}

    sb = get_supabase()

    # 1. Base bilties — both sources, same as every other cross-table bilty lookup in this codebase.
    b_query = (
        sb.table("bilty")
        .select(
            "id, gr_no, bilty_date, delivery_type, consignor_name, consignor_gst, consignor_number, "
            "consignee_name, consignee_gst, consignee_number, transport_name, transport_gst, "
            "from_city_id, to_city_id, payment_mode, no_of_pkg, wt, total, pvt_marks, e_way_bill, "
            "branch_id, is_active"
        )
        .ilike("consignor_name", f"%{consignor_name}%")
        .eq("is_active", True)
    )
    if consignor_gst:
        b_query = b_query.eq("consignor_gst", consignor_gst)
    if from_date:
        b_query = b_query.gte("bilty_date", from_date)
    if to_date:
        b_query = b_query.lte("bilty_date", to_date)
    bilty_rows = b_query.execute().data or []
    for r in bilty_rows:
        r["source_table"] = "bilty"

    # station_bilty_summary has no consignor_gst column — skip it entirely
    # when a GST was given to disambiguate, since it can't be verified there.
    station_rows = []
    if not consignor_gst:
        s_query = (
            sb.table("station_bilty_summary")
            .select(
                "id, gr_no, created_at, delivery_type, consignor, consignee, "
                "transport_name, transport_gst, city_id, payment_status, no_of_packets, weight, "
                "amount, pvt_marks, e_way_bill, branch_id"
            )
            .ilike("consignor", f"%{consignor_name}%")
        )
        station_rows = s_query.execute().data or []
    for r in station_rows:
        r["source_table"] = "station_bilty_summary"
        # normalize field names to match `bilty`'s shape
        r["consignor_name"] = r.pop("consignor")
        r["consignor_gst"] = None
        r["bilty_date"] = r.pop("created_at")
        r["to_city_id"] = r.pop("city_id")
        r["payment_mode"] = r.pop("payment_status")
        r["no_of_pkg"] = r.pop("no_of_packets")
        r["wt"] = r.pop("weight")
        r["total"] = r.pop("amount")

    all_rows = bilty_rows + station_rows
    if not all_rows:
        return {"status": "success", "data": {"total": 0, "rows": [], "page": page, "page_size": page_size, "has_more": False}}

    all_rows.sort(key=lambda r: r["bilty_date"] or "", reverse=True)
    total = len(all_rows)
    offset = (page - 1) * page_size
    page_rows = all_rows[offset: offset + page_size]
    gr_nos = [r["gr_no"] for r in page_rows]

    # 2. Transit + dispatch status
    transit_rows = (
        sb.table("transit_details")
        .select(
            "gr_no, challan_no, bilty_id, is_out_of_delivery_from_branch1, "
            "is_delivered_at_branch2, is_out_of_delivery_from_branch2, "
            "is_delivered_at_destination, out_for_door_delivery, vehicle_number, "
            "delivery_agent_name, delivery_agent_phone"
        )
        .in_("gr_no", gr_nos)
        .execute().data or []
    )
    transit_by_gr = {r["gr_no"]: r for r in transit_rows}

    challan_nos = list({r["challan_no"] for r in transit_rows if r.get("challan_no")})
    challan_map = {}
    truck_map = {}
    staff_map = {}
    if challan_nos:
        challans = (
            sb.table("challan_details")
            .select("challan_no, truck_id, owner_id, driver_id, date, is_dispatched, dispatch_date, "
                     "is_received_at_hub, received_at_hub_timing")
            .in_("challan_no", challan_nos)
            .execute().data or []
        )
        challan_map = {c["challan_no"]: c for c in challans}

        truck_ids = list({c["truck_id"] for c in challans if c.get("truck_id")})
        if truck_ids:
            trucks = sb.table("trucks").select("id, truck_number").in_("id", truck_ids).execute().data or []
            truck_map = {t["id"]: t["truck_number"] for t in trucks}

        staff_ids = list({c["owner_id"] for c in challans if c.get("owner_id")}
                         | {c["driver_id"] for c in challans if c.get("driver_id")})
        if staff_ids:
            staff = sb.table("staff").select("id, name, mobile_number").in_("id", staff_ids).execute().data or []
            staff_map = {s["id"]: s for s in staff}

    # 3. Bilty-wise-kaat / pohonch / crossing details
    kaat_rows = (
        sb.table("bilty_wise_kaat")
        .select("gr_no, challan_no, pohonch_no, bilty_number, crossing_challan_no, rate_type, "
                "rate_per_kg, rate_per_pkg, kaat, pf, dd_chrg, bilty_chrg, ewb_chrg, labour_chrg, other_chrg")
        .in_("gr_no", gr_nos)
        .execute().data or []
    )
    kaat_by_gr = {r["gr_no"]: r for r in kaat_rows}

    # 4. Part-B (transporter update) history — every attempt, most recent first
    tu_rows = (
        sb.table("transporter_updates")
        .select("gr_no, ewb_number, transporter_id, transporter_name, is_success, "
                 "update_status, error_message, pdf_url, update_date, is_downloaded, updated_at")
        .in_("gr_no", gr_nos)
        .order("updated_at", desc=True)
        .execute().data or []
    )
    part_b_by_gr = {}
    for r in tu_rows:
        part_b_by_gr.setdefault(r["gr_no"], []).append(r)

    # 5. E-way bill validation cache — latest per EWB number
    ewb_numbers = set()
    for r in page_rows:
        ewb_numbers.update(_split_ewb_numbers(r.get("e_way_bill")))
    ewb_val_by_number = {}
    if ewb_numbers:
        val_rows = (
            sb.table("ewb_validations")
            .select("ewb_number, is_valid, validation_status, valid_upto, validated_at")
            .in_("ewb_number", list(ewb_numbers))
            .order("validated_at", desc=True)
            .execute().data or []
        )
        for r in val_rows:
            ewb_val_by_number.setdefault(r["ewb_number"], r)  # first (latest) wins

    # 6. City name resolution
    city_ids = {r.get("from_city_id") for r in page_rows} | {r.get("to_city_id") for r in page_rows}
    city_map = _resolve_cities(sb, city_ids)

    # ── Assemble ──
    results = []
    for r in page_rows:
        gr_no = r["gr_no"]
        transit = transit_by_gr.get(gr_no)
        challan = challan_map.get(transit["challan_no"]) if transit and transit.get("challan_no") else None

        dispatch_info = {
            "is_in_transit": transit is not None,
            "challan_no": transit["challan_no"] if transit else None,
            "is_dispatched": bool(challan.get("is_dispatched")) if challan else False,
            "dispatch_date": challan.get("dispatch_date") if challan else None,
            "is_received_at_hub": bool(challan.get("is_received_at_hub")) if challan else False,
            "truck_number": truck_map.get(challan.get("truck_id")) if challan else None,
            "driver": staff_map.get(challan.get("driver_id")) if challan else None,
            "owner": staff_map.get(challan.get("owner_id")) if challan else None,
            "delivery_stages": {
                "out_from_branch1": bool(transit.get("is_out_of_delivery_from_branch1")) if transit else False,
                "delivered_at_branch2": bool(transit.get("is_delivered_at_branch2")) if transit else False,
                "out_from_branch2": bool(transit.get("is_out_of_delivery_from_branch2")) if transit else False,
                "delivered_at_destination": bool(transit.get("is_delivered_at_destination")) if transit else False,
                "out_for_door_delivery": bool(transit.get("out_for_door_delivery")) if transit else False,
            } if transit else None,
        }

        ewb_numbers_this = _split_ewb_numbers(r.get("e_way_bill"))
        eway_bills = [
            {"ewb_number": e, **{k: v for k, v in (ewb_val_by_number.get(e) or {}).items() if k != "ewb_number"}}
            for e in ewb_numbers_this
        ]

        kaat = kaat_by_gr.get(gr_no)

        results.append({
            "gr_no": gr_no,
            "source_table": r["source_table"],
            "bilty_date": r.get("bilty_date"),
            "consignor_name": r.get("consignor_name"),
            "consignor_gst": r.get("consignor_gst"),
            "consignee_name": r.get("consignee_name"),
            "consignee_gst": r.get("consignee_gst"),
            "transport_name": r.get("transport_name"),
            "transport_gst": r.get("transport_gst"),
            "from_city": city_map.get(r.get("from_city_id")),
            "to_city": city_map.get(r.get("to_city_id")),
            "payment_mode": r.get("payment_mode"),
            "no_of_pkg": r.get("no_of_pkg"),
            "weight": r.get("wt"),
            "total": r.get("total"),
            "pvt_marks": r.get("pvt_marks"),
            "eway_bills": eway_bills,
            "dispatch": dispatch_info,
            "part_b_updates": part_b_by_gr.get(gr_no, []),
            "kaat_details": {
                "pohonch_no": kaat.get("pohonch_no"),
                "bilty_number": kaat.get("bilty_number"),
                "crossing_challan_no": kaat.get("crossing_challan_no"),
                "challan_no": kaat.get("challan_no"),
                "rate_type": kaat.get("rate_type"),
                "rate_per_kg": kaat.get("rate_per_kg"),
                "rate_per_pkg": kaat.get("rate_per_pkg"),
                "kaat": kaat.get("kaat"),
                "pf": kaat.get("pf"),
                "dd_chrg": kaat.get("dd_chrg"),
                "bilty_chrg": kaat.get("bilty_chrg"),
                "ewb_chrg": kaat.get("ewb_chrg"),
                "labour_chrg": kaat.get("labour_chrg"),
                "other_chrg": kaat.get("other_chrg"),
            } if kaat else None,
        })

    return {
        "status": "success",
        "data": {
            "consignor_name": consignor_name,
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_more": (offset + page_size) < total,
            "rows": results,
        },
    }
