"""
Service: Get all-transport pending bilties
Returns bilties missing BOTH pohonch_no AND bilty_number in bilty_wise_kaat,
grouped: transport → challan → serial-order.
"""
from collections import defaultdict
from services.supabase_client import get_supabase

PAGE_SIZE = 1000  # internal supabase page size


def _fetch_all(query_fn):
    """Paginate through all supabase rows for a query."""
    all_rows = []
    page = 0
    while True:
        res = query_fn(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
        rows = res.data or []
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        page += 1
    return all_rows


def get_all_transport_pending_bilties():
    sb = get_supabase()

    # 1. All transports (id → details map)
    transport_rows = _fetch_all(
        lambda lo, hi: sb.table("transports").select(
            "id,transport_name,city_name,city_id,gst_number,mob_number,is_prior"
        ).range(lo, hi).execute()
    )
    transport_map = {t["id"]: t for t in transport_rows}

    # 2. All kaat rows that have a transport_id set
    all_kaat = _fetch_all(
        lambda lo, hi: sb.table("bilty_wise_kaat").select(
            "id,gr_no,challan_no,pohonch_no,bilty_number,transport_id,destination_city_id,kaat,pf,dd_chrg"
        ).not_.is_("transport_id", "null").range(lo, hi).execute()
    )

    # 3. Keep only rows missing BOTH pohonch_no AND bilty_number
    pending = [
        r for r in all_kaat
        if not r.get("pohonch_no") and not r.get("bilty_number")
    ]

    if not pending:
        return {
            "status": "success",
            "transports": [],
            "total_bilties": 0,
            "total_transports": 0,
        }

    gr_nos = [r["gr_no"] for r in pending]

    # 4. Fetch bilty details in batches of 500
    bilty_map = {}
    for i in range(0, len(gr_nos), 500):
        chunk = gr_nos[i:i + 500]
        res = sb.table("bilty").select(
            "gr_no,no_of_pkg,wt,pvt_marks,total,freight_amount,"
            "consignor_name,consignee_name,payment_mode,delivery_type,bilty_date"
        ).in_("gr_no", chunk).execute()
        for row in (res.data or []):
            bilty_map[row["gr_no"]] = row

    # 5. Fetch station_bilty_summary for station name
    sbs_map = {}
    for i in range(0, len(gr_nos), 500):
        chunk = gr_nos[i:i + 500]
        res = sb.table("station_bilty_summary").select(
            "gr_no,station,no_of_packets,weight,amount,consignor_name,consignee_name"
        ).in_("gr_no", chunk).execute()
        for row in (res.data or []):
            sbs_map[row["gr_no"]] = row

    # 6. Group by transport_id → challan_no → bilties
    transport_challan_groups = defaultdict(lambda: defaultdict(list))
    for row in pending:
        gr = row["gr_no"]
        bilty = bilty_map.get(gr, {})
        sbs = sbs_map.get(gr, {})
        tid = row.get("transport_id") or "UNKNOWN"
        challan_no = row.get("challan_no") or "NO_CHALLAN"
        transport_challan_groups[tid][challan_no].append({
            "gr_no":          gr,
            "challan_no":     row.get("challan_no"),
            "pohonch_no":     row.get("pohonch_no"),
            "bilty_number":   row.get("bilty_number"),
            "station":        sbs.get("station") or "",
            "no_of_pkg":      bilty.get("no_of_pkg") or sbs.get("no_of_packets") or 0,
            "weight":         float(bilty.get("wt") or sbs.get("weight") or 0),
            "pvt_marks":      bilty.get("pvt_marks") or "",
            "amount":         float(bilty.get("total") or sbs.get("amount") or 0),
            "freight_amount": float(bilty.get("freight_amount") or 0),
            "kaat":           float(row.get("kaat") or 0),
            "pf":             float(row.get("pf") or 0),
            "dd_chrg":        float(row.get("dd_chrg") or 0),
            "consignor_name": bilty.get("consignor_name") or sbs.get("consignor_name") or "",
            "consignee_name": bilty.get("consignee_name") or sbs.get("consignee_name") or "",
            "payment_mode":   bilty.get("payment_mode") or "",
            "delivery_type":  bilty.get("delivery_type") or "",
            "bilty_date":     str(bilty.get("bilty_date") or ""),
        })

    # 7. Build final transport list sorted by transport_name
    transports_out = []
    for tid in sorted(
        transport_challan_groups.keys(),
        key=lambda x: (transport_map.get(x, {}).get("transport_name", ""), transport_map.get(x, {}).get("city_name", ""))
    ):
        t_info = transport_map.get(tid, {"id": tid, "transport_name": "UNKNOWN", "city_name": ""})
        challan_data = transport_challan_groups[tid]

        challans = []
        for challan_no in sorted(challan_data.keys()):
            bilties = sorted(challan_data[challan_no], key=lambda x: x["gr_no"])
            for idx, b in enumerate(bilties, 1):
                b["serial"] = idx
            challans.append({
                "challan_no":   challan_no,
                "bilty_count":  len(bilties),
                "total_weight": round(sum(b["weight"] for b in bilties), 2),
                "total_amount": round(sum(b["amount"] for b in bilties), 2),
                "total_kaat":   round(sum(b["kaat"] for b in bilties), 2),
                "bilties":      bilties,
            })

        t_total_bilties = sum(c["bilty_count"] for c in challans)
        transports_out.append({
            "transport_id":    tid,
            "transport_name":  t_info.get("transport_name", ""),
            "city_name":       t_info.get("city_name", ""),
            "gst_number":      t_info.get("gst_number", ""),
            "mob_number":      t_info.get("mob_number", ""),
            "is_prior":        t_info.get("is_prior", False),
            "total_bilties":   t_total_bilties,
            "total_challans":  len(challans),
            "challans":        challans,
        })

    return {
        "status":           "success",
        "total_transports": len(transports_out),
        "total_bilties":    len(pending),
        "transports":       transports_out,
    }
