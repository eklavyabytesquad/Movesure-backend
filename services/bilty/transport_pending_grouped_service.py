"""
Service: Get grouped transport pending bilties (by GSTIN)
POST: dispatch_date_from, dispatch_date_to
Returns bilties missing BOTH pohonch_no AND bilty_number in bilty_wise_kaat,
grouped: GSTIN → transport_names[] → challan → serial-order.
"""
from collections import defaultdict
from services.supabase_client import get_supabase
from datetime import datetime

PAGE_SIZE = 1000

def _fetch_all(query_fn):
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


def get_grouped_transport_pending_bilties(dispatch_date_from: str, dispatch_date_to: str):
    sb = get_supabase()
    # 1. Parse dates
    try:
        dt_from = datetime.fromisoformat(dispatch_date_from)
        dt_to = datetime.fromisoformat(dispatch_date_to)
    except Exception:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD or ISO format."}

    # 2. Find all challans in date range
    challans = _fetch_all(
        lambda lo, hi: sb.table("challan_details").select(
            "challan_no,dispatch_date"
        ).gte("dispatch_date", dt_from.isoformat()).lte("dispatch_date", dt_to.isoformat()).range(lo, hi).execute()
    )
    challan_nos = [c["challan_no"] for c in challans]
    if not challan_nos:
        return {"status": "success", "groups": [], "total_bilties": 0, "total_groups": 0}

    # 3. All transports (id → details map, gstin → list of transports)
    transport_rows = _fetch_all(
        lambda lo, hi: sb.table("transports").select(
            "id,transport_name,city_name,city_id,gst_number,mob_number,is_prior"
        ).range(lo, hi).execute()
    )
    transport_map = {t["id"]: t for t in transport_rows}
    gstin_map = defaultdict(list)
    for t in transport_rows:
        gst = t.get("gst_number") or "NO_GSTIN"
        gstin_map[gst].append(t)

    # 4. All kaat rows for these challans (batched in chunks of 200 to avoid URL length limits)
    all_kaat = []
    for _ci in range(0, len(challan_nos), 200):
        _chunk = challan_nos[_ci:_ci + 200]
        all_kaat.extend(_fetch_all(
            lambda lo, hi, c=_chunk: sb.table("bilty_wise_kaat").select(
                "id,gr_no,challan_no,pohonch_no,bilty_number,transport_id,destination_city_id,kaat,pf,dd_chrg"
            ).in_("challan_no", c).not_.is_("transport_id", "null").range(lo, hi).execute()
        ))
    # 5. Only rows missing BOTH pohonch_no AND bilty_number
    pending = [r for r in all_kaat if not r.get("pohonch_no") and not r.get("bilty_number")]
    if not pending:
        return {"status": "success", "groups": [], "total_bilties": 0, "total_groups": 0}

    gr_nos = [r["gr_no"] for r in pending]

    # 6. Fetch bilty details in batches
    bilty_map = {}
    for i in range(0, len(gr_nos), 500):
        chunk = gr_nos[i:i + 500]
        res = sb.table("bilty").select(
            "gr_no,no_of_pkg,wt,pvt_marks,total,freight_amount,"
            "consignor_name,consignee_name,payment_mode,delivery_type,bilty_date"
        ).in_("gr_no", chunk).execute()
        for row in (res.data or []):
            bilty_map[row["gr_no"]] = row

    # 7. Fetch station_bilty_summary for station name
    sbs_map = {}
    for i in range(0, len(gr_nos), 500):
        chunk = gr_nos[i:i + 500]
        res = sb.table("station_bilty_summary").select(
            "gr_no,station,no_of_packets,weight,amount,consignor,consignee,payment_status,delivery_type,created_at"
        ).in_("gr_no", chunk).execute()
        for row in (res.data or []):
            sbs_map[row["gr_no"]] = row

    # 8. Group by GSTIN → challan → bilties
    gstin_challan_groups = defaultdict(lambda: defaultdict(list))
    for row in pending:
        gr = row["gr_no"]
        bilty = bilty_map.get(gr, {})
        sbs = sbs_map.get(gr, {})
        tid = row.get("transport_id")
        t = transport_map.get(tid, {})
        gst = t.get("gst_number") or "NO_GSTIN"
        challan_no = row.get("challan_no") or "NO_CHALLAN"
        gstin_challan_groups[gst][challan_no].append({
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
            "consignor_name": bilty.get("consignor_name") or sbs.get("consignor") or "",
            "consignee_name": bilty.get("consignee_name") or sbs.get("consignee") or "",
            "payment_mode":   bilty.get("payment_mode") or sbs.get("payment_status") or "",
            "delivery_type":  bilty.get("delivery_type") or sbs.get("delivery_type") or "",
            "bilty_date":     str(bilty.get("bilty_date") or (sbs.get("created_at") or "")[:10]),
            "transport_id":   tid,
            "transport_name": t.get("transport_name", ""),
            "city_name":      t.get("city_name", ""),
        })

    # 9. Build final GSTIN group list
    groups_out = []
    for gst in sorted(gstin_challan_groups.keys()):
        transports = gstin_map.get(gst, [])
        transport_names = sorted(set(t["transport_name"] for t in transports))
        city_names = sorted(set(t["city_name"] for t in transports))
        mob_numbers = sorted(set(t["mob_number"] for t in transports if t.get("mob_number")))
        is_prior = any(t.get("is_prior") for t in transports)
        challan_data = gstin_challan_groups[gst]
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
        total_bilties = sum(c["bilty_count"] for c in challans)
        groups_out.append({
            "gst_number":      gst,
            "transport_names": transport_names,
            "city_names":      city_names,
            "mob_numbers":     mob_numbers,
            "is_prior":        is_prior,
            "total_bilties":   total_bilties,
            "total_challans":  len(challans),
            "challans":        challans,
        })
    return {
        "status":           "success",
        "total_groups":     len(groups_out),
        "total_bilties":    sum(g["total_bilties"] for g in groups_out),
        "groups":           groups_out,
    }
