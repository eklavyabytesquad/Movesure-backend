"""
Kaat Bill Report Service

Given a transport GSTIN and a date range, returns bilty records with kaat details.
Includes per-bilty details and summary totals.
"""

from datetime import date
from services.supabase_client import get_supabase


def _safe(val, default=""):
    return val if val is not None else default


def _next_day(date_str: str) -> str:
    from datetime import timedelta
    return str(date.fromisoformat(date_str) + timedelta(days=1))


PAGE_SIZE = 1000


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def get_kaat_bill_report(
    transport_gstin=None,
    from_date=None,
    to_date=None,
):
    """
    Get kaat bill report for a transport GSTIN within a date range.

    Args:
        transport_gstin: GSTIN of the transport company
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)

    Returns:
        Dict with status, summary, and bilties list
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
    to_date_exclusive = _next_day(to_date)

    bilties = _fetch_bilties_with_kaat(sb, transport_gstin, from_date, to_date)

    if not bilties:
        return {
            "status": "success",
            "from_date": from_date,
            "to_date": to_date,
            "transport_gstin": transport_gstin,
            "summary": {
                "total_bilties": 0,
                "total_weight": 0,
                "total_dd": 0,
                "total_kaat": 0,
                "total_to_pay": 0,
                "total_pf": 0,
            },
            "bilties": [],
        }

    city_ids = {b.get("to_city_id") for b in bilties if b.get("to_city_id")}
    city_map = _fetch_cities(sb, city_ids)

    processed_bilties = []
    total_weight = 0
    total_dd = 0
    total_kaat = 0
    total_to_pay = 0
    total_pf = 0

    for b in bilties:
        gr_no = b.get("gr_no", "")
        weight = b.get("wt") or 0
        dd = b.get("dd_chrg") or 0
        kaat = b.get("kaat") or 0
        pf = b.get("pf")
        payment_mode = _safe(b.get("payment_mode", "")).lower()

        destination = city_map.get(b.get("to_city_id"), "")
        kaat_rate = b.get("actual_kaat_rate") or 0

        total = b.get("total") or 0
        no_of_pkg = b.get("no_of_pkg") or 0
        pvt_marks = _safe(b.get("pvt_marks"))
        delivery_type = _safe(b.get("delivery_type", ""))
        pohonch_number = _safe(b.get("pohonch_number"))
        pohonch_no_kaat = _safe(b.get("pohonch_no"))

        if payment_mode == "paid":
            to_pay = None
            pf_display = "PAID"
        else:
            to_pay = round(total - kaat - dd, 2) if total else 0
            pf_display = pf

        processed_bilties.append({
            "gr_no": gr_no,
            "pohonch_no_kaat": pohonch_no_kaat,
            "pohonch_number": pohonch_number,
            "destination": destination,
            "payment_mode": payment_mode,
            "delivery_type": delivery_type,
            "pkgs": no_of_pkg,
            "pvt_marks": pvt_marks,
            "weight": weight,
            "kaat_rate": kaat_rate,
            "dd": dd,
            "kaat": kaat,
            "to_pay": to_pay,
            "pf": pf_display,
            "total": total,
        })

        total_weight += weight
        total_dd += dd
        total_kaat += kaat
        if to_pay is not None:
            total_to_pay += to_pay
        if pf_display != "PAID" and pf_display:
            total_pf += pf_display

    total_pf = round(total_to_pay - total_kaat - total_dd, 2)

    return {
        "status": "success",
        "from_date": from_date,
        "to_date": to_date,
        "transport_gstin": transport_gstin,
        "summary": {
            "total_bilties": len(processed_bilties),
            "total_weight": round(total_weight, 2),
            "total_dd": round(total_dd, 2),
            "total_kaat": round(total_kaat, 2),
            "total_to_pay": round(total_to_pay, 2),
            "total_pf": total_pf,
        },
        "bilties": processed_bilties,
    }


def _fetch_bilties_with_kaat(sb, transport_gstin, from_date, to_date):
    """
    Fetch bilties with kaat details for a given transport GSTIN and date range.
    Joins bilty, bilty_wise_kaat, and pohonch tables.
    """
    rows = []
    page = 0

    while True:
        lo, hi = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE - 1

        q = (
            sb.table("bilty")
            .select(
                "gr_no, transport_gst, bilty_date, wt, total, no_of_pkg, "
                "payment_mode, pvt_marks, to_city_id, delivery_type, "
                "bilty_wise_kaat(challan_no, pohonch_no, bilty_number, kaat, pf, dd_chrg, actual_kaat_rate), "
                "pohonch(pohonch_number)"
            )
            .eq("is_active", True)
            .eq("transport_gst", transport_gstin.strip().upper())
            .gte("bilty_date", from_date)
            .lte("bilty_date", to_date)
            .range(lo, hi)
        )

        res = q.execute()
        batch = res.data or []

        for row in batch:
            kaat_data = row.get("bilty_wise_kaat")
            if not kaat_data:
                continue

            if isinstance(kaat_data, list) and kaat_data:
                kaat_info = kaat_data[0]
            elif isinstance(kaat_data, dict):
                kaat_info = kaat_data
            else:
                continue

            pohonch_data = row.get("pohonch")
            pohonch_number = ""
            if isinstance(pohonch_data, list) and pohonch_data:
                pohonch_number = pohonch_data[0].get("pohonch_number", "")
            elif isinstance(pohonch_data, dict):
                pohonch_number = pohonch_data.get("pohonch_number", "")

            processed_row = {
                "gr_no": row.get("gr_no", ""),
                "wt": row.get("wt") or 0,
                "total": row.get("total") or 0,
                "no_of_pkg": row.get("no_of_pkg") or 0,
                "payment_mode": _safe(row.get("payment_mode", "")),
                "pvt_marks": _safe(row.get("pvt_marks")),
                "to_city_id": row.get("to_city_id"),
                "delivery_type": _safe(row.get("delivery_type", "")),
                "kaat": kaat_info.get("kaat") or 0,
                "pf": kaat_info.get("pf"),
                "dd_chrg": kaat_info.get("dd_chrg") or 0,
                "actual_kaat_rate": kaat_info.get("actual_kaat_rate") or 0,
                "pohonch_no": _safe(kaat_info.get("pohonch_no")),
                "pohonch_number": pohonch_number,
            }
            rows.append(processed_row)

        if len(batch) < PAGE_SIZE:
            break
        page += 1

    return rows


def _fetch_cities(sb, city_ids):
    """Fetch city names by IDs."""
    city_map = {}
    if not city_ids:
        return city_map

    for chunk in _chunks(list(city_ids), 100):
        res = sb.table("cities").select("id, city_name").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = c.get("city_name", "")

    return city_map
