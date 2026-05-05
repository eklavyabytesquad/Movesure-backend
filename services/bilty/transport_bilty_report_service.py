"""
Transport Bilty Report Service
────────────────────────────────
Given a transport GSTIN (or transport_name) and a date range, returns every
bilty for that transport across two sources:
  1. bilty table          (uses bilty_date)
  2. station_bilty_summary (uses created_at date)

For each bilty it also resolves:
  - pohonch_number     – our internal pohonch code  (e.g. HC0002)
  - has_crossing_challan – True if crossing challans exist
  - crossing_challans  – pipe-separated challan numbers  (e.g. "0239 | B00017")
  - dest_pohonch_no    – destination-side bilty number from bilty_wise_kaat
  - kaat / kaat_pf / kaat_dd / kaat_rate

Result is split into two sorted lists:
  with_pohonch    – ascending gr_no
  without_pohonch – ascending gr_no (appended after)
"""

from datetime import date
from services.supabase_client import get_supabase

PAGE_SIZE = 1000


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _safe(val, default=""):
    return val if val is not None else default


# ─────────────────────────────────────────────────────────────────────────────

def get_transport_bilty_report(
    transport_gstin: str | None = None,
    transport_name: str | None = None,
    from_date: str = None,
    to_date: str = None,
) -> dict:
    """
    Fetch all bilties for a transport in a date range.

    Priority:  transport_gstin (exact) → transport_name (partial match).
    Dates are inclusive: from_date <= bilty_date <= to_date  (YYYY-MM-DD).
    """
    if not transport_gstin and not transport_name:
        return {
            "status": "error",
            "message": "transport_gstin or transport_name is required",
            "status_code": 400,
        }
    if not from_date or not to_date:
        return {
            "status": "error",
            "message": "from_date and to_date are required (YYYY-MM-DD)",
            "status_code": 400,
        }

    # Validate dates
    try:
        date.fromisoformat(from_date)
        date.fromisoformat(to_date)
    except ValueError:
        return {
            "status": "error",
            "message": "Invalid date format. Use YYYY-MM-DD",
            "status_code": 400,
        }

    sb = get_supabase()
    to_date_exclusive = str(date.fromisoformat(to_date).replace(day=date.fromisoformat(to_date).day + 1) if False else _next_day(to_date))

    # ── 1a. bilty table ───────────────────────────────────────────────────────
    bilty_rows = _fetch_bilty(sb, transport_gstin, transport_name, from_date, to_date)

    # ── 1b. station_bilty_summary ─────────────────────────────────────────────
    sbs_rows = _fetch_station_bilty_summary(sb, transport_gstin, transport_name, from_date, to_date_exclusive)

    # ── 1c. Merge (bilty wins on duplicate gr_no) ─────────────────────────────
    seen = set()
    unified = []

    for b in bilty_rows:
        gr = b["gr_no"]
        seen.add(gr)
        unified.append({
            "source":         "bilty",
            "gr_no":          gr,
            "bilty_date":     _safe(b.get("bilty_date")),
            "transport_name": _safe(b.get("transport_name")),
            "transport_gst":  _safe(b.get("transport_gst")),
            "consignor_name": _safe(b.get("consignor_name")),
            "consignee_name": _safe(b.get("consignee_name")),
            "city_id_from":   b.get("from_city_id"),
            "city_id_to":     b.get("to_city_id"),
            "payment_mode":   _safe(b.get("payment_mode")),
            "no_of_pkg":      b.get("no_of_pkg") or 0,
            "wt":             b.get("wt") or 0,
            "freight_amount": b.get("freight_amount") or 0,
            "pf_charge":      b.get("pf_charge") or 0,
            "dd_charge":      b.get("dd_charge") or 0,
            "labour_charge":  b.get("labour_charge") or 0,
            "bill_charge":    b.get("bill_charge") or 0,
            "toll_charge":    b.get("toll_charge") or 0,
            "other_charge":   b.get("other_charge") or 0,
            "total":          b.get("total") or 0,
            "contain":        _safe(b.get("contain")),
            "pvt_marks":      _safe(b.get("pvt_marks")),
            "remark":         _safe(b.get("remark")),
        })

    for s in sbs_rows:
        gr = s["gr_no"]
        if gr in seen:
            continue
        seen.add(gr)
        created_raw = s.get("created_at") or ""
        unified.append({
            "source":         "station_bilty_summary",
            "gr_no":          gr,
            "bilty_date":     created_raw[:10],
            "transport_name": _safe(s.get("transport_name")),
            "transport_gst":  _safe(s.get("transport_gst")),
            "consignor_name": _safe(s.get("consignor")),
            "consignee_name": _safe(s.get("consignee")),
            "city_id_from":   None,
            "city_id_to":     s.get("city_id"),
            "payment_mode":   _safe(s.get("payment_status")),
            "no_of_pkg":      s.get("no_of_packets") or 0,
            "wt":             s.get("weight") or 0,
            "freight_amount": s.get("amount") or 0,
            "pf_charge":      0,
            "dd_charge":      0,
            "labour_charge":  0,
            "bill_charge":    0,
            "toll_charge":    0,
            "other_charge":   0,
            "total":          s.get("amount") or 0,
            "contain":        _safe(s.get("contents")),
            "pvt_marks":      _safe(s.get("pvt_marks")),
            "remark":         _safe(s.get("delivery_type")),
        })

    if not unified:
        return {
            "status": "success",
            "from_date": from_date,
            "to_date": to_date,
            "transport_gstin": transport_gstin,
            "transport_name": transport_name,
            "total": 0,
            "with_pohonch_count": 0,
            "without_pohonch_count": 0,
            "bilties": [],
        }

    gr_nos = [b["gr_no"] for b in unified]

    # ── 2. bilty_wise_kaat ────────────────────────────────────────────────────
    kaat_map = _fetch_kaat(sb, gr_nos)

    # ── 3. Pohonch records → gr_no maps ──────────────────────────────────────
    transport_filter = transport_gstin or transport_name
    is_gstin = bool(transport_gstin)
    gr_to_pohonch_number, gr_to_crossing_challans = _fetch_pohonch_maps(
        sb, transport_filter, is_gstin
    )

    # ── 4. City names ─────────────────────────────────────────────────────────
    city_ids = {b["city_id_from"] for b in unified if b.get("city_id_from")} | \
               {b["city_id_to"]   for b in unified if b.get("city_id_to")}
    city_map = _fetch_cities(sb, city_ids)

    # ── 5. Enrich & split ─────────────────────────────────────────────────────
    with_pohonch    = []
    without_pohonch = []

    for b in unified:
        gr   = b["gr_no"]
        kaat = kaat_map.get(gr, {})
        pohonch_number    = gr_to_pohonch_number.get(gr, "")
        crossing_challans = gr_to_crossing_challans.get(gr, "")
        dest_pohonch_no   = _safe(kaat.get("pohonch_no")).strip()

        row = {
            "source":               b["source"],
            "gr_no":                gr,
            "bilty_date":           b["bilty_date"],
            "transport_name":       b["transport_name"],
            "transport_gst":        b["transport_gst"],
            "consignor_name":       b["consignor_name"],
            "consignee_name":       b["consignee_name"],
            "from_city":            city_map.get(b["city_id_from"], "") if b.get("city_id_from") else "",
            "to_city":              city_map.get(b["city_id_to"], "")   if b.get("city_id_to")   else "",
            "payment_mode":         b["payment_mode"],
            "no_of_pkg":            b["no_of_pkg"],
            "wt":                   b["wt"],
            "freight_amount":       b["freight_amount"],
            "pf_charge":            b["pf_charge"],
            "dd_charge":            b["dd_charge"],
            "labour_charge":        b["labour_charge"],
            "bill_charge":          b["bill_charge"],
            "toll_charge":          b["toll_charge"],
            "other_charge":         b["other_charge"],
            "total":                b["total"],
            "contain":              b["contain"],
            "pvt_marks":            b["pvt_marks"],
            "remark":               b["remark"],
            "pohonch_number":       pohonch_number,
            "has_crossing_challan": bool(crossing_challans),
            "crossing_challans":    crossing_challans,
            "dest_pohonch_no":      dest_pohonch_no,
            "bilty_number":         _safe(kaat.get("bilty_number")),
            "kaat":                 kaat.get("kaat"),
            "kaat_pf":              kaat.get("pf"),
            "kaat_dd":              kaat.get("dd_chrg"),
            "kaat_rate":            kaat.get("actual_kaat_rate"),
        }

        if pohonch_number:
            with_pohonch.append(row)
        else:
            without_pohonch.append(row)

    with_pohonch.sort(key=lambda x: x["gr_no"])
    without_pohonch.sort(key=lambda x: x["gr_no"])

    bilties = with_pohonch + without_pohonch

    return {
        "status": "success",
        "from_date": from_date,
        "to_date": to_date,
        "transport_gstin": transport_gstin or "",
        "transport_name": (with_pohonch or without_pohonch or [{}])[0].get("transport_name", transport_name or ""),
        "sources": {
            "bilty_table": len(bilty_rows),
            "station_bilty_summary": len(sbs_rows),
        },
        "total": len(bilties),
        "with_pohonch_count": len(with_pohonch),
        "without_pohonch_count": len(without_pohonch),
        "bilties": bilties,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _next_day(date_str: str) -> str:
    """Return the calendar day after date_str as a string."""
    from datetime import date as _date, timedelta
    return str(_date.fromisoformat(date_str) + timedelta(days=1))


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
            .select("gr_no, pohonch_no, bilty_number, kaat, pf, dd_chrg, actual_kaat_rate")
            .in_("gr_no", chunk)
            .execute()
        )
        for row in res.data or []:
            gr = row.get("gr_no")
            if gr and gr not in kaat_map:
                kaat_map[gr] = row
    return kaat_map


def _fetch_pohonch_maps(sb, transport_filter, is_gstin):
    """Return (gr_to_pohonch_number, gr_to_crossing_challans) dicts."""
    gr_to_pohonch_number    = {}
    gr_to_crossing_challans = {}

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
            pno      = p.get("pohonch_number", "")
            challans = p.get("challan_metadata") or []
            crossing = " | ".join(str(c) for c in challans) if challans else ""
            for entry in (p.get("bilty_metadata") or []):
                gr = entry.get("gr_no") if isinstance(entry, dict) else None
                if gr:
                    gr_to_pohonch_number[gr]    = pno
                    gr_to_crossing_challans[gr] = crossing

        if len(batch) < PAGE_SIZE:
            break
        page += 1

    return gr_to_pohonch_number, gr_to_crossing_challans


def _fetch_cities(sb, city_ids):
    city_map = {}
    if not city_ids:
        return city_map
    for chunk in _chunks(list(city_ids), 100):
        res = sb.table("cities").select("id, city_name").in_("id", chunk).execute()
        for c in res.data or []:
            city_map[c["id"]] = c.get("city_name", "")
    return city_map
