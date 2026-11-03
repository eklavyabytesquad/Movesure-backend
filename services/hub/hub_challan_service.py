"""
Hub challan details (page /hub-management/<challan_no>)
=======================================================
One call returns what the page used to read with ~13 direct Supabase queries:
the challan header, every bilty on it (bilty / station_bilty_summary merged),
kaat rows, cross-challan (pohonch) numbers, POD GRs and Kanpur GRs.

Master data (branches, cities, transports, hub rates) is the same for every
challan, so it is cached in memory for 10 minutes and served separately
(GET /api/hub/masters), or inline with ?include_masters=true.
"""
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from services.supabase_client import get_supabase

CHUNK = 200          # .in_() batch size (URL length)
POHONCH_CHUNK = 40   # GRs per JSONB-containment OR query
MASTERS_TTL = 600    # seconds

CHALLAN_SELECT = (
    "id, challan_no, branch_id, truck_id, owner_id, driver_id, date, "
    "total_bilty_count, remarks, is_active, is_dispatched, dispatch_date, "
    "is_received_at_hub, received_at_hub_timing, received_by_user, "
    "created_by, created_at, updated_at, "
    "truck:trucks(id, truck_number, truck_type), "
    "owner:staff!challan_details_owner_id_fkey(id, name, mobile_number), "
    "driver:staff!challan_details_driver_id_fkey(id, name, mobile_number)"
)
TRANSIT_SELECT = (
    "id, challan_no, gr_no, bilty_id, challan_book_id, from_branch_id, to_branch_id, "
    "is_out_of_delivery_from_branch1, out_of_delivery_from_branch1_date, "
    "is_delivered_at_branch2, delivered_at_branch2_date, "
    "is_out_of_delivery_from_branch2, out_of_delivery_from_branch2_date, "
    "is_delivered_at_destination, delivered_at_destination_date, "
    "out_for_door_delivery, out_for_door_delivery_date, "
    "delivery_agent_name, delivery_agent_phone, vehicle_number, remarks, created_at"
)
BILTY_SELECT = (
    "id, gr_no, bilty_date, consignor_name, consignee_name, payment_mode, "
    "no_of_pkg, total, to_city_id, wt, rate, freight_amount, contain, "
    "e_way_bill, pvt_marks, consignor_number, consignee_number, consignor_gst, "
    "transport_name, delivery_type, invoice_no, invoice_value, "
    "labour_charge, bill_charge, toll_charge, dd_charge, other_charge, remark, bilty_image"
)
SBS_SELECT = (
    "id, station, gr_no, consignor, consignee, contents, no_of_packets, "
    "weight, payment_status, amount, pvt_marks, e_way_bill, delivery_type, "
    "created_at, w_name, transport_name, transport_gst, bilty_image"
)

_cache = {"at": 0.0, "data": None}
_lock = threading.Lock()


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _all(query_fn, page=1000):
    """Read every row of a query (PostgREST pages of 1000). query_fn() must return an ordered query."""
    out, start = [], 0
    while True:
        rows = query_fn().range(start, start + page - 1).execute().data or []
        out += rows
        if len(rows) < page:
            return out
        start += page


def _in(sb, table, select, col, values, extra=None):
    out = []
    for ch in _chunks(list(values), CHUNK):
        q = sb.table(table).select(select).in_(col, ch)
        if extra:
            q = extra(q)
        out += q.execute().data or []
    return out


# ── masters ────────────────────────────────────────────────────────────────
def _load_masters() -> dict:
    sb = get_supabase()
    with ThreadPoolExecutor(4) as ex:
        f_br = ex.submit(lambda: sb.table("branches").select("id, branch_name").eq("is_active", True).execute().data or [])
        f_ci = ex.submit(lambda: _all(lambda: sb.table("cities").select("id, city_name, city_code").order("city_name").order("id")))
        f_tr = ex.submit(lambda: _all(lambda: sb.table("transports")
                                      .select("id, transport_name, city_id, city_name, mob_number, gst_number, is_prior")
                                      .order("is_prior", desc=True).order("transport_name").order("id")))
        f_hr = ex.submit(lambda: _all(lambda: sb.table("transport_hub_rates").select("*").eq("is_active", True).order("id")))
        branches, cities, transports, hub_rates = f_br.result(), f_ci.result(), f_tr.result(), f_hr.result()

    transports_by_city: dict = {}
    for t in transports:
        transports_by_city.setdefault(t.get("city_id") or "none", []).append(t)
    rates_map: dict = {}   # transport_id -> destination_city_id -> [rates]
    for r in hub_rates:
        rates_map.setdefault(r.get("transport_id") or "none", {}) \
                 .setdefault(r.get("destination_city_id") or "none", []).append(r)
    return {
        "branches": branches,
        "branch_map": {b["id"]: b["branch_name"] for b in branches},
        "cities": cities,
        "transports": transports,
        "transports_by_city": transports_by_city,
        "hub_rates": hub_rates,
        "hub_rates_map": rates_map,
        "cached_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def get_masters(refresh: bool = False) -> dict:
    with _lock:
        if refresh or not _cache["data"] or time.time() - _cache["at"] > MASTERS_TTL:
            _cache["data"], _cache["at"] = _load_masters(), time.time()
        return _cache["data"]


def get_hub_masters(refresh: bool = False) -> dict:
    return {"status": "success", "data": get_masters(refresh)}


# ── per-GR lookups (also usable on their own) ──────────────────────────────
def kaat_map(gr_nos: list[str]) -> dict:
    sb = get_supabase()
    return {r["gr_no"]: r for r in _in(sb, "bilty_wise_kaat", "*", "gr_no", gr_nos)}


def pohonch_map(gr_nos: list[str]) -> dict:
    """gr_no -> pohonch_number, via a server-side JSONB containment filter (not all pohonch)."""
    sb = get_supabase()
    want, out = set(gr_nos), {}
    for ch in _chunks(list(want), POHONCH_CHUNK):
        cond = ",".join("bilty_metadata.cs." + json.dumps([{"gr_no": g}], separators=(",", ":")) for g in ch)
        rows = (sb.table("pohonch").select("pohonch_number, bilty_metadata, created_at")
                .eq("is_active", True).or_(cond).order("created_at").execute().data or [])
        for p in rows:
            for m in p.get("bilty_metadata") or []:
                g = m.get("gr_no")
                if g in want:
                    out[g] = p["pohonch_number"]     # newest pohonch wins (ordered by created_at)
    return out


def pod_gr_nos(gr_nos: list[str]) -> list[str]:
    sb = get_supabase()
    return sorted({r["gr_no"] for r in _in(sb, "pod_details", "gr_no", "gr_no", gr_nos)})


# ── the page ───────────────────────────────────────────────────────────────
def get_hub_challan(challan_no: str, include_masters: bool = False) -> dict:
    challan_no = (challan_no or "").strip()
    if not challan_no:
        return {"status": "error", "message": "challan_no is required", "status_code": 400}
    sb = get_supabase()
    masters = get_masters()
    branch_map = masters["branch_map"]
    city_by_id = {c["id"]: c for c in masters["cities"]}
    city_by_code = {(c.get("city_code") or "").upper(): c for c in masters["cities"] if c.get("city_code")}

    ch = sb.table("challan_details").select(CHALLAN_SELECT).eq("challan_no", challan_no).limit(1).execute().data
    if not ch:
        return {"status": "error", "message": f"Challan '{challan_no}' not found", "status_code": 404}
    challan = ch[0]
    challan["branch_name"] = branch_map.get(challan.get("branch_id"))

    transit = _all(lambda: sb.table("transit_details").select(TRANSIT_SELECT)
                   .eq("challan_no", challan_no).order("created_at").order("id"))
    gr_nos = list(dict.fromkeys(t["gr_no"] for t in transit if t.get("gr_no")))

    bilty_map, sbs_map, kaat, pohonch, pods = {}, {}, {}, {}, []
    if gr_nos:
        with ThreadPoolExecutor(5) as ex:
            f_b = ex.submit(_in, sb, "bilty", BILTY_SELECT, "gr_no", gr_nos, lambda q: q.eq("is_active", True))
            f_s = ex.submit(_in, sb, "station_bilty_summary", SBS_SELECT, "gr_no", gr_nos)
            f_k = ex.submit(kaat_map, gr_nos)
            f_p = ex.submit(pohonch_map, gr_nos)
            f_d = ex.submit(pod_gr_nos, gr_nos)
            bilty_map = {r["gr_no"]: r for r in f_b.result()}
            sbs_map = {r["gr_no"]: r for r in f_s.result()}
            kaat, pohonch, pods = f_k.result(), f_p.result(), f_d.result()

    bilties, kanpur = [], set()
    for i, t in enumerate(transit, 1):
        g = t.get("gr_no")
        b, s = bilty_map.get(g), (None if g in bilty_map else sbs_map.get(g))
        row = {**t, "idx": i,
               "from_branch_name": branch_map.get(t.get("from_branch_id")),
               "to_branch_name": branch_map.get(t.get("to_branch_id"))}
        if b:
            city = city_by_id.get(b.get("to_city_id")) or {}
            row.update({
                "source": "bilty", "bilty_row_id": b["id"],
                "consignor": b.get("consignor_name"), "consignee": b.get("consignee_name"),
                "destination": city.get("city_name") or "", "destination_code": city.get("city_code") or "",
                "to_city_id": b.get("to_city_id"),
                "packets": b.get("no_of_pkg"), "weight": b.get("wt"), "amount": b.get("total"),
                "payment": b.get("payment_mode"), "contain": b.get("contain"),
                "bilty_date": b.get("bilty_date"), "freight_amount": b.get("freight_amount"),
                "rate": b.get("rate"), "labour_charge": b.get("labour_charge") or 0,
                "bill_charge": b.get("bill_charge") or 0, "toll_charge": b.get("toll_charge") or 0,
                "dd_charge": b.get("dd_charge") or 0, "other_charge": b.get("other_charge") or 0,
                "consignor_number": b.get("consignor_number"), "consignee_number": b.get("consignee_number"),
                "consignor_gst": b.get("consignor_gst"), "e_way_bill": b.get("e_way_bill"),
                "pvt_marks": b.get("pvt_marks"), "transport_name": b.get("transport_name"),
                "delivery_type": b.get("delivery_type"), "invoice_no": b.get("invoice_no"),
                "invoice_value": b.get("invoice_value"), "bilty_image": b.get("bilty_image"),
                "w_name": None, "remark": b.get("remark") or t.get("remarks"),
            })
        elif s:
            code = (s.get("station") or "").upper()
            city = city_by_code.get(code) or {}
            row.update({
                "source": "station_bilty_summary", "bilty_row_id": s["id"],
                "consignor": s.get("consignor"), "consignee": s.get("consignee"),
                "destination": city.get("city_name") or s.get("station") or "", "destination_code": s.get("station") or "",
                "to_city_id": city.get("id"),
                "packets": s.get("no_of_packets"), "weight": s.get("weight"), "amount": s.get("amount"),
                "payment": s.get("payment_status"), "contain": s.get("contents"),
                "bilty_date": s.get("created_at"), "freight_amount": s.get("amount"),
                "rate": None, "labour_charge": 0, "bill_charge": 0, "toll_charge": 0, "dd_charge": 0, "other_charge": 0,
                "consignor_number": "", "consignee_number": "", "consignor_gst": "",
                "e_way_bill": s.get("e_way_bill"), "pvt_marks": s.get("pvt_marks"),
                "transport_name": s.get("transport_name"), "transport_gst": s.get("transport_gst"),
                "delivery_type": s.get("delivery_type"), "invoice_no": None, "invoice_value": None,
                "bilty_image": s.get("bilty_image"), "w_name": s.get("w_name"), "remark": t.get("remarks"),
            })
        else:
            row.update({"source": "unknown", "consignor": None, "consignee": None, "destination": "",
                        "destination_code": "", "packets": 0, "weight": 0, "amount": 0, "payment": None,
                        "remark": t.get("remarks")})
        if "kanpur" in (row.get("destination") or "").lower():
            kanpur.add(g)
        bilties.append(row)

    data = {
        "challan": challan,
        "bilties": bilties,
        "kaat": kaat,                       # gr_no -> bilty_wise_kaat row
        "pohonch_map": pohonch,             # gr_no -> pohonch_number (cross challan)
        "pod_gr_nos": pods,                 # GRs that already have a POD
        "kanpur_gr_nos": sorted(kanpur),    # GRs going to a Kanpur city
        "summary": {
            "total_bilties": len(bilties),
            "from_bilty": sum(1 for b in bilties if b["source"] == "bilty"),
            "from_station": sum(1 for b in bilties if b["source"] == "station_bilty_summary"),
            "unknown": sum(1 for b in bilties if b["source"] == "unknown"),
            "total_packets": sum(float(b.get("packets") or 0) for b in bilties),
            "total_weight": round(sum(float(b.get("weight") or 0) for b in bilties), 2),
            "total_amount": round(sum(float(b.get("amount") or 0) for b in bilties), 2),
            "with_kaat": sum(1 for g in gr_nos if g in kaat),
            "with_pod": len(pods),
            "with_pohonch": len(pohonch),
        },
    }
    if include_masters:
        data["masters"] = masters
    return {"status": "success", "data": data}
