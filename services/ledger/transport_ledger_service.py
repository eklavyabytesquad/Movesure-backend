"""
TRANSPORT LEDGERS — per bilty, from challans received at the hub
=================================================================
Every challan received at the hub (challan_details.is_received_at_hub) lists its
bilties (transit_details). Each bilty is handed to a delivery transport at its
station (Gorakhpur -> SWASTIK TRANSPORT, Kanpur -> SS TRANSPORT CORPORATION …).
Moving a bilty into that transport's ledger posts ONE journal voucher:

  to-pay : the transport collects the freight from the consignee and keeps its kaat
           Cr Freight Income (total) · Dr Transport Kaat (kaat + dd) · Dr Transport (pf)
  paid   : we already have the freight; we owe the transport its kaat
           Dr Transport Kaat (kaat)                                  · Cr Transport (kaat)

  pf = total - kaat - dd (to-pay)  /  -kaat (paid) — the same numbers bilty_wise_kaat,
  pohonch and crossing bills already use. Transport ledger balance = sum of pf:
  Dr = the transport owes us, Cr = we owe the transport.

Kaat comes from bilty_wise_kaat (the existing kaat screens) when it was worked out
for the same transport; otherwise from that transport's station rate
(transport_hub_rates: per_kg x weight / per_pkg x packages, min_charge).
A bilty with no kaat is NOT posted unless its ledger has allow_zero_kaat on (our own
delivery, e.g. SS TRANSPORT CORPORATION in Kanpur) or the request sends allow_zero_kaat.

One ledger per transport GSTIN: all transports rows sharing a GSTIN (one per city)
are one ledger. Ledgers sit under the Transporters group, so the Transport PF screen's
balance / bills / Collect / Give work on them unchanged — each bilty is a bill there.

Bilties no transport can be found for stay unassigned and are listed with a reason.

Guards against counting the same money twice — such bilties are flagged and skipped
unless the request says include_settled:
  - already settled through a pohonch / crossing challan (bilty_wise_kaat.pohonch_no /
    crossing_challan_no) — about 64% of received bilties today
  - to-pay bilty already owed by its consignee in a party ledger (always blocked)
  - GR already booked as Kanpur Delivery income (always blocked)
"""
from datetime import date, timedelta
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.ledger.ledger_service import get_ledger, get_ledger_balance, get_ledger_statement
from services.ledger.voucher_service import VOUCHER_PREFIX, source_supported, cancel_voucher, update_voucher, last_voucher_seq
from services.ledger.ledger_helpers import get_or_create_named_ledger, today_ist
from services.ledger.transporter_service import create_transporter, _transporters_group_id

FREIGHT_INCOME_LEDGER_NAME = "Freight Income"
DIRECT_INCOMES_GROUP_NAME = "Direct Incomes"
KAAT_EXPENSE_LEDGER_NAME = "Transport Kaat"
DIRECT_EXPENSES_GROUP_NAME = "Direct Expenses"
CHUNK = 200
POST_CHUNK = 250

REG_COLS = ("id, gr_no, bilty_date, branch_id, to_city_id, consignor_name, consignee_name, payment_mode, "
            "no_of_pkg, wt, total, transport_id, transport_gst, transport_name, is_active")
MAN_COLS = ("id, gr_no, created_at, branch_id, city_id, station, consignor, consignee, payment_status, "
            "no_of_packets, weight, amount, transport_id, transport_gst, transport_name")
LINK_COLS = ("id, ledger_id, gr_no, bilty_type, bilty_id, challan_no, bilty_date, station, consignor, consignee, "
             "payment_mode, packages, weight, total, kaat_rate, kaat, dd, pf, kaat_source, transport_id, "
             "voucher_id, bill_reference_id, is_active, added_via, added_by, created_at")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _err(msg: str, code: int = 400, **extra) -> dict:
    return {"status": "error", "message": msg, "status_code": code, **extra}


def _chunks(lst, n=CHUNK):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _in(sb, table: str, cols: str, col: str, values, extra=None) -> list[dict]:
    """select … where col in values — chunked (big in_() lists overflow the URL)."""
    out = []
    for part in _chunks(list(dict.fromkeys(v for v in values if v)), CHUNK):
        q = sb.table(table).select(cols).in_(col, part)
        if extra:
            q = extra(q)
        out += q.execute().data or []
    return out


def _paged(fn) -> list[dict]:
    rows, off = [], 0
    while True:
        r = fn().range(off, off + 999).execute().data or []
        rows += r
        if len(r) < 1000:
            return rows
        off += 1000


def _gst(v) -> str | None:
    v = (v or "").strip().upper()
    return v or None


def _pay(v) -> str:
    v = str(v or "").strip().lower()
    return "foc" if v in ("freeofcost", "foc", "free") else (v or "to-pay")


# ── Loading received bilties ─────────────────────────────────────────────────
def _received_challans(sb, from_date=None, to_date=None, branch_id=None) -> list[dict]:
    def q():
        x = sb.table("challan_details").select(
            "id, challan_no, date, branch_id, total_bilty_count, is_received_at_hub, received_at_hub_timing, is_active") \
            .eq("is_received_at_hub", True)
        if from_date:
            x = x.gte("date", from_date)
        if to_date:
            x = x.lte("date", to_date)
        if branch_id:
            x = x.eq("branch_id", branch_id)
        return x.order("date", desc=True)
    return [c for c in _paged(q) if c.get("is_active") is not False]


def _load_bilties(sb, transit_rows: list[dict]) -> dict[str, dict]:
    """{gr_no: bilty in one shape} for transit rows (regular via bilty_id, manual via gr_no)."""
    out = {}
    reg = _in(sb, "bilty", REG_COLS, "id", [t["bilty_id"] for t in transit_rows if t.get("bilty_id")])
    for r in reg:
        out[r["gr_no"]] = {
            "gr_no": r["gr_no"], "bilty_type": "regular", "bilty_id": r["id"], "bilty_date": r.get("bilty_date"),
            "to_city_id": r.get("to_city_id"), "station_code": None,
            "consignor": r.get("consignor_name"), "consignee": r.get("consignee_name"),
            "payment_mode": _pay(r.get("payment_mode")), "packages": r.get("no_of_pkg") or 0,
            "weight": float(r.get("wt") or 0), "total": round(float(r.get("total") or 0), 2),
            "bilty_transport_id": r.get("transport_id"), "bilty_transport_gst": _gst(r.get("transport_gst")),
            "bilty_transport_name": r.get("transport_name"), "is_deleted": r.get("is_active") is False,
        }
    manual_grs = [t["gr_no"] for t in transit_rows if not t.get("bilty_id") and t["gr_no"] not in out]
    for r in _in(sb, "station_bilty_summary", MAN_COLS, "gr_no", manual_grs):
        if r["gr_no"] in out:
            continue
        out[r["gr_no"]] = {
            "gr_no": r["gr_no"], "bilty_type": "manual", "bilty_id": r["id"],
            "bilty_date": (r.get("created_at") or "")[:10] or None,
            "to_city_id": r.get("city_id"), "station_code": r.get("station"),
            "consignor": r.get("consignor"), "consignee": r.get("consignee"),
            "payment_mode": _pay(r.get("payment_status")), "packages": r.get("no_of_packets") or 0,
            "weight": float(r.get("weight") or 0), "total": round(float(r.get("amount") or 0), 2),
            "bilty_transport_id": r.get("transport_id"), "bilty_transport_gst": _gst(r.get("transport_gst")),
            "bilty_transport_name": r.get("transport_name"), "is_deleted": False,
        }
    return out


class _Ctx:
    """Everything detection + kaat need, fetched once per request."""

    def __init__(self, sb, bilties: dict[str, dict]):
        self.sb = sb
        grs = list(bilties)
        self.kaat = {r["gr_no"]: r for r in _in(sb, "bilty_wise_kaat",
                     "gr_no, transport_id, kaat, pf, dd_chrg, actual_kaat_rate, challan_no, pohonch_no, crossing_challan_no",
                     "gr_no", grs)}
        # station codes -> city ids (manual bilties sometimes only carry the code)
        codes = {b["station_code"] for b in bilties.values() if b.get("station_code") and not b.get("to_city_id")}
        code_city = {c["city_code"]: c for c in _in(sb, "cities", "id, city_code, city_name", "city_code", codes)}
        for b in bilties.values():
            if not b.get("to_city_id") and b.get("station_code") in code_city:
                b["to_city_id"] = code_city[b["station_code"]]["id"]
        city_ids = {b["to_city_id"] for b in bilties.values() if b.get("to_city_id")}
        self.cities = {c["id"]: c for c in _in(sb, "cities", "id, city_code, city_name", "id", city_ids)}
        self.rates = [r for r in (sb.table("transport_hub_rates").select("*").eq("is_active", True).execute().data or [])]
        self.rates_by_city: dict[str, list] = {}
        for r in self.rates:
            self.rates_by_city.setdefault(r["destination_city_id"], []).append(r)
        t_ids = {k["transport_id"] for k in self.kaat.values() if k.get("transport_id")} \
            | {b["bilty_transport_id"] for b in bilties.values() if b.get("bilty_transport_id")} \
            | {r["transport_id"] for r in self.rates if r.get("transport_id")}
        cols = "id, transport_name, gst_number, city_id, city_name, is_prior"
        self.transports = {t["id"]: t for t in _in(sb, "transports", cols, "id", t_ids)}
        gsts = {b["bilty_transport_gst"] for b in bilties.values() if b.get("bilty_transport_gst")}
        self.by_gst: dict[str, list] = {}
        for t in _in(sb, "transports", cols, "gst_number", gsts):
            self.transports.setdefault(t["id"], t)
        for t in self.transports.values():
            g = _gst(t.get("gst_number"))
            if g:
                self.by_gst.setdefault(g, []).append(t)
        self.city_prior: dict[str, list] = {}
        for t in _in(sb, "transports", cols, "city_id", city_ids, extra=lambda q: q.eq("is_prior", True)):
            self.transports.setdefault(t["id"], t)
            self.city_prior.setdefault(t["city_id"], []).append(t)
        try:
            profiles = sb.table("transport_ledger_profiles").select("*").execute().data or []
        except Exception as exc:
            if "does not exist" not in str(exc):
                raise
            profiles = []                  # migration 026 not run yet — detection still works
        self.profiles = profiles
        self.ledger_names = {l["id"]: l["name"] for l in _in(sb, "ledgers", "id, name", "id", [p["ledger_id"] for p in profiles])}

    def settled_via(self, gr_no: str) -> str | None:
        """Already settled with the transport through the pohonch / crossing-challan system."""
        k = self.kaat.get(gr_no) or {}
        if k.get("pohonch_no"):
            return f"pohonch {k['pohonch_no']}"
        if k.get("crossing_challan_no"):
            return f"crossing challan {k['crossing_challan_no']}"
        return None

    def station(self, b: dict) -> str | None:
        c = self.cities.get(b.get("to_city_id"))
        return c["city_name"] if c else b.get("station_code")

    def profile_for_transport(self, transport_id: str | None) -> dict | None:
        if not transport_id:
            return None
        t = self.transports.get(transport_id)
        g = _gst(t.get("gst_number")) if t else None
        for p in self.profiles:
            if transport_id in (p.get("transport_ids") or []) or (g and p.get("transport_gstin") == g):
                return p
        return None

    def same_party(self, transport_id: str | None, profile: dict) -> bool:
        if not transport_id:
            return False
        t = self.transports.get(transport_id)
        g = _gst(t.get("gst_number")) if t else None
        return transport_id in (profile.get("transport_ids") or []) or bool(g and g == profile.get("transport_gstin"))

    def detect(self, b: dict) -> dict:
        """Suggested delivery transport for a bilty + where the suggestion came from."""
        def pick(tid, source):
            t = self.transports.get(tid) or {}
            p = self.profile_for_transport(tid)
            return {"transport_id": tid, "transport_name": t.get("transport_name"), "gstin": _gst(t.get("gst_number")),
                    "city": t.get("city_name"), "source": source,
                    "ledger_id": p["ledger_id"] if p else None,
                    "ledger_name": self.ledger_names.get(p["ledger_id"]) if p else None}

        k = self.kaat.get(b["gr_no"]) or {}
        if k.get("transport_id"):
            return pick(k["transport_id"], "kaat_table")
        if b.get("bilty_transport_id"):
            return pick(b["bilty_transport_id"], "bilty")
        gst = b.get("bilty_transport_gst")
        if gst and self.by_gst.get(gst):
            cands = self.by_gst[gst]
            same_city = [t for t in cands if t.get("city_id") == b.get("to_city_id")]
            t = (same_city or sorted(cands, key=lambda t: not t.get("is_prior")))[0]
            return pick(t["id"], "bilty_gst")
        rates = [r for r in self.rates_by_city.get(b.get("to_city_id"), []) if r.get("transport_id")]
        if rates:
            prior = [r for r in rates if (self.transports.get(r["transport_id"]) or {}).get("is_prior")]
            return pick((prior or rates)[0]["transport_id"], "station_rate" if len(rates) == 1 else "station_rate_prior")
        prior = self.city_prior.get(b.get("to_city_id")) or []
        if prior:
            return pick(prior[0]["id"], "city_prior")
        return {"transport_id": None, "source": None, "ledger_id": None,
                "reason": "no transport on the bilty, kaat record, station rate or station priority"}

    def kaat_for(self, b: dict, profile: dict | None, transport_id: str | None) -> dict:
        """kaat / dd / pf for posting this bilty to `profile`'s transport."""
        k = self.kaat.get(b["gr_no"]) or {}
        dd = round(float(k.get("dd_chrg") or 0), 2)
        kaat, rate, source = 0.0, None, "zero"
        same = (profile and self.same_party(k.get("transport_id"), profile)) or \
               (not profile and k.get("transport_id") == transport_id)
        if float(k.get("kaat") or 0) > 0 and (same or not k.get("transport_id")):
            kaat, rate, source = round(float(k["kaat"]), 2), k.get("actual_kaat_rate"), "kaat_table"
        else:
            ids = set(profile.get("transport_ids") or []) if profile else set()
            if transport_id:
                ids.add(transport_id)
            hub = [r for r in self.rates_by_city.get(b.get("to_city_id"), []) if r.get("transport_id") in ids]
            if hub:
                r = hub[0]
                if r.get("pricing_mode") == "per_pkg":
                    kaat, rate = float(b["packages"] or 0) * float(r.get("rate_per_pkg") or 0), r.get("rate_per_pkg")
                else:
                    kaat, rate = float(b["weight"] or 0) * float(r.get("rate_per_kg") or 0), r.get("rate_per_kg")
                kaat = round(max(kaat, float(r.get("min_charge") or 0)), 2)
                source = "hub_rate" if kaat > 0 else "zero"
        if b["payment_mode"] == "paid":
            pf = round(-kaat, 2)
        elif b["payment_mode"] == "foc":
            pf = round(-(kaat + dd), 2)
        else:
            pf = round(b["total"] - kaat - dd, 2)
        return {"kaat": kaat, "dd": dd, "pf": pf, "kaat_rate": rate, "kaat_source": source}


def _active_links(sb, grs) -> dict[str, dict]:
    try:
        rows = _in(sb, "transport_ledger_bilties", LINK_COLS, "gr_no", grs, extra=lambda q: q.eq("is_active", True))
    except Exception as exc:
        if "transport_ledger_bilties" in str(exc) and "does not exist" in str(exc):
            return {}
        raise
    links = {r["gr_no"]: r for r in rows}
    # a voucher cancelled from the Day Book frees its bilty again
    dead = {v["id"] for v in _in(sb, "vouchers", "id, is_active", "id", [l["voucher_id"] for l in links.values()])
            if not v["is_active"]}
    for gr, l in list(links.items()):
        if l.get("voucher_id") in dead:
            sb.table("transport_ledger_bilties").update({"is_active": False, "removed_at": _now(), "updated_at": _now()}) \
                .eq("id", l["id"]).execute()
            links.pop(gr)
    return links


def _party_ledger_holds(sb, bilties: list[dict]) -> dict[str, str]:
    """to-pay bilties already owed by a CONSIGNEE in a party ledger — posting them here too
    would count the same freight twice. {gr_no: ledger name}."""
    topay = [b for b in bilties if b["payment_mode"] == "to-pay" and b.get("bilty_id")]
    if not topay:
        return {}
    try:
        rows = _in(sb, "ledger_bilties", "gr_no, bilty_id, ledger_id, voucher_id", "bilty_id",
                   [b["bilty_id"] for b in topay], extra=lambda q: q.eq("is_active", True))
    except Exception:
        return {}
    rows = [r for r in rows if r.get("voucher_id")]
    names = {l["id"]: l["name"] for l in _in(sb, "ledgers", "id, name", "id", [r["ledger_id"] for r in rows])}
    return {r["gr_no"]: names.get(r["ledger_id"], "a party ledger") for r in rows}


def _delivery_holds(sb, grs: list[str]) -> set[str]:
    """GRs already booked as Kanpur Delivery income (quick_entry record_delivery_income:
    an active voucher, reference_no = GR, with a line on a 'Delivery Income' ledger)."""
    if not grs:
        return set()
    inc = [l["id"] for l in (sb.table("ledgers").select("id").ilike("name", "Delivery Income").execute().data or [])]
    if not inc:
        return set()
    vs = _in(sb, "vouchers", "id, reference_no", "reference_no", grs, extra=lambda q: q.eq("is_active", True))
    if not vs:
        return set()
    hit = {e["voucher_id"] for e in _in(sb, "voucher_entries", "voucher_id, ledger_id", "voucher_id", [v["id"] for v in vs])
           if e["ledger_id"] in inc}
    return {v["reference_no"] for v in vs if v["id"] in hit}


# ── Received challans + their bilties ────────────────────────────────────────
def list_received_challans(from_date=None, to_date=None, branch_id=None, page: int = 1, page_size: int = 30) -> dict:
    """Challans received at the hub, newest first, with how many bilties are in a transport ledger."""
    sb = get_supabase()
    challans = _received_challans(sb, from_date, to_date, branch_id)
    total = len(challans)
    page, page_size = max(1, page), min(max(1, page_size), 100)
    challans = challans[(page - 1) * page_size: page * page_size]
    transit = _in(sb, "transit_details", "gr_no, bilty_id, challan_no", "challan_no", [c["challan_no"] for c in challans])
    links = _active_links(sb, [t["gr_no"] for t in transit])
    branches = {b["id"]: b["branch_name"] for b in sb.table("branches").select("id, branch_name").execute().data or []}
    by_ch: dict[str, list] = {}
    for t in transit:
        by_ch.setdefault(t["challan_no"], []).append(t["gr_no"])
    rows = []
    for c in challans:
        grs = by_ch.get(c["challan_no"], [])
        assigned = [g for g in grs if g in links]
        rows.append({**c, "branch_name": branches.get(c["branch_id"]), "bilty_count": len(grs),
                     "assigned_count": len(assigned), "unassigned_count": len(grs) - len(assigned)})
    return {"status": "success", "data": {"rows": rows, "total": total, "page": page, "page_size": page_size}}


def _rows_for(sb, transit: list[dict]) -> list[dict]:
    bilties = _load_bilties(sb, transit)
    ctx = _Ctx(sb, bilties)
    links = _active_links(sb, list(bilties))
    holds = _party_ledger_holds(sb, list(bilties.values()))
    delivery = _delivery_holds(sb, list(bilties))
    ch_by_gr = {t["gr_no"]: t["challan_no"] for t in transit}
    out = []
    for gr, b in bilties.items():
        sug = ctx.detect(b)
        prof = next((p for p in ctx.profiles if p["ledger_id"] == sug.get("ledger_id")), None)
        calc = ctx.kaat_for(b, prof, sug.get("transport_id"))
        link = links.get(gr)
        row = {k: b[k] for k in ("gr_no", "bilty_type", "bilty_id", "bilty_date", "consignor", "consignee",
                                 "payment_mode", "packages", "weight", "total")}
        row.update({
            "challan_no": ch_by_gr.get(gr), "station": ctx.station(b),
            "to_pay_amount": b["total"] if b["payment_mode"] == "to-pay" else 0.0,
            "paid_amount": b["total"] if b["payment_mode"] == "paid" else 0.0,
            "suggested": sug, "estimate": calc,
            "assigned": ({"link_id": link["id"], "ledger_id": link["ledger_id"],
                          "ledger_name": ctx.ledger_names.get(link["ledger_id"]), "kaat": link["kaat"],
                          "pf": link["pf"]} if link else None),
            "blocked": (f"to-pay bilty already owed by the consignee in '{holds[gr]}'" if gr in holds
                        else "already booked as Kanpur Delivery income" if gr in delivery else None),
            "already_settled_via": ctx.settled_via(gr),
        })
        out.append(row)
    out.sort(key=lambda r: (r.get("challan_no") or "", r["gr_no"]))
    return out


def get_challan_bilties(challan_no: str) -> dict:
    """Every bilty on one received challan: GR, date, station, parties, to-pay / paid amount,
    weight, kaat, pf, suggested transport (+ source) and the ledger it's already in."""
    sb = get_supabase()
    ch = sb.table("challan_details").select("challan_no, date, branch_id, is_received_at_hub, received_at_hub_timing") \
        .eq("challan_no", challan_no).execute().data
    if not ch:
        return _err("Challan not found", 404)
    transit = sb.table("transit_details").select("gr_no, bilty_id, challan_no").eq("challan_no", challan_no).execute().data or []
    rows = _rows_for(sb, transit)
    return {"status": "success", "data": {
        "challan": ch[0], "bilties": rows,
        "totals": {
            "bilties": len(rows), "assigned": sum(1 for r in rows if r["assigned"]),
            "to_pay_total": round(sum(r["to_pay_amount"] for r in rows), 2),
            "paid_total": round(sum(r["paid_amount"] for r in rows), 2),
        },
    }}


def list_unassigned(from_date=None, to_date=None, branch_id=None, only_undetected: bool = False,
                    include_settled: bool = False) -> dict:
    """Bilties on received challans that are in NO transport ledger, each with its suggestion
    (or the reason none was found). only_undetected=true -> just the ones with no transport at all.
    Pohonch / crossing-challan settled bilties are left out unless include_settled=true.
    Defaults to the last 30 days (by challan date) — the full history is ~18k bilties."""
    sb = get_supabase()
    if not from_date and not to_date:
        from_date = (date.today() - timedelta(days=30)).isoformat()
    challans = _received_challans(sb, from_date, to_date, branch_id)
    transit = _in(sb, "transit_details", "gr_no, bilty_id, challan_no", "challan_no", [c["challan_no"] for c in challans])
    links = _active_links(sb, [t["gr_no"] for t in transit])
    transit = [t for t in transit if t["gr_no"] not in links]
    rows = _rows_for(sb, transit)
    settled_count = sum(1 for r in rows if r["already_settled_via"])
    if not include_settled:
        rows = [r for r in rows if not r["already_settled_via"]]
    if only_undetected:
        rows = [r for r in rows if not r["suggested"].get("transport_id")]
    return {"status": "success", "data": {
        "rows": rows, "count": len(rows), "from_date": from_date, "to_date": to_date,
        "undetected_count": sum(1 for r in rows if not r["suggested"].get("transport_id")),
        "settled_elsewhere_count": settled_count,
    }}


# ── Transport ledgers ────────────────────────────────────────────────────────
def create_transport_ledger(data: dict) -> dict:
    """
    data = { transport_id, branch_id (the hub branch whose books it lives in), created_by,
             ledger_id? (attach an EXISTING Transporters ledger instead of creating one), name?,
             allow_zero_kaat? (true for our own delivery transport, e.g. SS TRANSPORT in Kanpur) }
    Every transports row with the same GSTIN joins this ledger.
    """
    sb = get_supabase()
    tid, branch_id, created_by = data.get("transport_id"), data.get("branch_id"), data.get("created_by")
    if not tid or not created_by or (not branch_id and not data.get("ledger_id")):
        return _err("transport_id, branch_id and created_by are required")
    t = sb.table("transports").select("id, transport_name, gst_number, city_name, mob_number, address").eq("id", tid).execute().data
    if not t:
        return _err("Transport not found", 404)
    t = t[0]
    gst = _gst(t.get("gst_number"))
    group = [tid]
    if gst:
        group = list({r["id"] for r in (sb.table("transports").select("id").eq("gst_number", t["gst_number"]).execute().data or [])} | {tid})
    try:
        existing = sb.table("transport_ledger_profiles").select("ledger_id, transport_ids, transport_gstin").execute().data or []
    except Exception as exc:
        if "does not exist" in str(exc):
            return _err("Transport ledgers need migrations/026_transport_bilty_ledgers.sql — run it first", 500)
        raise
    for p in existing:
        if (gst and p.get("transport_gstin") == gst) or tid in (p.get("transport_ids") or []):
            return _err("This transport already has a ledger", 409, ledger_id=p["ledger_id"])

    if data.get("ledger_id"):
        led = sb.table("ledgers").select("id, name, group_id").eq("id", data["ledger_id"]).execute().data
        if not led:
            return _err("ledger_id not found", 404)
        if led[0]["group_id"] != _transporters_group_id():
            return _err("ledger_id must be a ledger under the Transporters group")
        ledger_id = led[0]["id"]
    else:
        name = (data.get("name") or t["transport_name"]).strip()
        clash = [l for l in (sb.table("ledgers").select("id, name").eq("branch_id", branch_id)
                             .eq("group_id", _transporters_group_id()).eq("is_active", True).execute().data or [])
                 if l["name"].strip().lower() == name.lower()]
        if clash:
            n_bills = len(sb.table("ledger_bill_references").select("id").eq("ledger_id", clash[0]["id"]).execute().data or [])
            return _err(f"A Transporters ledger '{clash[0]['name']}' already exists in this branch ({n_bills} bill(s) on it). "
                        f"Send ledger_id to attach it — careful: its manual PF bills and per-bilty entries would then "
                        f"both count — or send a different name.", 409,
                        existing_ledger_id=clash[0]["id"], existing_bill_count=n_bills)
        created = create_transporter({"branch_id": branch_id, "name": name, "gstin": gst, "phone": t.get("mob_number"),
                                      "address": t.get("address"), "created_by": created_by})
        if created["status"] != "success":
            return created
        ledger_id = created["data"]["id"]

    sb.table("transport_ledger_profiles").insert({
        "ledger_id": ledger_id, "transport_gstin": gst, "transport_ids": group, "primary_transport_id": tid,
        "transport_name": t["transport_name"], "allow_zero_kaat": bool(data.get("allow_zero_kaat")),
        "created_by": created_by, "updated_by": created_by,
    }).execute()
    return get_transport_ledger(ledger_id)


def update_transport_ledger(ledger_id: str, data: dict) -> dict:
    """data = { add_transport_ids?: [...], remove_transport_ids?: [...], allow_zero_kaat?, updated_by }
    — group transports that have no GSTIN (or a different one) into this ledger."""
    sb = get_supabase()
    p = sb.table("transport_ledger_profiles").select("*").eq("ledger_id", ledger_id).execute().data
    if not p:
        return _err("Transport ledger not found", 404)
    ids = set(p[0].get("transport_ids") or [])
    add = set(data.get("add_transport_ids") or [])
    others = [x for x in (sb.table("transport_ledger_profiles").select("ledger_id, transport_ids").execute().data or [])
              if x["ledger_id"] != ledger_id]
    taken = {t for x in others for t in (x.get("transport_ids") or [])} & add
    if taken:
        return _err("some of these transports already belong to another transport ledger")
    ids = (ids | add) - set(data.get("remove_transport_ids") or [])
    upd = {"transport_ids": list(ids), "updated_by": data.get("updated_by"), "updated_at": _now()}
    if "allow_zero_kaat" in data:
        upd["allow_zero_kaat"] = bool(data["allow_zero_kaat"])
    sb.table("transport_ledger_profiles").update(upd).eq("ledger_id", ledger_id).execute()
    return get_transport_ledger(ledger_id)


def _ledger_stats(sb, ledger_id: str) -> dict:
    rows = _paged(lambda: sb.table("transport_ledger_bilties").select("payment_mode, total, kaat, pf")
                  .eq("ledger_id", ledger_id).eq("is_active", True))
    return {
        "bilty_count": len(rows),
        "to_pay_total": round(sum(float(r["total"] or 0) for r in rows if r["payment_mode"] == "to-pay"), 2),
        "kaat_total": round(sum(float(r["kaat"] or 0) for r in rows), 2),
        "pf_total": round(sum(float(r["pf"] or 0) for r in rows), 2),
    }


def list_transport_ledgers(branch_id: str | None = None) -> dict:
    sb = get_supabase()
    profiles = sb.table("transport_ledger_profiles").select("*").execute().data or []
    leds = {l["id"]: l for l in _in(sb, "ledgers", "id, name, branch_id, is_active", "id", [p["ledger_id"] for p in profiles])}
    out = []
    for p in profiles:
        l = leds.get(p["ledger_id"])
        if not l or not l["is_active"] or (branch_id and l["branch_id"] != branch_id):
            continue
        bal = get_ledger_balance(l["id"])["data"]
        out.append({**p, "name": l["name"], "branch_id": l["branch_id"], "balance": bal["balance"],
                    "balance_type": bal["balance_type"], **_ledger_stats(sb, l["id"])})
    out.sort(key=lambda r: r["name"])
    return {"status": "success", "data": out}


def get_transport_ledger(ledger_id: str) -> dict:
    sb = get_supabase()
    p = sb.table("transport_ledger_profiles").select("*").eq("ledger_id", ledger_id).execute().data
    if not p:
        return _err("Transport ledger not found", 404)
    led = get_ledger(ledger_id)
    if led["status"] != "success":
        return led
    members = _in(sb, "transports", "id, transport_name, city_name, gst_number, is_prior", "id", p[0].get("transport_ids") or [])
    statement = get_ledger_statement(ledger_id)["data"]["entries"]
    return {"status": "success", "data": {
        "ledger": led["data"], "profile": p[0], "transports": members,
        "balance": get_ledger_balance(ledger_id)["data"],
        "summary": _ledger_stats(sb, ledger_id),
        "statement": statement[-200:], "statement_total_rows": len(statement),
    }}


def list_transport_ledger_bilties(ledger_id: str, challan_no=None, payment_mode=None, status=None,
                                  q=None, page: int = 1, page_size: int = 50) -> dict:
    """status: open (pf not settled) | settled"""
    sb = get_supabase()

    def base():
        x = sb.table("transport_ledger_bilties").select(LINK_COLS).eq("ledger_id", ledger_id).eq("is_active", True)
        if challan_no:
            x = x.eq("challan_no", challan_no)
        if payment_mode:
            x = x.eq("payment_mode", payment_mode)
        if q:
            term = "".join(ch for ch in q if ch not in ",()%*\\").strip()
            if term:
                x = x.or_(f"gr_no.ilike.%{term}%,consignor.ilike.%{term}%,consignee.ilike.%{term}%,station.ilike.%{term}%")
        return x.order("bilty_date", desc=True)

    rows = _paged(base)
    bills = {b["id"]: b for b in _in(sb, "ledger_bill_references", "id, bill_amount, balance_amount, is_settled",
                                    "id", [r["bill_reference_id"] for r in rows])}
    for r in rows:
        b = bills.get(r.get("bill_reference_id"))
        r["outstanding"] = float(b["balance_amount"]) if b else 0.0
        r["is_settled"] = bool(b["is_settled"]) if b else True
    if status in ("open", "settled"):
        rows = [r for r in rows if r["is_settled"] == (status == "settled")]
    page, page_size = max(1, page), min(max(1, page_size), 200)
    return {"status": "success", "data": {"rows": rows[(page - 1) * page_size: page * page_size], "total": len(rows),
                                          "page": page, "page_size": page_size}}


# ── Posting ──────────────────────────────────────────────────────────────────
def _lines(b: dict, calc: dict, transport_ledger: str, freight_id: str, kaat_id: str) -> list[dict]:
    """Signed lines (Dr +, Cr -) for one bilty; they always sum to 0."""
    freight = -b["total"] if b["payment_mode"] == "to-pay" else 0.0
    transport = calc["pf"]
    kaat_exp = round(-(freight + transport), 2)
    out = []
    for ledger_id, signed, is_party in ((freight_id, freight, False), (kaat_id, kaat_exp, False),
                                        (transport_ledger, transport, True)):
        signed = round(signed, 2)
        if signed:
            out.append({"ledger_id": ledger_id, "entry_type": "dr" if signed > 0 else "cr",
                        "amount": abs(signed), "is_party": is_party})
    return out


def _post(sb, ledger: dict, items: list[tuple[dict, dict, str | None]], added_via: str, user_id: str) -> dict:
    """items: (bilty, calc, challan_no). Vouchers / bills / entries / links in batched inserts."""
    freight = get_or_create_named_ledger(ledger["branch_id"], FREIGHT_INCOME_LEDGER_NAME, DIRECT_INCOMES_GROUP_NAME, user_id)
    kaat = get_or_create_named_ledger(ledger["branch_id"], KAAT_EXPENSE_LEDGER_NAME, DIRECT_EXPENSES_GROUP_NAME, user_id)
    for r in (freight, kaat):
        if r["status"] != "success":
            return r
    with_source = source_supported(sb)
    added = []
    for chunk in _chunks(items, POST_CHUNK):
        v_ids, bill_ids = [], []
        try:
            postable = []
            for b, calc, ch in chunk:
                lines = _lines(b, calc, ledger["id"], freight["data"]["id"], kaat["data"]["id"])
                if len(lines) >= 2:
                    postable.append((b, calc, ch, lines))
            v_by_gr, bill_by_gr = {}, {}
            if postable:
                for attempt in range(2):
                    seq = last_voucher_seq(sb, ledger["branch_id"], "journal")
                    rows = []
                    for i, (b, calc, ch, lines) in enumerate(postable, start=1):
                        row = {
                            "branch_id": ledger["branch_id"], "voucher_type": "journal",
                            "voucher_no": f"{VOUCHER_PREFIX['journal']}{str(seq + i).zfill(4)}",
                            "voucher_date": b.get("bilty_date") or today_ist(),
                            "narration": f"Bilty {b['gr_no']} ({b['payment_mode']}) - challan {ch or '-'} - kaat {calc['kaat']:.2f}",
                            "reference_no": b["gr_no"],
                            "total_amount": round(sum(l["amount"] for l in lines if l["entry_type"] == "dr"), 2),
                            "created_by": user_id,
                        }
                        if with_source:
                            row["source"] = {"action": "transport_bilty", "gr_no": b["gr_no"], "challan_no": ch}
                        rows.append(row)
                    try:
                        ins = sb.table("vouchers").insert(rows).execute().data or []
                        break
                    except Exception as exc:
                        if attempt == 0 and "vouchers_branch_type_no_unique" in str(exc):
                            continue
                        raise
                no_to_gr = {r["voucher_no"]: p[0]["gr_no"] for r, p in zip(rows, postable)}
                for v in ins:
                    v_by_gr[no_to_gr[v["voucher_no"]]] = v["id"]
                    v_ids.append(v["id"])
                bill_rows = []
                for b, calc, ch, lines in postable:
                    party = next(l for l in lines if l["is_party"]) if any(l["is_party"] for l in lines) else None
                    if party:
                        bill_rows.append({
                            "ledger_id": ledger["id"], "reference_no": b["gr_no"],
                            "reference_date": b.get("bilty_date") or today_ist(),
                            "bill_amount": party["amount"], "balance_amount": party["amount"],
                            "entry_type": party["entry_type"],
                            "source_table": "bilty" if b["bilty_type"] == "regular" else "station_bilty_summary",
                            "source_id": b["bilty_id"],
                            "metadata": {"gr_no": b["gr_no"], "challan_no": ch, "payment_mode": b["payment_mode"],
                                         "total": b["total"], "kaat": calc["kaat"], "dd": calc["dd"], "pf": calc["pf"]},
                            "created_by": user_id,
                        })
                if bill_rows:
                    for bl in sb.table("ledger_bill_references").insert(bill_rows).execute().data or []:
                        bill_by_gr[bl["reference_no"]] = bl["id"]
                        bill_ids.append(bl["id"])
                entries = []
                for b, calc, ch, lines in postable:
                    for l in lines:
                        e = {"voucher_id": v_by_gr[b["gr_no"]], "ledger_id": l["ledger_id"], "entry_type": l["entry_type"],
                             "amount": l["amount"]}
                        if l["is_party"]:
                            e.update({"bill_reference_id": bill_by_gr.get(b["gr_no"]), "bill_allocation_type": "new_ref"})
                        entries.append(e)
                sb.table("voucher_entries").insert(entries).execute()
            links = [{
                "ledger_id": ledger["id"], "gr_no": b["gr_no"], "bilty_type": b["bilty_type"], "bilty_id": b["bilty_id"],
                "challan_no": ch, "bilty_date": b.get("bilty_date"), "station": b.get("station"),
                "consignor": b.get("consignor"), "consignee": b.get("consignee"), "payment_mode": b["payment_mode"],
                "packages": b.get("packages"), "weight": b.get("weight"), "total": b["total"],
                "kaat_rate": calc.get("kaat_rate"), "kaat": calc["kaat"], "dd": calc["dd"], "pf": calc["pf"],
                "kaat_source": calc["kaat_source"], "transport_id": b.get("_transport_id"),
                "voucher_id": v_by_gr.get(b["gr_no"]), "bill_reference_id": bill_by_gr.get(b["gr_no"]),
                "added_via": added_via, "added_by": user_id,
            } for b, calc, ch in chunk]
            sb.table("transport_ledger_bilties").insert(links).execute()
        except Exception as exc:
            for part in _chunks(v_ids):
                sb.table("vouchers").delete().in_("id", part).execute()
            for part in _chunks(bill_ids):
                sb.table("ledger_bill_references").delete().in_("id", part).execute()
            msg = str(exc)
            if "uq_transport_ledger_bilties_active" in msg:
                msg = "one of these bilties was just assigned by someone else — refresh and try again"
            return _err(f"Could not assign bilties: {msg}", assigned=added)
        added += [b["gr_no"] for b, _, _ in chunk]
    return {"status": "success", "assigned": added}


def assign_bilties(data: dict) -> dict:
    """
    Move bilties into transport ledgers.
    data = { items: [{gr_no, ledger_id? | transport_id?}]   (neither = use the auto-detected transport),
             allow_zero_kaat? (default false), create_missing_ledgers? (default true),
             branch_id? (hub branch for new ledgers; default the challan's destination branch),
             updated_by }
    """
    sb = get_supabase()
    user_id = data.get("updated_by")
    items = [i for i in (data.get("items") or []) if i.get("gr_no")]
    if not items or not user_id:
        return _err("items: [{gr_no, ledger_id?|transport_id?}] and updated_by are required")
    allow_zero = bool(data.get("allow_zero_kaat"))
    include_settled = bool(data.get("include_settled"))
    create_missing = data.get("create_missing_ledgers", True)
    transit = _in(sb, "transit_details", "gr_no, bilty_id, challan_no, to_branch_id", "gr_no", [i["gr_no"] for i in items])
    t_by_gr = {t["gr_no"]: t for t in transit}
    received = {c["challan_no"] for c in _in(sb, "challan_details", "challan_no, is_received_at_hub", "challan_no",
                                              [t["challan_no"] for t in transit]) if c["is_received_at_hub"]}
    bilties = _load_bilties(sb, transit)
    ctx = _Ctx(sb, bilties)
    links = _active_links(sb, list(bilties))
    holds = _party_ledger_holds(sb, list(bilties.values()))
    delivery = _delivery_holds(sb, list(bilties))

    plan: dict[str, list] = {}
    differs = []
    skipped = []
    new_ledgers = {}
    for it in items:
        gr = it["gr_no"]
        t, b = t_by_gr.get(gr), bilties.get(gr)
        if not t or not b:
            skipped.append({"gr_no": gr, "reason": "not on any challan"})
            continue
        if t["challan_no"] not in received:
            skipped.append({"gr_no": gr, "reason": f"challan {t['challan_no']} isn't received at the hub yet"})
            continue
        if b.get("is_deleted"):
            skipped.append({"gr_no": gr, "reason": "bilty is deleted"})
            continue
        if gr in links:
            skipped.append({"gr_no": gr, "reason": f"already in '{ctx.ledger_names.get(links[gr]['ledger_id'])}'"})
            continue
        if gr in holds:
            skipped.append({"gr_no": gr, "reason": f"to-pay bilty already owed by the consignee in '{holds[gr]}'"})
            continue
        if gr in delivery:
            skipped.append({"gr_no": gr, "reason": "already booked as Kanpur Delivery income"})
            continue
        via = ctx.settled_via(gr)
        if via and not include_settled:
            skipped.append({"gr_no": gr, "reason": f"already settled with the transport via {via} — "
                                                   f"send include_settled to post it anyway"})
            continue
        profile, tid = None, it.get("transport_id")
        if it.get("ledger_id"):
            profile = next((p for p in ctx.profiles if p["ledger_id"] == it["ledger_id"]), None)
            if not profile:
                skipped.append({"gr_no": gr, "reason": "ledger_id is not a transport ledger"})
                continue
        else:
            if not tid:
                tid = ctx.detect(b).get("transport_id")
            if not tid:
                skipped.append({"gr_no": gr, "reason": "no transport found — pick one"})
                continue
            profile = ctx.profile_for_transport(tid) or new_ledgers.get(tid)
            if not profile:
                if not create_missing:
                    skipped.append({"gr_no": gr, "reason": "this transport has no ledger yet"})
                    continue
                made = create_transport_ledger({"transport_id": tid, "created_by": user_id,
                                                "branch_id": data.get("branch_id") or t.get("to_branch_id")})
                if made["status"] == "success":
                    profile = made["data"]["profile"]
                    ctx.ledger_names[profile["ledger_id"]] = made["data"]["ledger"]["name"]
                elif made.get("ledger_id"):          # created a moment ago by another request
                    found = sb.table("transport_ledger_profiles").select("*").eq("ledger_id", made["ledger_id"]).execute().data
                    profile = found[0] if found else None
                if not profile:
                    skipped.append({"gr_no": gr, "reason": made["message"]})
                    continue
                ctx.profiles.append(profile)
                for x in profile.get("transport_ids") or []:
                    new_ledgers[x] = profile
        calc = ctx.kaat_for(b, profile, tid)
        if calc["kaat"] <= 0 and not (allow_zero or profile.get("allow_zero_kaat")):
            skipped.append({"gr_no": gr, "reason": "no kaat for this transport at this station — add a station rate, "
                                                   "or send allow_zero_kaat", "estimate": calc})
            continue
        rec_tid = (ctx.kaat.get(gr) or {}).get("transport_id")
        if rec_tid and not ctx.same_party(rec_tid, profile):
            differs.append({"gr_no": gr, "kaat_record_transport": (ctx.transports.get(rec_tid) or {}).get("transport_name"),
                            "posted_to": ctx.ledger_names.get(profile["ledger_id"])})
        b["station"] = ctx.station(b)
        b["_transport_id"] = tid or (ctx.kaat.get(gr) or {}).get("transport_id")
        plan.setdefault(profile["ledger_id"], []).append((b, calc, t["challan_no"]))

    assigned = []
    for ledger_id, its in plan.items():
        led = sb.table("ledgers").select("id, name, branch_id").eq("id", ledger_id).execute().data
        if not led:
            skipped += [{"gr_no": b["gr_no"], "reason": "ledger not found"} for b, _, _ in its]
            continue
        res = _post(sb, led[0], its, data.get("_via") or "manual", user_id)
        if res["status"] != "success":
            return {**res, "assigned": assigned, "skipped": skipped}
        assigned += [{"gr_no": g, "ledger_id": ledger_id, "ledger_name": led[0]["name"]} for g in res["assigned"]]
    return {"status": "success", "message": f"{len(assigned)} bilty(s) assigned, {len(skipped)} skipped",
            "assigned": assigned, "skipped": skipped,
            # posted to a different transport than the kaat / pohonch record names — that record is NOT changed
            "kaat_record_transport_differs": differs}


def auto_assign_challan(challan_no: str, data: dict) -> dict:
    """Assign every not-yet-assigned bilty on a received challan to its auto-detected transport.
    data = { dry_run? (default true), allow_zero_kaat?, branch_id?, updated_by }"""
    sb = get_supabase()
    res = get_challan_bilties(challan_no)
    if res["status"] != "success":
        return res
    rows = [r for r in res["data"]["bilties"] if not r["assigned"]]
    zero_ok = set()
    try:
        zero_ok = {p["ledger_id"] for p in sb.table("transport_ledger_profiles").select("ledger_id, allow_zero_kaat")
                   .eq("allow_zero_kaat", True).execute().data or []}
    except Exception:
        pass
    plan = [{"gr_no": r["gr_no"], "transport_id": r["suggested"].get("transport_id"),
             "transport_name": r["suggested"].get("transport_name"), "source": r["suggested"].get("source"),
             "ledger_name": r["suggested"].get("ledger_name"), "kaat": r["estimate"]["kaat"], "pf": r["estimate"]["pf"],
             "blocked": r["blocked"] or (None if data.get("include_settled") or not r["already_settled_via"]
                                         else f"already settled via {r['already_settled_via']}"),
             "zero_ok": bool(data.get("allow_zero_kaat")) or r["suggested"].get("ledger_id") in zero_ok} for r in rows]
    if data.get("dry_run", True):
        return {"status": "success", "dry_run": True, "data": {
            "will_assign": [p for p in plan if p["transport_id"] and not p["blocked"]
                            and (p["kaat"] > 0 or p["zero_ok"])],
            "left_unassigned": [p for p in plan if not p["transport_id"] or p["blocked"]
                                or (p["kaat"] <= 0 and not p["zero_ok"])],
        }}
    todo = [{"gr_no": p["gr_no"], "transport_id": p["transport_id"]} for p in plan if p["transport_id"] and not p["blocked"]]
    if not todo:
        return {"status": "success", "message": "Nothing to assign", "assigned": [], "skipped": plan}
    return assign_bilties({**data, "items": todo, "_via": "auto"})


def unassign_bilty(ledger_id: str, link_id: str, user_id: str | None = None, reason: str | None = None) -> dict:
    sb = get_supabase()
    l = sb.table("transport_ledger_bilties").select(LINK_COLS).eq("id", link_id).eq("ledger_id", ledger_id).execute().data
    if not l or not l[0]["is_active"]:
        return _err("Bilty not found in this ledger", 404)
    link = l[0]
    if link.get("voucher_id"):
        v = sb.table("vouchers").select("is_active").eq("id", link["voucher_id"]).execute().data
        if v and v[0]["is_active"]:
            c = cancel_voucher(link["voucher_id"], user_id, reason or f"Bilty {link['gr_no']} removed from transport ledger")
            if c["status"] != "success":
                return c
    sb.table("transport_ledger_bilties").update({"is_active": False, "removed_by": user_id, "removed_at": _now(),
                                                 "updated_at": _now()}).eq("id", link_id).execute()
    return {"status": "success", "message": f"Bilty {link['gr_no']} is unassigned again"}


def refresh_transport_ledger(ledger_id: str, data: dict) -> dict:
    """Kaat edited on the kaat screens (or a bilty total changed) after assigning?
    data = { dry_run? (default true), updated_by } — lists / applies the differences."""
    sb = get_supabase()
    p = sb.table("transport_ledger_profiles").select("*").eq("ledger_id", ledger_id).execute().data
    if not p:
        return _err("Transport ledger not found", 404)
    links = _paged(lambda: sb.table("transport_ledger_bilties").select(LINK_COLS).eq("ledger_id", ledger_id).eq("is_active", True))
    transit = [{"gr_no": l["gr_no"], "bilty_id": l["bilty_id"] if l["bilty_type"] == "regular" else None,
                "challan_no": l["challan_no"]} for l in links]
    bilties = _load_bilties(sb, transit)
    ctx = _Ctx(sb, bilties)
    changes = []
    for l in links:
        b = bilties.get(l["gr_no"])
        if not b:
            continue
        calc = ctx.kaat_for(b, p[0], l.get("transport_id"))
        if calc["kaat_source"] == "zero" and float(l["kaat"] or 0) > 0:
            continue                       # never drop a posted kaat to 0 automatically
        if round(float(l["pf"] or 0), 2) != calc["pf"] or round(float(l["total"] or 0), 2) != b["total"]:
            changes.append({"link": l, "bilty": b, "calc": calc})
    report = [{"gr_no": c["link"]["gr_no"], "old_kaat": float(c["link"]["kaat"] or 0), "new_kaat": c["calc"]["kaat"],
               "old_pf": float(c["link"]["pf"] or 0), "new_pf": c["calc"]["pf"]} for c in changes]
    if data.get("dry_run", True):
        return {"status": "success", "dry_run": True, "data": {"changes": report}}

    led = sb.table("ledgers").select("id, branch_id").eq("id", ledger_id).execute().data[0]
    freight = get_or_create_named_ledger(led["branch_id"], FREIGHT_INCOME_LEDGER_NAME, DIRECT_INCOMES_GROUP_NAME, data.get("updated_by"))
    kaat = get_or_create_named_ledger(led["branch_id"], KAAT_EXPENSE_LEDGER_NAME, DIRECT_EXPENSES_GROUP_NAME, data.get("updated_by"))
    done, failed = [], []
    for c in changes:
        l, b, calc = c["link"], c["bilty"], c["calc"]
        if not l.get("voucher_id"):
            failed.append({"gr_no": l["gr_no"], "reason": "posted without a voucher — unassign and assign again"})
            continue
        old = sb.table("voucher_entries").select("id, ledger_id, entry_type, bill_reference_id, bill_allocation_type") \
            .eq("voucher_id", l["voucher_id"]).execute().data or []
        new_lines = _lines(b, calc, ledger_id, freight["data"]["id"], kaat["data"]["id"])
        entries = []
        for nl in new_lines:
            e = {"ledger_id": nl["ledger_id"], "entry_type": nl["entry_type"], "amount": nl["amount"]}
            if nl["is_party"]:
                e.update({"bill_allocation_type": "new_ref", "bill_reference_id": l.get("bill_reference_id")})
            entries.append(e)
        res = update_voucher(l["voucher_id"], {"entries": entries, "updated_by": data.get("updated_by"),
                                               "edit_reason": f"Kaat/pf refreshed for {l['gr_no']}"}, auto_match=True)
        if res["status"] != "success":
            failed.append({"gr_no": l["gr_no"], "reason": res["message"]})
            continue
        sb.table("transport_ledger_bilties").update({
            "total": b["total"], "kaat": calc["kaat"], "dd": calc["dd"], "pf": calc["pf"],
            "kaat_rate": calc["kaat_rate"], "kaat_source": calc["kaat_source"], "updated_at": _now(),
        }).eq("id", l["id"]).execute()
        done.append(l["gr_no"])
    return {"status": "success", "dry_run": False, "message": f"{len(done)} bilty(s) updated, {len(failed)} failed",
            "updated": done, "failed": failed}


def transport_options(station_city_id: str | None = None, q: str | None = None) -> dict:
    """Transports for the manual pick dropdown — the station's own first (priority first), each
    with its ledger if it has one."""
    sb = get_supabase()
    x = sb.table("transports").select("id, transport_name, gst_number, city_id, city_name, is_prior")
    if station_city_id:
        x = x.eq("city_id", station_city_id)
    if q:
        x = x.ilike("transport_name", f"%{q.strip()}%")
    rows = x.limit(100).execute().data or []
    try:
        profiles = sb.table("transport_ledger_profiles").select("ledger_id, transport_ids, transport_gstin").execute().data or []
    except Exception:
        profiles = []
    for r in rows:
        g = _gst(r.get("gst_number"))
        p = next((p for p in profiles if r["id"] in (p.get("transport_ids") or []) or (g and p.get("transport_gstin") == g)), None)
        r["ledger_id"] = p["ledger_id"] if p else None
    rows.sort(key=lambda r: (not r.get("is_prior"), r["transport_name"]))
    return {"status": "success", "data": rows}
