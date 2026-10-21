"""
Public Consignor Tracking (customer-facing)
============================================
What a consignor sees on /tracking/<their GSTIN> — only what helps them:
GR, dates, route, packages / weight, their freight amount, invoice details,
e-way bill validity, a plain-language tracking status + stepper, the vehicle,
the bilty PDF, and their complaints.

NEVER returned here (internal / commercial): kaat, pf, rates, dd / labour / other
charges, pohonch / crossing numbers, driver & owner names and mobiles, Part-B update
history / error messages, the delivery transport's GSTIN, staff notes.
The full internal view is consignor_bilty_service.get_consignor_bilties (the /360 route).

Complaints: a customer can raise one on a bilty of THEIR GSTIN (checked server-side); it goes
into the existing `complaints` table (the office's complaint screen) and the customer sees
its status and the office's resolution notes.
"""
import re
from datetime import datetime, timezone
from services.supabase_client import get_supabase
from services.consignor.consignor_bilty_service import get_consignor_bilties

CATEGORIES = {
    "delay": "Delivery delayed", "damage": "Goods damaged", "short_delivery": "Short / missing packages",
    "wrong_delivery": "Delivered to wrong party", "not_received": "Consignment not received",
    "billing": "Billing / freight issue", "behaviour": "Staff behaviour", "other": "Other",
}
MAX_OPEN_PER_GR = 3
_PHONE = re.compile(r"^[6-9]\d{9}$")
_GSTIN = re.compile(r"^[0-9]{2}[A-Z0-9]{13}$")


def _err(msg, code=400):
    return {"status": "error", "message": msg, "status_code": code}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _status(stages: dict | None, dispatch: dict) -> dict:
    """Plain-language status + step index (0-5) for the stepper."""
    steps = ["Booked", "Dispatched", "Reached hub", "Out for delivery", "Reached destination", "Out for door delivery"]
    s = stages or {}
    if s.get("out_for_door_delivery"):
        i = 5
    elif s.get("delivered_at_destination"):
        i = 4
    elif s.get("out_from_branch2"):
        i = 3
    elif s.get("delivered_at_branch2") or dispatch.get("is_received_at_hub"):
        i = 2
    elif s.get("out_from_branch1") or dispatch.get("is_dispatched"):
        i = 1
    else:
        i = 0
    return {"code": ["booked", "dispatched", "at_hub", "out_for_delivery", "at_destination", "door_delivery"][i],
            "label": steps[i], "step": i, "steps": steps}


def get_public_consignor_bilties(consignor_gst: str, from_date: str | None = None, to_date: str | None = None,
                                 page: int = 1, page_size: int = 20, gr_no: str | None = None) -> dict:
    gst = (consignor_gst or "").strip().upper()
    if not _GSTIN.match(gst):
        return _err("A valid consignor GSTIN is required")
    page, page_size = max(1, int(page or 1)), min(max(1, int(page_size or 20)), 50)
    full = get_consignor_bilties(gst, None, from_date, to_date, page, page_size)
    if full.get("status") != "success":
        return full
    d = full["data"]
    rows = d["rows"]
    if gr_no:
        rows = [r for r in rows if r["gr_no"].upper() == gr_no.strip().upper()]

    sb = get_supabase()
    grs = [r["gr_no"] for r in rows]
    extra = {}
    if grs:
        for b in sb.table("bilty").select("gr_no, pdf_bucket, invoice_no, invoice_value, invoice_date, delivery_type, "
                                           "transport_name, contain").in_("gr_no", grs).eq("is_active", True) \
                .ilike("consignor_gst", gst).execute().data or []:
            extra[b["gr_no"]] = b
    complaints = {}
    if grs:
        for c in sb.table("complaints").select("gr_no, complaint_no, status, created_at") \
                .in_("gr_no", grs).like("complaint_no", f"{CUST_PREFIX}%").order("created_at", desc=True).execute().data or []:
            complaints.setdefault(c["gr_no"], []).append({"ticket_no": c["complaint_no"], "status": c["status"],
                                                           "status_label": STATUS_LABELS.get(c["status"], c["status"]),
                                                           "created_at": c["created_at"]})

    out = []
    for r in rows:
        x = extra.get(r["gr_no"], {})
        disp = r.get("dispatch") or {}
        stages = disp.get("delivery_stages")
        cl = complaints.get(r["gr_no"], [])
        out.append({
            "gr_no": r["gr_no"],
            "bilty_date": r.get("bilty_date"),
            "consignor_name": r.get("consignor_name"),
            "consignee_name": r.get("consignee_name"),
            "from_city": {"city_name": (r.get("from_city") or {}).get("city_name")},
            "to_city": {"city_name": (r.get("to_city") or {}).get("city_name")},
            "payment_mode": r.get("payment_mode"),
            "delivery_type": x.get("delivery_type"),
            "no_of_pkg": r.get("no_of_pkg"),
            "weight": r.get("weight"),
            "contents": x.get("contain"),
            "pvt_marks": r.get("pvt_marks"),
            "freight_amount": r.get("total"),
            "invoice": {"invoice_no": x.get("invoice_no"), "invoice_value": x.get("invoice_value"),
                        "invoice_date": x.get("invoice_date")},
            "eway_bills": [{"ewb_number": e.get("ewb_number"), "valid_upto": e.get("valid_upto"),
                            "is_valid": e.get("is_valid")} for e in (r.get("eway_bills") or [])],
            "tracking": {
                **_status(stages, disp),
                "dispatch_date": disp.get("dispatch_date"),
                "vehicle_number": disp.get("truck_number"),
                "delivery_stages": stages,
            },
            "delivery_partner": x.get("transport_name"),     # name only — for the consignee to collect from
            "bilty_pdf_url": x.get("pdf_bucket"),
            "complaints": {"count": len(cl), "open": sum(1 for c in cl if c["status"] not in DONE_STATUSES),
                           "latest": cl[0] if cl else None},
        })
    return {"status": "success", "data": {
        "consignor_gst": gst, "consignor_name": d.get("consignor_name"),
        "total": d["total"] if not gr_no else len(out), "page": d["page"], "page_size": d["page_size"],
        "has_more": d["has_more"] if not gr_no else False, "rows": out,
        "complaint_categories": [{"code": k, "label": v} for k, v in CATEGORIES.items()],
    }}


# ── Complaints (existing `complaints` table — the office's complaint screen) ───
# Customer complaints are stored exactly like the office's own:
#   complaint_no   CUST-<GR>-<epoch ms>   (office ones are TRACK-<GR>-<epoch ms>)
#   complaint_type DELAY / DAMAGE / SHORT_DELIVERY / … (the category, upper-case)
#   status         INVESTIGATING (the office then moves it on as usual)
#   complaint_description "[Customer: <name>, <phone> · tracking page] <message>"
#   created_by     the user who booked the bilty (the table needs a user; the public has none)
# resolution_notes is what the customer sees as the office's reply.
CUST_PREFIX = "CUST-"
STATUS_LABELS = {"INVESTIGATING": "Under investigation", "IN_TRANSIT": "In transit", "DELIVERED": "Delivered",
                 "RESOLVED": "Resolved", "CLOSED": "Closed"}
DONE_STATUSES = {"RESOLVED", "CLOSED", "DELIVERED"}
URGENT = {"damage", "not_received", "short_delivery"}


def _public_complaint(c: dict) -> dict:
    desc = c.get("complaint_description") or ""
    msg = desc.split("] ", 1)[1] if desc.startswith("[Customer") and "] " in desc else desc
    code = (c.get("complaint_type") or "").lower()
    return {"ticket_no": c["complaint_no"], "gr_no": c["gr_no"], "category": code,
            "category_label": CATEGORIES.get(code, c.get("complaint_type")), "message": msg,
            "status": c["status"], "status_label": STATUS_LABELS.get(c["status"], c["status"]),
            "reply": c.get("resolution_notes"), "created_at": c["created_at"], "updated_at": c.get("updated_at"),
            "resolved_at": c.get("resolved_at") or c.get("closed_at")}


def raise_complaint(data: dict) -> dict:
    """data = { consignor_gst, gr_no, category, message, name?, phone }"""
    gst = (data.get("consignor_gst") or "").strip().upper()
    gr = (data.get("gr_no") or "").strip().upper()
    cat = (data.get("category") or "").strip().lower()
    msg = (data.get("message") or "").strip()
    phone = re.sub(r"\D", "", str(data.get("phone") or ""))[-10:]
    name = (data.get("name") or "").strip()[:100]
    if not _GSTIN.match(gst) or not gr:
        return _err("consignor_gst and gr_no are required")
    if cat not in CATEGORIES:
        return _err(f"category must be one of: {', '.join(CATEGORIES)}")
    if len(msg) < 5:
        return _err("Please describe the problem (at least 5 characters)")
    if len(msg) > 2000:
        return _err("Message is too long (max 2000 characters)")
    if not _PHONE.match(phone):
        return _err("A valid 10-digit mobile number is required so we can call you back")

    sb = get_supabase()
    b = sb.table("bilty").select("id, gr_no, staff_id, to_city_id").eq("gr_no", gr).ilike("consignor_gst", gst) \
        .eq("is_active", True).limit(1).execute().data
    if not b:
        return _err("This GR number isn't one of your bilties", 404)
    b = b[0]
    mine = sb.table("complaints").select("status").eq("gr_no", gr).like("complaint_no", f"{CUST_PREFIX}%").execute().data or []
    if sum(1 for c in mine if c["status"] not in DONE_STATUSES) >= MAX_OPEN_PER_GR:
        return _err("This bilty already has open complaints — our team will contact you", 429)

    # same delivery-progress fields the office's TRACK- complaints carry
    t = (sb.table("transit_details").select("challan_no, is_out_of_delivery_from_branch1, is_delivered_at_branch2, "
                                            "is_out_of_delivery_from_branch2, is_delivered_at_destination")
         .eq("gr_no", gr).limit(1).execute().data or [{}])[0]
    city = (sb.table("cities").select("city_name").eq("id", b["to_city_id"]).execute().data or [{}])[0] \
        if b.get("to_city_id") else {}
    who = ", ".join(x for x in (name, phone) if x)
    row = {
        "complaint_no": f"{CUST_PREFIX}{gr}-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
        "gr_no": gr, "bilty_id": b["id"], "complaint_type": cat.upper(),
        "complaint_description": f"[Customer: {who} · tracking page] {msg}",
        "priority": "URGENT" if cat in URGENT else "MEDIUM", "status": "INVESTIGATING",
        "destination_city": city.get("city_name"), "challan_no": t.get("challan_no"),
        "is_out_for_delivery": bool(t.get("is_out_of_delivery_from_branch1")),
        "is_at_hub": bool(t.get("is_delivered_at_branch2")),
        "is_out_for_delivery_from_hub": bool(t.get("is_out_of_delivery_from_branch2")),
        "is_delivered_at_destination": bool(t.get("is_delivered_at_destination")),
        "created_by": b["staff_id"],
    }
    ins = sb.table("complaints").insert(row).execute().data[0]
    return {"status": "success", "message": f"Complaint registered. Your ticket number is {ins['complaint_no']}",
            "data": _public_complaint(ins)}


def list_public_complaints(consignor_gst: str, gr_no: str | None = None) -> dict:
    """The consignor's own (customer-raised) complaints. Office-internal TRACK- rows aren't shown."""
    gst = (consignor_gst or "").strip().upper()
    if not _GSTIN.match(gst):
        return _err("A valid consignor GSTIN is required")
    sb = get_supabase()
    q = sb.table("complaints").select("*").like("complaint_no", f"{CUST_PREFIX}%").order("created_at", desc=True)
    if gr_no:
        q = q.eq("gr_no", gr_no.strip().upper())
    rows = q.limit(500).execute().data or []
    ids = list({r["bilty_id"] for r in rows if r.get("bilty_id")})
    own = set()
    for i in range(0, len(ids), 200):
        own |= {x["id"] for x in sb.table("bilty").select("id").in_("id", ids[i:i + 200]).ilike("consignor_gst", gst)
                .execute().data or []}
    return {"status": "success", "data": [_public_complaint(c) for c in rows if c.get("bilty_id") in own][:100]}
