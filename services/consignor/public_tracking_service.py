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

Complaints: a customer can raise one on a bilty of THEIR GSTIN (checked server-side),
see its status and the office's public reply.
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
    try:
        for c in sb.table("bilty_complaints").select("gr_no, ticket_no, status, created_at") \
                .ilike("consignor_gst", gst).in_("gr_no", grs or ["-"]).order("created_at", desc=True).execute().data or []:
            complaints.setdefault(c["gr_no"], []).append(c)
    except Exception:
        pass                                            # migration 028 not run yet

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
            "complaints": {"count": len(cl), "open": sum(1 for c in cl if c["status"] in ("open", "in_progress")),
                           "latest": cl[0] if cl else None},
        })
    return {"status": "success", "data": {
        "consignor_gst": gst, "consignor_name": d.get("consignor_name"),
        "total": d["total"] if not gr_no else len(out), "page": d["page"], "page_size": d["page_size"],
        "has_more": d["has_more"] if not gr_no else False, "rows": out,
        "complaint_categories": [{"code": k, "label": v} for k, v in CATEGORIES.items()],
    }}


# ── Complaints (public) ───────────────────────────────────────────────────────
def _ticket_no(sb) -> str:
    prefix = f"CMP-{datetime.now(timezone.utc):%y%m}-"
    last = sb.table("bilty_complaints").select("ticket_no").like("ticket_no", f"{prefix}%") \
        .order("ticket_no", desc=True).limit(1).execute().data
    n = int(last[0]["ticket_no"].rsplit("-", 1)[1]) + 1 if last else 1
    return f"{prefix}{n:04d}"


def raise_complaint(data: dict) -> dict:
    """data = { consignor_gst, gr_no, category, message, name?, phone }"""
    gst = (data.get("consignor_gst") or "").strip().upper()
    gr = (data.get("gr_no") or "").strip().upper()
    cat = (data.get("category") or "").strip().lower()
    msg = (data.get("message") or "").strip()
    phone = re.sub(r"\D", "", str(data.get("phone") or ""))[-10:]
    name = (data.get("name") or "").strip()[:100] or None
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
    b = sb.table("bilty").select("id, gr_no").eq("gr_no", gr).ilike("consignor_gst", gst).eq("is_active", True) \
        .limit(1).execute().data
    if not b:
        return _err("This GR number isn't one of your bilties", 404)
    try:
        open_now = sb.table("bilty_complaints").select("id", count="exact").eq("gr_no", gr) \
            .in_("status", ["open", "in_progress"]).execute().count or 0
    except Exception as exc:
        if "does not exist" in str(exc):
            return _err("Complaints aren't enabled yet (migrations/028_bilty_complaints.sql)", 503)
        raise
    if open_now >= MAX_OPEN_PER_GR:
        return _err("This bilty already has open complaints — our team will contact you", 429)

    for attempt in range(3):
        row = {"ticket_no": _ticket_no(sb), "gr_no": gr, "bilty_id": b[0]["id"], "consignor_gst": gst,
               "category": cat, "message": msg, "raised_by_name": name, "raised_by_phone": phone}
        try:
            ins = sb.table("bilty_complaints").insert(row).execute().data[0]
            break
        except Exception as exc:
            if "bilty_complaints_ticket_unique" in str(exc) and attempt < 2:
                continue
            raise
    return {"status": "success", "message": f"Complaint registered. Your ticket number is {ins['ticket_no']}",
            "data": _public_complaint(ins)}


def _public_complaint(c: dict) -> dict:
    return {"ticket_no": c["ticket_no"], "gr_no": c["gr_no"], "category": c["category"],
            "category_label": CATEGORIES.get(c["category"]), "message": c["message"], "status": c["status"],
            "reply": c.get("public_reply"), "created_at": c["created_at"], "updated_at": c.get("updated_at"),
            "resolved_at": c.get("resolved_at")}


def list_public_complaints(consignor_gst: str, gr_no: str | None = None) -> dict:
    gst = (consignor_gst or "").strip().upper()
    if not _GSTIN.match(gst):
        return _err("A valid consignor GSTIN is required")
    q = get_supabase().table("bilty_complaints").select("*").ilike("consignor_gst", gst).order("created_at", desc=True)
    if gr_no:
        q = q.eq("gr_no", gr_no.strip().upper())
    try:
        rows = q.limit(100).execute().data or []
    except Exception as exc:
        if "does not exist" in str(exc):
            return {"status": "success", "data": []}
        raise
    return {"status": "success", "data": [_public_complaint(c) for c in rows]}


# ── Complaints (staff) ────────────────────────────────────────────────────────
def list_complaints(status: str | None = None, q: str | None = None, page: int = 1, page_size: int = 50) -> dict:
    sb = get_supabase()
    x = sb.table("bilty_complaints").select("*", count="exact").order("created_at", desc=True)
    if status:
        x = x.in_("status", [s.strip() for s in status.split(",")])
    if q:
        term = re.sub(r"[,()%*\\]", " ", q).strip()
        if term:
            x = x.or_(f"gr_no.ilike.%{term}%,ticket_no.ilike.%{term}%,consignor_gst.ilike.%{term}%,raised_by_phone.ilike.%{term}%")
    page, page_size = max(1, page), min(max(1, page_size), 200)
    res = x.range((page - 1) * page_size, page * page_size - 1).execute()
    return {"status": "success", "data": {"rows": res.data or [], "total": res.count or 0, "page": page, "page_size": page_size}}


def update_complaint(complaint_id: str, data: dict) -> dict:
    """data = { status?, public_reply?, internal_note?, assigned_to?, updated_by }"""
    sb = get_supabase()
    if not sb.table("bilty_complaints").select("id").eq("id", complaint_id).execute().data:
        return _err("Complaint not found", 404)
    payload = {}
    if "status" in data:
        if data["status"] not in ("open", "in_progress", "resolved", "closed"):
            return _err("status must be open, in_progress, resolved or closed")
        payload["status"] = data["status"]
        payload["resolved_at"] = _now() if data["status"] in ("resolved", "closed") else None
    for f in ("public_reply", "internal_note", "assigned_to"):
        if f in data:
            payload[f] = data[f] or None
    if not payload:
        return _err("Nothing to update")
    payload.update({"updated_by": data.get("updated_by"), "updated_at": _now()})
    sb.table("bilty_complaints").update(payload).eq("id", complaint_id).execute()
    return {"status": "success", "data": sb.table("bilty_complaints").select("*").eq("id", complaint_id).execute().data[0]}
