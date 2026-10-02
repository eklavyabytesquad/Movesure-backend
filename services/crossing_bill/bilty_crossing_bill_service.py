"""
Bilty Crossing Bill Service
=============================
A bill built directly from bilties, for one transport, for one month —
for the case where there's no formal pohonch (crossing challan), only a
hand-written bilty number recorded on bilty_wise_kaat.bilty_number as
crossing proof, optionally backed by a transit-bilty photo URL
(bilty.bilty_image / station_bilty_summary.transit_bilty_image).

Deliberately separate from crossing_bill/pohonch (see
services/crossing_bill/crossing_bill_service.py) — that system always
requires real pohonch_numbers. This is the lighter, bilty-direct
alternative for proof that never became (and may never become) a real
pohonch. If you DO want it to go through the normal pohonch pipeline
instead, see services/crossing_bill/nil_bilty_service.py.
"""
from __future__ import annotations
from datetime import datetime, timezone
from services.supabase_client import get_supabase

BILL_COLS = (
    "id, bill_no, transport_name, transport_gstin, bill_month, bill_year, "
    "metadata, total_bilties, total_kaat, total_pf, total_amount, pdf_url, "
    "is_active, created_by, updated_by, created_at, updated_at"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _next_bill_no(sb, bill_year: int, bill_month: int) -> str:
    period = f"{bill_year}{bill_month:02d}"
    prefix = f"BCB-{period}-"
    res = (
        sb.table("bilty_crossing_bill")
        .select("bill_no")
        .ilike("bill_no", f"{prefix}%")
        .order("bill_no", desc=True)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return f"{prefix}0001"
    try:
        last_seq = int(rows[0]["bill_no"][len(prefix):])
    except (ValueError, IndexError):
        last_seq = 0
    return f"{prefix}{last_seq + 1:04d}"


def _resolve_gr_metadata(sb, gr_nos: list[str]) -> tuple[list[dict], list[str], list[str]]:
    """The core per-GR lookup shared by create_bilty_crossing_bill AND the
    single-GR search/preview endpoint — one GR or a thousand, same code
    path, so a preview can never show something different from what
    actually gets saved onto a bill.

    Returns (metadata_list, unmatched_gr_nos, no_kaat_data_gr_nos).
    """
    # ── Crossing proof + kaat/pf ─────────────────────────────────────────
    kaat_map = {}
    for chunk in _chunks(gr_nos, 200):
        res = (
            sb.table("bilty_wise_kaat")
            .select("gr_no, bilty_number, kaat, pf")
            .in_("gr_no", chunk)
            .execute()
        )
        for r in res.data or []:
            kaat_map[r["gr_no"]] = r

    # ── bilty table (primary) ────────────────────────────────────────────
    bilty_map = {}
    for chunk in _chunks(gr_nos, 200):
        res = (
            sb.table("bilty")
            .select("gr_no, bilty_image, total, consignor_name, consignee_name, to_city_id, bilty_date")
            .in_("gr_no", chunk)
            .eq("is_active", True)
            .execute()
        )
        for r in res.data or []:
            bilty_map[r["gr_no"]] = r

    # ── station_bilty_summary (fallback) ─────────────────────────────────
    missing = [g for g in gr_nos if g not in bilty_map]
    station_map = {}
    if missing:
        for chunk in _chunks(missing, 200):
            res = (
                sb.table("station_bilty_summary")
                .select("gr_no, transit_bilty_image, amount, consignor, consignee, city_id, created_at")
                .in_("gr_no", chunk)
                .execute()
            )
            for r in res.data or []:
                station_map[r["gr_no"]] = r

    # ── Destination city names (nice-to-have, cheap) ─────────────────────
    city_ids = {b["to_city_id"] for b in bilty_map.values() if b.get("to_city_id")}
    city_ids |= {s["city_id"] for s in station_map.values() if s.get("city_id")}
    city_name_map = {}
    if city_ids:
        for chunk in _chunks(list(city_ids), 200):
            for c in sb.table("cities").select("id, city_name").in_("id", chunk).execute().data or []:
                city_name_map[c["id"]] = c["city_name"]

    metadata = []
    unmatched_gr = []
    no_kaat_gr = []

    for gr in gr_nos:
        k = kaat_map.get(gr, {})
        if not k:
            no_kaat_gr.append(gr)

        b = bilty_map.get(gr)
        s = station_map.get(gr) if not b else None
        if not b and not s:
            unmatched_gr.append(gr)

        kaat = float(k.get("kaat") or 0)
        pf = float(k.get("pf") or 0)
        amount = float((b.get("total") if b else (s.get("amount") if s else 0)) or 0)
        city_id = (b.get("to_city_id") if b else (s.get("city_id") if s else None))

        metadata.append({
            "gr_no": gr,
            "bilty_number": k.get("bilty_number"),
            "crossing_proof_url": (b.get("bilty_image") if b else (s.get("transit_bilty_image") if s else None)),
            "kaat": round(kaat, 2),
            "pf": round(pf, 2),
            "amount": round(amount, 2),
            "consignor_name": (b.get("consignor_name") if b else (s.get("consignor") if s else None)),
            "consignee_name": (b.get("consignee_name") if b else (s.get("consignee") if s else None)),
            "destination": city_name_map.get(city_id, ""),
            "source_table": "bilty" if b else ("station_bilty_summary" if s else None),
        })

    return metadata, unmatched_gr, no_kaat_gr


def search_transporters(q: str, limit: int = 20) -> dict:
    """Autocomplete for the 'search a transporter' step — matches
    transport_name or gst_number, deduped down to one row per real
    transporter (the transports table has one row per branch/city a
    transporter has shipped through, so the same GSTIN repeats a lot)."""
    if not q or len(q.strip()) < 2:
        return {"status": "success", "data": []}
    q = q.strip()
    sb = get_supabase()
    rows = (
        sb.table("transports")
        .select("transport_name, gst_number")
        .or_(f"transport_name.ilike.%{q}%,gst_number.ilike.%{q}%")
        .limit(500)
        .execute()
        .data or []
    )
    seen = {}
    for r in rows:
        gstin = (r.get("gst_number") or "").strip().upper()
        key = gstin or r["transport_name"].strip().upper()
        if key not in seen:
            seen[key] = {"transport_name": r["transport_name"].strip(), "transport_gstin": gstin or None}
    results = sorted(seen.values(), key=lambda x: x["transport_name"])[:limit]
    return {"status": "success", "data": results}


def preview_gr(gr_no: str) -> dict:
    """The 'search a GR number' step — same shape that ends up in a
    bill's metadata, so what the user sees while building a bill is
    exactly what gets saved, not an approximation."""
    if not gr_no:
        return {"status": "error", "message": "gr_no is required", "status_code": 400}
    sb = get_supabase()
    metadata, unmatched, _ = _resolve_gr_metadata(sb, [gr_no.strip()])
    if unmatched:
        return {"status": "error", "message": f"GR '{gr_no}' not found in bilty or station_bilty_summary", "status_code": 404}
    return {"status": "success", "data": metadata[0]}


def create_bilty_crossing_bill(data: dict) -> dict:
    """
    data = {
      transport_name, transport_gstin?, bill_month (1-12), bill_year,
      gr_nos: [...],  created_by?
    }

    For each gr_no, pulls crossing proof + kaat/pf from bilty_wise_kaat,
    and amount/consignor/destination + the crossing proof photo URL from
    whichever of `bilty` / `station_bilty_summary` the GR belongs to.
    GRs with no bilty_wise_kaat row still get included (kaat/pf default
    to 0) — flagged back in `warnings` so nothing silently vanishes.
    """
    transport_name = (data.get("transport_name") or "").strip()
    transport_gstin = (data.get("transport_gstin") or "").strip().upper() or None
    bill_month = data.get("bill_month")
    bill_year = data.get("bill_year")
    gr_nos = data.get("gr_nos") or []
    created_by = data.get("created_by")

    if not transport_name:
        return {"status": "error", "message": "transport_name is required", "status_code": 400}
    if not bill_month or not (1 <= int(bill_month) <= 12):
        return {"status": "error", "message": "bill_month is required (1-12)", "status_code": 400}
    if not bill_year:
        return {"status": "error", "message": "bill_year is required", "status_code": 400}
    if not gr_nos:
        return {"status": "error", "message": "gr_nos is required and must not be empty", "status_code": 400}

    gr_nos = list(dict.fromkeys(gr_nos))  # dedupe, preserve order
    sb = get_supabase()

    metadata, unmatched_gr, no_kaat_gr = _resolve_gr_metadata(sb, gr_nos)
    total_kaat = round(sum(m["kaat"] for m in metadata), 2)
    total_pf = round(sum(m["pf"] for m in metadata), 2)
    total_amount = round(sum(m["amount"] for m in metadata), 2)

    bill_year = int(bill_year)
    bill_month = int(bill_month)
    bill_no = _next_bill_no(sb, bill_year, bill_month)

    record = {
        "bill_no": bill_no,
        "transport_name": transport_name,
        "transport_gstin": transport_gstin,
        "bill_month": bill_month,
        "bill_year": bill_year,
        "metadata": metadata,
        "total_bilties": len(gr_nos),
        "total_kaat": round(total_kaat, 2),
        "total_pf": round(total_pf, 2),
        "total_amount": round(total_amount, 2),
        "created_by": created_by,
        "updated_by": created_by,
    }

    try:
        res = sb.table("bilty_crossing_bill").insert(record).execute()
    except Exception as e:
        msg = str(e)
        if "uq_bilty_crossing_bill_transport_period" in msg:
            return {"status": "error",
                    "message": f"An active bill already exists for {transport_gstin or transport_name} for {bill_month}/{bill_year}",
                    "status_code": 409}
        return {"status": "error", "message": msg, "status_code": 500}

    if not res.data:
        return {"status": "error", "message": "Insert failed", "status_code": 500}

    response = {"status": "success", "message": f"Bill {bill_no} created", "data": res.data[0]}
    warnings = {}
    if unmatched_gr:
        warnings["unmatched_gr_nos"] = unmatched_gr
    if no_kaat_gr:
        warnings["no_kaat_data_gr_nos"] = no_kaat_gr
    if warnings:
        warnings["message"] = "Some GRs had no bilty/station record and/or no kaat data — included with zeroed/blank fields."
        response["warnings"] = warnings
    return response


def get_bilty_crossing_bill(bill_id: str) -> dict:
    sb = get_supabase()
    res = sb.table("bilty_crossing_bill").select(BILL_COLS).eq("id", bill_id).execute()
    if not res.data:
        return {"status": "error", "message": "Bill not found", "status_code": 404}
    return {"status": "success", "data": res.data[0]}


def list_bilty_crossing_bills(
    transport_gstin: str = None,
    bill_month: int = None,
    bill_year: int = None,
    is_active: bool = True,
    page: int = 1,
    page_size: int = 40,
) -> dict:
    sb = get_supabase()
    q = sb.table("bilty_crossing_bill").select(BILL_COLS, count="exact")
    if transport_gstin:
        q = q.eq("transport_gstin", transport_gstin.strip().upper())
    if bill_month:
        q = q.eq("bill_month", bill_month)
    if bill_year:
        q = q.eq("bill_year", bill_year)
    if is_active is not None:
        q = q.eq("is_active", is_active)

    offset = (page - 1) * page_size
    q = q.order("created_at", desc=True).range(offset, offset + page_size - 1)
    res = q.execute()
    rows = res.data or []
    total = res.count if res.count is not None else len(rows)
    return {
        "status": "success",
        "data": {"rows": rows, "page": page, "page_size": page_size,
                  "total": total, "has_more": (offset + page_size) < total},
    }


_EDITABLE_FIELDS = {"pdf_url", "transport_name", "transport_gstin"}


def update_bilty_crossing_bill(bill_id: str, data: dict) -> dict:
    """Save the bill's PDF URL (after uploading it, same pattern as
    crossing_bill.bill_url) or correct transport_name/transport_gstin.
    data = { pdf_url?, transport_name?, transport_gstin?, updated_by? }"""
    sb = get_supabase()
    existing = sb.table("bilty_crossing_bill").select("id").eq("id", bill_id).execute().data
    if not existing:
        return {"status": "error", "message": "Bill not found", "status_code": 404}

    payload = {k: v for k, v in (data or {}).items() if k in _EDITABLE_FIELDS}
    if not payload:
        return {"status": "error", "message": "No editable fields supplied", "status_code": 400}
    payload["updated_by"] = data.get("updated_by")
    payload["updated_at"] = _now()

    res = sb.table("bilty_crossing_bill").update(payload).eq("id", bill_id).execute()
    return {"status": "success", "message": "Bill updated", "data": (res.data or [None])[0]}


def delete_bilty_crossing_bill(bill_id: str, updated_by: str = None) -> dict:
    """Soft-delete — frees the (transport_gstin, bill_month, bill_year) slot for a corrected re-bill."""
    sb = get_supabase()
    existing = sb.table("bilty_crossing_bill").select("id, bill_no").eq("id", bill_id).execute().data
    if not existing:
        return {"status": "error", "message": "Bill not found", "status_code": 404}
    sb.table("bilty_crossing_bill").update({
        "is_active": False, "updated_by": updated_by, "updated_at": _now(),
    }).eq("id", bill_id).execute()
    return {"status": "success", "message": f"Bill {existing[0]['bill_no']} deleted"}
