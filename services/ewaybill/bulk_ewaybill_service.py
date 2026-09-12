"""
Bulk E-Way Bill Details (per challan) — served from our own cache
====================================================================
Originally this called Masters India's GetEwayBillData once per EWB.
That live govt lookup turned out to be unreliable in bulk (NIC's backend
returns "NIC01: NIC responded with an error" fairly often, and it does
that per-EWB regardless of whether calls are made in parallel or one at a
time — confirmed live, not a concurrency/rate-limit artifact of this
code).

The frontend's Part-B screen already runs a full "Validate All" pass over
every EWB on a challan and stores each govt response, TABLE, in
ewb_validations.raw_result_metadata — including the full itemList /
consignor / consignee / transporter payload the PDF generator needs, not
just the pass/fail flag. So this now reads that cache directly instead of
re-asking Masters India: it's already there, it's not going to change for
an EWB once generated, and it isn't subject to NIC's live flakiness.

If an EWB was never validated (no row yet, or validation itself failed),
that one comes back as "not_validated" — the fix is to run Validate on
it in the Part-B screen first, not to retry this endpoint.
"""
from services.challan.transit_service import get_transit_bilties
from services.supabase_client import get_supabase


def _split_ewb_numbers(raw: str) -> list:
    """e_way_bill columns store one or more EWB numbers comma-separated."""
    if not raw:
        return []
    return [p.strip() for p in str(raw).split(",") if p.strip()]


def _extract_message(raw_result_metadata) -> dict | None:
    """
    raw_result_metadata was saved as our own backend's {"data": <masters
    india response>} wrapper, and Masters India's own response is itself
    {"data": {"results": {"message": {...}}}} — hence the double "data".
    Walk it defensively so a slightly different save shape doesn't break this.
    """
    node = raw_result_metadata
    for _ in range(8):
        if not isinstance(node, dict):
            return None
        if "eway_bill_number" in node:
            return node
        if "message" in node and isinstance(node["message"], dict):
            node = node["message"]
            continue
        if "results" in node:
            node = node["results"]
            continue
        if "data" in node:
            node = node["data"]
            continue
        return None
    return None


def get_challan_ewaybills_bulk(challan_no: str) -> dict:
    """
    Every EWB on the challan, pulled from our own validation cache
    (ewb_validations) — no live Masters India/NIC call at all.
    """
    if not challan_no:
        return {"status": "error", "message": "challan_no is required", "status_code": 400}

    transit_res = get_transit_bilties(challan_no, page_size=10000)
    if transit_res["status"] != "success":
        return transit_res

    rows = transit_res["data"]["rows"]
    if not rows:
        return {"status": "error", "message": f"No bilties found on challan {challan_no}", "status_code": 404}

    # One EWB can (rarely) cover more than one GR — map ewb_number -> [gr_no, ...]
    ewb_to_grs: dict[str, list] = {}
    for r in rows:
        for ewb in _split_ewb_numbers(r.get("e_way_bill")):
            ewb_to_grs.setdefault(ewb, []).append(r["gr_no"])

    grs_without_ewb = [r["gr_no"] for r in rows if not _split_ewb_numbers(r.get("e_way_bill"))]

    if not ewb_to_grs:
        return {"status": "error", "message": f"No e-way bill numbers found on challan {challan_no}", "status_code": 404}

    sb = get_supabase()
    ewb_numbers = list(ewb_to_grs.keys())
    cached = (
        sb.table("ewb_validations")
        .select("ewb_number, is_valid, validated_at, raw_result_metadata")
        .in_("ewb_number", ewb_numbers)
        .order("validated_at", desc=True)
        .execute()
        .data or []
    )

    # Keep only the latest cached row per EWB number.
    latest_by_ewb = {}
    for row in cached:
        if row["ewb_number"] not in latest_by_ewb:
            latest_by_ewb[row["ewb_number"]] = row

    results = []
    for ewb_number, gr_nos in ewb_to_grs.items():
        cached_row = latest_by_ewb.get(ewb_number)
        message = _extract_message(cached_row["raw_result_metadata"]) if cached_row else None

        if message:
            results.append({
                "ewb_number": ewb_number, "gr_nos": gr_nos,
                "status": "success", "message": message,
                "validated_at": cached_row["validated_at"], "error": None,
            })
        elif cached_row:
            results.append({
                "ewb_number": ewb_number, "gr_nos": gr_nos,
                "status": "error", "message": None, "validated_at": cached_row["validated_at"],
                "error": "Validated but no usable detail was cached for this EWB",
            })
        else:
            results.append({
                "ewb_number": ewb_number, "gr_nos": gr_nos,
                "status": "not_validated", "message": None, "validated_at": None,
                "error": "Not validated yet — run Validate on this EWB in the Part-B screen first",
            })

    results.sort(key=lambda r: (r["gr_nos"][0] if r["gr_nos"] else "", r["ewb_number"]))

    success_count = sum(1 for r in results if r["status"] == "success")
    not_validated = [r["ewb_number"] for r in results if r["status"] == "not_validated"]
    failed = [r for r in results if r["status"] == "error"]

    return {
        "status": "success",
        "data": {
            "challan_no": challan_no,
            "total_ewb": len(results),
            "success_count": success_count,
            "not_validated_count": len(not_validated),
            "failed_count": len(failed),
            "grs_without_ewb": grs_without_ewb,
            "results": results,
        },
    }
