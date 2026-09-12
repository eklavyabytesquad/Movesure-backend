"""
Bulk E-Way Bill Details (per challan)
=======================================
The single-EWB endpoint (GET /api/ewaybill) is what the frontend's
EWBPDFGenerator calls once per bilty to build one PDF page. Printing an
entire challan today means opening that modal once per GR and clicking
Print each time.

This collects every distinct EWB number riding on a challan (from
transit_details -> bilty / station_bilty_summary, same join
transit_service.get_transit_bilties() already does) and fetches all of
their govt details in one call, in parallel, so the frontend can loop the
results into ONE jsPDF document (one addEWBContent() call per item,
pdf.addPage() between) instead of N separate ones.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.challan.transit_service import get_transit_bilties
from services.ewaybill.ewaybill_service import get_ewaybill_details

MAX_WORKERS = 6  # parallel govt-API calls — keeps a 20-30 GR challan well under the request timeout


def _split_ewb_numbers(raw: str) -> list:
    """e_way_bill columns store one or more EWB numbers comma-separated."""
    if not raw:
        return []
    return [p.strip() for p in str(raw).split(",") if p.strip()]


def get_challan_ewaybills_bulk(challan_no: str, gstin: str) -> dict:
    """
    Returns every EWB on the challan, each fetched in full (same shape as
    GET /api/ewaybill's `data`), plus which GR number(s) it belongs to.
    """
    if not challan_no or not gstin:
        return {"status": "error", "message": "challan_no and gstin are required", "status_code": 400}

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

    results = []

    def _fetch(ewb_number: str):
        return ewb_number, get_ewaybill_details(ewb_number, gstin)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(_fetch, ewb) for ewb in ewb_to_grs]
        for future in as_completed(futures):
            ewb_number, detail = future.result()
            results.append({
                "ewb_number": ewb_number,
                "gr_nos": ewb_to_grs[ewb_number],
                "status": detail.get("status"),
                "data": detail.get("data"),
                "error": detail.get("message") if detail.get("status") == "error" else None,
            })

    # Stable order for printing: by the first GR number each EWB belongs to
    results.sort(key=lambda r: (r["gr_nos"][0] if r["gr_nos"] else "", r["ewb_number"]))

    success_count = sum(1 for r in results if r["status"] == "success")
    failed = [r for r in results if r["status"] != "success"]

    return {
        "status": "success",
        "data": {
            "challan_no": challan_no,
            "total_ewb": len(results),
            "success_count": success_count,
            "failed_count": len(failed),
            "grs_without_ewb": grs_without_ewb,
            "results": results,
        },
    }
