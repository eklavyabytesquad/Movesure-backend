"""
FastAPI Backend for E-Way Bill Management & Bilty Operations
"""
import logging
import sys
import time

# ── Logging setup (stdout so Coolify / Docker captures it) ──────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
# Silence noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)  # we log ourselves

log = logging.getLogger("movesure")

from fastapi import FastAPI, Request, Query, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from contextlib import asynccontextmanager
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Import service modules
from auth.auth_service import get_jwt_token, load_jwt_token
from services.ewaybill.ewaybill_service import get_ewaybill_details
from services.ewaybill.consolidated_ewaybill_service import create_consolidated_ewaybill
from services.ewaybill.transporter_id_service import update_transporter_id
from services.ewaybill.transporter_update_with_pdf_service import update_transporter_and_get_pdf
from services.ewaybill.extend_ewaybill_service import extend_ewaybill_validity
from services.ewaybill.distance_service import get_distance
from services.ewaybill.gstin_details_service import get_gstin_details
from services.ewaybill.transporter_details_service import get_transporter_details
from services.ewaybill.generate_ewaybill_service import generate_ewaybill, generate_delivery_challan_ewaybill
from services.bilty.reference_data_service import get_reference_data
from services.bilty.bilty_save_service import save_bilty, get_bilty_with_cities
from services.bilty.payment_tracking_service import (
    save_bilty_payment, save_station_bilty_payment,
    get_bilty_payment_details, get_station_bilty_payment_details
)
from services.bilty.consignor_rates_service import get_consignor_rates, get_default_rates, get_all_rates, calculate_dd_charge
from services.bilty.gr_reservation_service import (
    get_next_available_grs, reserve_gr, release_reservation,
    complete_reservation, extend_reservation, get_branch_gr_status,
    release_all_user_reservations, fix_gr_sequence, cleanup_expired_reservations,
    validate_bill_book,
)
from services.bilty.master_data_service import (
    list_records, get_record, create_record, update_record, delete_record,
    bulk_update, bulk_create, bulk_delete,
)
from services.bilty.transport_pending_service import get_all_transport_pending_bilties
from services.bilty.transport_pending_grouped_service import get_grouped_transport_pending_bilties
from services.bilty.transport_bilty_report_service import get_transport_bilty_report
from services.kaat.kaat_update_service import bulk_update_kaat_rate, bulk_update_kaat_by_gr_nos, update_single_gr_kaat
from services.kaat.kaat_bill_report_service import get_kaat_bill_report
from services.challan.challan_book_service import (
    list_challan_books, get_challan_book, create_challan_book, update_challan_book,
)
from services.challan.challan_service import (
    list_challans, get_challan, create_challan, update_challan,
    dispatch_challan, undispatch_challan, mark_hub_received, delete_challan,
    get_challan_init,
)
from services.challan.transit_service import (
    get_available_bilties, get_transit_bilties, add_to_transit,
    remove_from_transit, bulk_remove_from_transit,
    bulk_update_delivery_status, get_challan_stats,
)
from services.challan.truck_trip_service import (
    list_trips, get_trip, create_trip, update_trip, delete_trip,
    dispatch_trip, receive_trip, link_challans, unlink_challan,
    create_trip_with_challans, add_challan_to_trip, get_trip_init,
)
from services.staff_service import list_staff, get_staff_member, create_staff, update_staff, deactivate_staff
from services.truck_service import list_trucks, get_truck
from services.crossing_bill.crossing_bill_service import (
    get_unbilled_pohonch, create_crossing_bill, add_transaction,
    update_bill, list_crossing_bills, get_crossing_bill,
    remove_pohonch_from_bill, cancel_crossing_bill,
)
from services.pohonch.pohonch_service import (
    list_pohonch, get_pohonch, get_pohonch_by_number,
    update_pohonch, sign_pohonch, unsign_pohonch, delete_pohonch,
)
from services.pohonch.pohonch_create_service import create_pohonch_from_gr_items
from services.pohonch.pohonch_edit_service import (
    edit_pohonch, update_gr_fields, recalculate_pohonch, bulk_recalculate_pohonch,
)
from services.invoices.tenant_service import (
    list_tenants, get_tenant, create_tenant, update_tenant, delete_tenant,
)
from services.invoices.inventory_service import (
    list_inventory, get_inventory_item, create_inventory_item,
    update_inventory_item, delete_inventory_item,
)
from services.invoices.receiver_service import (
    list_receivers, get_receiver, create_receiver, update_receiver, delete_receiver,
)
from services.invoices.series_service import (
    list_series, create_series, update_series, delete_series,
)
from services.invoices.invoice_service import (
    create_invoice, list_invoices, get_invoice,
    update_invoice, cancel_invoice, delete_invoice, update_line_items,
)
from services.invoices.payment_service import (
    add_payment, list_payments, delete_payment,
)


@asynccontextmanager
async def lifespan(app):
    # Startup
    log.info("=" * 70)
    log.info("STARTING E-WAY BILL API SERVER (FastAPI)")
    log.info("=" * 70)
    token = load_jwt_token()
    if token:
        log.info("JWT Token loaded successfully")
    else:
        log.warning("Getting new JWT token...")
        token = get_jwt_token()
        if token:
            log.info("JWT Token obtained successfully")
        else:
            log.error("Failed to get JWT token. Server may not work properly.")
    log.info("Server running at: http://localhost:5000")
    log.info("Available Endpoints:")
    for ep in [
        "GET  /api/health",
        "GET  /api/ewaybill?eway_bill_number=XXX&gstin=YYY",
        "POST /api/consolidated-ewaybill",
        "POST /api/transporter-update",
        "POST /api/transporter-update-with-pdf (2 API calls)",
        "POST /api/extend-ewaybill",
        "GET  /api/distance?fromPincode=XXX&toPincode=YYY",
        "GET  /api/gstin-details?userGstin=XXX&gstin=YYY",
        "GET  /api/transporter-details?userGstin=XXX&gstin=YYY",
        "POST /api/generate-ewaybill",
        "POST /api/refresh-token",
        "GET  /api/bilty/reference-data?branch_id=XXX&user_id=YYY",
        "POST /api/bilty/save",
        "GET  /api/bilty/{bilty_id}",
        "POST /api/bilty/payment/save",
        "GET  /api/bilty/payment/{bilty_id}",
        "POST /api/station-bilty/payment/save",
        "GET  /api/station-bilty/payment/{gr_no}",
        "GET  /api/bilty/rates/consignor/{consignor_id}",
        "GET  /api/bilty/rates/default?branch_id=XXX",
        "GET  /api/bilty/rates/all?consignor_id=XXX&branch_id=YYY",
    ]:
        log.info("  - %s", ep)
    log.info("Token auto-refresh enabled - Server will run continuously!")
    log.info("=" * 70)
    yield
    # Shutdown — clean up both thread pools
    from services.thread_pool import shared_pool
    _executor.shutdown(wait=False)
    shared_pool.shutdown(wait=False)


app = FastAPI(title="Movesure Backend", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Comprehensive Request/Response Logging Middleware ──────────────
@app.middleware("http")
async def log_requests_responses(request: Request, call_next):
    import json

    request_id = f"{datetime.now().isoformat()}"

    # Log request details
    request_body = b""
    if request.method in ["POST", "PUT", "PATCH"]:
        request_body = await request.body()
        request._body = request_body

    log.info(f"\n{'='*80}")
    log.info(f"[REQUEST #{request_id}] {request.method} {request.url.path}")

    if request.query_params:
        log.info(f"  Query Params: {dict(request.query_params)}")

    if request_body:
        try:
            body_dict = json.loads(request_body)
            log.info(f"  Body: {json.dumps(body_dict, indent=2)}")
        except:
            log.info(f"  Body: {request_body.decode('utf-8', errors='ignore')}")

    # Call the endpoint and measure time
    start_time = time.time()
    response = await call_next(request)
    elapsed_time = time.time() - start_time

    log.info(f"[RESPONSE #{request_id}] Status: {response.status_code} | Time: {elapsed_time:.3f}s")
    log.info(f"{'='*80}\n")

    return response


# Thread pool for running blocking service calls without blocking the event loop
# Sized to handle concurrent requests — services reuse this pool instead of creating their own
_executor = ThreadPoolExecutor(max_workers=20)

# Semaphore limits concurrent blocking tasks — returns 503 immediately instead of hanging
_semaphore = asyncio.Semaphore(16)


async def _run(func, *args):
    """Run a blocking function in the thread pool.
    Queues if all slots busy; raises _OverloadError if wait exceeds 30 s."""
    async def _inner():
        async with _semaphore:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(_executor, func, *args)
    try:
        return await asyncio.wait_for(_inner(), timeout=30)
    except asyncio.TimeoutError:
        raise _OverloadError()


class _OverloadError(Exception):
    """Raised when server is at capacity."""
    pass


@app.middleware("http")
async def access_log(request: Request, call_next):
    """Log every request with method, path, status code, and duration."""
    start = time.perf_counter()
    response = None
    try:
        response = await call_next(request)
        return response
    except _OverloadError:
        log.warning("SERVER OVERLOADED - %s %s", request.method, request.url.path)
        return JSONResponse(
            content={"status": "error", "message": "Server busy, please retry in a moment"},
            status_code=503,
            headers={"Retry-After": "2"},
        )
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        status = response.status_code if response else 500
        log_fn = log.warning if status >= 400 else log.info
        log_fn("%s %s -> %d (%.1fms)", request.method, request.url.path, status, duration_ms)


def _response(result: dict) -> JSONResponse:
    """Convert a service result dict to a JSONResponse."""
    if result.get("status") == "success":
        return JSONResponse(content=result, status_code=200)
    return JSONResponse(content=result, status_code=result.get("status_code", 500))


# ── Middleware: JWT token validation for e-way bill endpoints ──

SKIP_AUTH_PATHS = {"/api/health", "/api/refresh-token", "/docs", "/openapi.json", "/redoc"}


@app.middleware("http")
async def ensure_valid_token(request: Request, call_next):
    path = request.url.path
    if (path in SKIP_AUTH_PATHS
            or path.startswith("/api/bilty")
            or path.startswith("/api/station-bilty")
            or path.startswith("/api/challan")
            or path.startswith("/api/truck-trips")
            or path.startswith("/api/staff")
            or path.startswith("/api/trucks")
            or path.startswith("/api/crossing-bill")
        or path.startswith("/api/invoice")):
        return await call_next(request)

    log.info("Token check: %s %s", request.method, path)
    token = load_jwt_token()
    if not token:
        log.warning("Token missing, attempting refresh...")
        token = get_jwt_token()
        if not token:
            log.error("Failed to obtain valid JWT token for %s %s", request.method, path)
            return JSONResponse(
                content={"status": "error", "message": "Authentication failed. Unable to obtain valid JWT token."},
                status_code=503,
            )
        log.info("JWT token refreshed successfully")
    else:
        log.debug("JWT token valid")

    return await call_next(request)


# ============================================================
# E-WAY BILL ENDPOINTS (unchanged paths & behaviour)
# ============================================================


@app.get("/api/ewaybill")
async def get_ewaybill(eway_bill_number: str = Query(None), gstin: str = Query(None)):
    try:
        if not eway_bill_number or not gstin:
            return JSONResponse(
                content={"status": "error", "message": "Missing required parameters: eway_bill_number and gstin"},
                status_code=400,
            )
        result = await _run(get_ewaybill_details, eway_bill_number, gstin)
        return _response(result)
    except Exception as e:
        log.exception("Error in get_ewaybill: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/consolidated-ewaybill")
async def consolidated_ewaybill_endpoint(request: Request):
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)
        result = await _run(create_consolidated_ewaybill, data)
        return _response(result)
    except Exception as e:
        log.exception("Error in consolidated_ewaybill: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/refresh-token")
async def refresh_token():
    try:
        token = get_jwt_token()
        if token:
            return JSONResponse(
                content={"status": "success", "message": "JWT token refreshed successfully", "token": token},
                status_code=200,
            )
        return JSONResponse(content={"status": "error", "message": "Failed to refresh JWT token"}, status_code=500)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Error: {str(e)}"}, status_code=500)


@app.post("/api/transporter-update")
async def transporter_update(request: Request):
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)

        required_fields = ['user_gstin', 'eway_bill_number', 'transporter_id', 'transporter_name']
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            return JSONResponse(
                content={"status": "error", "message": f"Missing required fields: {', '.join(missing_fields)}"},
                status_code=400,
            )

        result = await _run(
            lambda: update_transporter_id(
                user_gstin=data['user_gstin'],
                eway_bill_number=data['eway_bill_number'],
                transporter_id=data['transporter_id'],
                transporter_name=data['transporter_name'],
            )
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in transporter_update: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/transporter-update-with-pdf")
async def transporter_update_with_pdf(request: Request):
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)

        required_fields = ['user_gstin', 'eway_bill_number', 'transporter_id', 'transporter_name']
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            return JSONResponse(
                content={"status": "error", "message": f"Missing required fields: {', '.join(missing_fields)}"},
                status_code=400,
            )

        result = await _run(
            lambda: update_transporter_and_get_pdf(
                user_gstin=data['user_gstin'],
                eway_bill_number=data['eway_bill_number'],
                transporter_id=data['transporter_id'],
                transporter_name=data['transporter_name'],
            )
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in transporter_update_with_pdf: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/extend-ewaybill")
async def extend_ewaybill(request: Request):
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)
        result = await _run(extend_ewaybill_validity, data)
        return _response(result)
    except Exception as e:
        log.exception("Error in extend_ewaybill: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/distance")
async def distance(fromPincode: str = Query(None), toPincode: str = Query(None)):
    try:
        if not fromPincode or not toPincode:
            return JSONResponse(
                content={"status": "error", "message": "Missing required query parameters: fromPincode and toPincode"},
                status_code=400,
            )
        result = await _run(get_distance, fromPincode, toPincode)
        return _response(result)
    except Exception as e:
        log.exception("Error in distance: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/gstin-details")
async def gstin_details(userGstin: str = Query(None), gstin: str = Query(None)):
    try:
        if not userGstin or not gstin:
            return JSONResponse(
                content={"status": "error", "message": "Missing required query parameters: userGstin and gstin"},
                status_code=400,
            )
        result = await _run(get_gstin_details, userGstin, gstin)
        return _response(result)
    except Exception as e:
        log.exception("Error in gstin_details: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/transporter-details")
async def transporter_details(userGstin: str = Query(None), gstin: str = Query(None)):
    try:
        if not userGstin or not gstin:
            return JSONResponse(
                content={"status": "error", "message": "Missing required query parameters: userGstin and gstin"},
                status_code=400,
            )
        result = await _run(get_transporter_details, userGstin, gstin)
        return _response(result)
    except Exception as e:
        log.exception("Error in transporter_details: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/generate-ewaybill")
async def generate_ewaybill_endpoint(request: Request):
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)
        result = await _run(generate_ewaybill, data)
        return _response(result)
    except Exception as e:
        log.exception("Error in generate_ewaybill: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/generate-delivery-challan")
async def generate_delivery_challan_endpoint(request: Request):
    """
    Generate a Delivery Challan EWB with pre-set defaults:
      document_type        = Delivery Challan
      sub_supply_type      = Others
      sub_supply_description = auto-generated from document_number

    Caller only needs to supply parties, amounts, items, and transport fields.
    """
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)
        result = await _run(generate_delivery_challan_ewaybill, data)
        return _response(result)
    except Exception as e:
        log.exception("Error in generate_delivery_challan: %s", e)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


# ============================================================
# BILTY ENDPOINTS - Server-side validated bilty operations
# ============================================================


@app.get("/api/bilty/reference-data")
async def bilty_reference_data(branch_id: str = Query(None), user_id: str = Query(None)):
    try:
        if not branch_id or not user_id:
            return JSONResponse(
                content={"status": "error", "message": "Missing required parameters: branch_id and user_id"},
                status_code=400,
            )
        result = await _run(get_reference_data, branch_id, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/bilty/save")
async def bilty_save(request: Request):
    try:
        data = await request.json()
        if not data:
            return JSONResponse(content={"status": "error", "message": "No data provided"}, status_code=400)
        result = await _run(save_bilty, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


# ============================================================
# RATE ENDPOINTS - Consignor profile rates & default rates
# ============================================================


@app.get("/api/bilty/rates/consignor/{consignor_id}")
async def consignor_rates(consignor_id: str = Path(...)):
    """Fetch all active rate profiles for a consignor (from consignor_bilty_profile)."""
    try:
        result = await _run(get_consignor_rates, consignor_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/bilty/rates/default")
async def default_rates(branch_id: str = Query(...)):
    """Fetch default city-wise rates for a branch (from rates table)."""
    try:
        result = await _run(get_default_rates, branch_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/bilty/rates/all")
async def all_rates(consignor_id: str = Query(...), branch_id: str = Query(...)):
    """Fetch both consignor-specific and default rates in parallel."""
    try:
        result = await _run(get_all_rates, consignor_id, branch_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/bilty/calculate/dd")
async def bilty_calculate_dd(
    consignor_id: str = Query(..., description="Consignor UUID"),
    destination_city_id: str = Query(..., description="Destination city UUID (destination_station_id)"),
    weight: float = Query(..., description="Gross weight in kg"),
    no_of_pkg: int = Query(..., description="Number of packages (nag)"),
):
    """
    Calculate door-delivery charge from the consignor bilty profile.

    Logic:
      1. Look up active consignor_bilty_profile row for consignor + destination city.
      2. If dd_charge_per_kg > 0  →  dd = dd_charge_per_kg × weight
         elif dd_charge_per_nag > 0 →  dd = dd_charge_per_nag × no_of_pkg
         else                        →  dd = 0
      3. Apply minimum of 150:  dd_charge = max(dd, 150)

    Returns dd_charge, raw_calculated, basis, per-unit rates, and profile_id.
    """
    try:
        result = await _run(calculate_dd_charge, consignor_id, destination_city_id, weight, no_of_pkg)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


# ============================================================
# GR RESERVATION ENDPOINTS - Atomic GR number management
# ============================================================


@app.get("/api/bilty/gr/next-available")
async def gr_next_available(
    bill_book_id: str = Query(...),
    branch_id: str = Query(...),
    count: int = Query(5),
):
    """Get next N available GR numbers (skips reserved + used)."""
    try:
        result = await _run(get_next_available_grs, bill_book_id, branch_id, count)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/reserve")
async def gr_reserve(request: Request):
    """Reserve the next available GR or a specific GR number."""
    try:
        data = await request.json()
        for f in ["bill_book_id", "branch_id", "user_id", "user_name"]:
            if not data.get(f):
                return JSONResponse(content={"status": "error", "message": f"Missing field: {f}"}, status_code=400)
        result = await _run(
            reserve_gr,
            data["bill_book_id"], data["branch_id"],
            data["user_id"], data["user_name"],
            data.get("gr_number"),  # optional — specific number
        )
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/release/{reservation_id}")
async def gr_release(request: Request, reservation_id: str = Path(...)):
    """Release a reservation (user no longer needs it)."""
    try:
        data = await request.json()
        if not data.get("user_id"):
            return JSONResponse(content={"status": "error", "message": "Missing user_id"}, status_code=400)
        result = await _run(release_reservation, reservation_id, data["user_id"])
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/complete/{reservation_id}")
async def gr_complete(request: Request, reservation_id: str = Path(...)):
    """Complete a reservation after bilty save — marks used + advances current_number."""
    try:
        data = await request.json()
        if not data.get("user_id"):
            return JSONResponse(content={"status": "error", "message": "Missing user_id"}, status_code=400)
        result = await _run(complete_reservation, reservation_id, data["user_id"])
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/extend/{reservation_id}")
async def gr_extend(request: Request, reservation_id: str = Path(...)):
    """Heartbeat — extend reservation TTL by 30 more minutes."""
    try:
        data = await request.json()
        if not data.get("user_id"):
            return JSONResponse(content={"status": "error", "message": "Missing user_id"}, status_code=400)
        result = await _run(extend_reservation, reservation_id, data["user_id"])
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/bilty/gr/status/{branch_id}")
async def gr_status(branch_id: str = Path(...), bill_book_id: str = Query(None)):
    """Live status: all active reservations + recent bilties for a branch."""
    try:
        result = await _run(get_branch_gr_status, branch_id, bill_book_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/release-all")
async def gr_release_all(request: Request):
    """Release ALL reservations for a user in a branch (logout / page close)."""
    try:
        data = await request.json()
        for f in ["user_id", "branch_id"]:
            if not data.get(f):
                return JSONResponse(content={"status": "error", "message": f"Missing field: {f}"}, status_code=400)
        result = await _run(release_all_user_reservations, data["user_id"], data["branch_id"])
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/fix-sequence")
async def gr_fix_sequence(request: Request):
    """Fix bill book current_number — auto-detect or manually set."""
    try:
        data = await request.json()
        if not data.get("bill_book_id"):
            return JSONResponse(content={"status": "error", "message": "Missing bill_book_id"}, status_code=400)
        result = await _run(fix_gr_sequence, data["bill_book_id"], data.get("correct_number"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/gr/cleanup")
async def gr_cleanup(request: Request):
    """Expire stale reservations. Can be called periodically or on-demand."""
    try:
        data = await request.json() if await request.body() else {}
        result = await _run(cleanup_expired_reservations, data.get("branch_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/bilty/gr/validate/{bill_book_id}")
async def gr_validate_bill_book(bill_book_id: str = Path(...)):
    """Validate & auto-correct bill book current_number. Call on every bill book load/edit."""
    try:
        result = await _run(validate_bill_book, bill_book_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# MASTER DATA CRUD — cities, transports, consignors, consignees, rates
# ============================================================


@app.get("/api/bilty/master/{entity}")
async def master_list(
    entity: str = Path(...),
    page: int = Query(1),
    page_size: int = Query(40),
    search: str = Query(None),
    branch_id: str = Query(None),
    city_id: str = Query(None),
    consignor_id: str = Query(None),
):
    """Paginated list. 40 rows/page. Pass search for text filter."""
    try:
        filters = {}
        if branch_id:
            filters["branch_id"] = branch_id
        if city_id:
            filters["city_id"] = city_id
        if consignor_id:
            filters["consignor_id"] = consignor_id
        result = await _run(list_records, entity, page, page_size, search, filters if filters else None)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/bilty/master/{entity}/{record_id}")
async def master_get(entity: str = Path(...), record_id: str = Path(...)):
    """Get a single record by ID."""
    try:
        result = await _run(get_record, entity, record_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/master/{entity}")
async def master_create(request: Request, entity: str = Path(...)):
    """Create a single record."""
    try:
        data = await request.json()
        user_id = data.pop("user_id", None)
        result = await _run(create_record, entity, data, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/bilty/master/{entity}/{record_id}")
async def master_update(request: Request, entity: str = Path(...), record_id: str = Path(...)):
    """Update a single record."""
    try:
        data = await request.json()
        user_id = data.pop("user_id", None)
        result = await _run(update_record, entity, record_id, data, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/bilty/master/{entity}/{record_id}")
async def master_delete(entity: str = Path(...), record_id: str = Path(...)):
    """Delete a single record."""
    try:
        result = await _run(delete_record, entity, record_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/master/{entity}/bulk-create")
async def master_bulk_create(request: Request, entity: str = Path(...)):
    """Create multiple records at once."""
    try:
        data = await request.json()
        user_id = data.get("user_id")
        records = data.get("records", [])
        if not records:
            return JSONResponse(content={"status": "error", "message": "records array is required"}, status_code=400)
        result = await _run(bulk_create, entity, records, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/bilty/master/{entity}/bulk-update")
async def master_bulk_update(request: Request, entity: str = Path(...)):
    """Update multiple records. Each item must have 'id' + fields to change."""
    try:
        data = await request.json()
        user_id = data.get("user_id")
        updates = data.get("updates", [])
        if not updates:
            return JSONResponse(content={"status": "error", "message": "updates array is required"}, status_code=400)
        result = await _run(bulk_update, entity, updates, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/bilty/master/{entity}/bulk-delete")
async def master_bulk_delete(request: Request, entity: str = Path(...)):
    """Delete multiple records by IDs."""
    try:
        data = await request.json()
        ids = data.get("ids", [])
        if not ids:
            return JSONResponse(content={"status": "error", "message": "ids array is required"}, status_code=400)
        result = await _run(bulk_delete, entity, ids)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# CHALLAN & TRANSIT MANAGEMENT
# ============================================================


# ── Challan Books ─────────────────────────────────────────────

@app.get("/api/challan/books")
async def challan_books_list(
    branch_id: str = Query(None),
    active_only: bool = Query(False),
    page: int = Query(1),
    page_size: int = Query(40),
):
    try:
        result = await _run(list_challan_books, branch_id, active_only, page, page_size)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/challan/books/{book_id}")
async def challan_book_get(book_id: str = Path(...)):
    try:
        result = await _run(get_challan_book, book_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/books")
async def challan_book_create(request: Request):
    try:
        data = await request.json()
        result = await _run(create_challan_book, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/challan/books/{book_id}")
async def challan_book_update(request: Request, book_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(update_challan_book, book_id, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Challans ──────────────────────────────────────────────────

@app.get("/api/challan/init")
async def challan_init(
    branch_id: str = Query(...),
):
    try:
        result = await _run(get_challan_init, branch_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/challan/list")
async def challans_list(
    branch_id: str = Query(None),
    is_dispatched: bool = Query(None),
    page: int = Query(1),
    page_size: int = Query(40),
    search: str = Query(None),
):
    try:
        result = await _run(list_challans, branch_id, is_dispatched, page, page_size, search)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/create")
async def challan_create(request: Request):
    try:
        data = await request.json()
        result = await _run(create_challan, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Transit (bilty ↔ challan assignments) ─────────────────────

@app.get("/api/challan/transit/available")
async def transit_available(
    page: int = Query(1),
    page_size: int = Query(50),
    search: str = Query(None),
    payment_mode: str = Query(None),
    city_id: str = Query(None),
    source: str = Query(None),
    branch_id: str = Query(None),
):
    try:
        result = await _run(get_available_bilties, page, page_size, search, payment_mode, city_id, source, branch_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/challan/transit/bilties/{challan_no}")
async def transit_bilties(
    challan_no: str = Path(...),
    page: int = Query(1),
    page_size: int = Query(10000),
    search: str = Query(None),
):
    try:
        result = await _run(get_transit_bilties, challan_no, page, page_size, search)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/challan/transit/stats/{challan_no}")
async def transit_stats(challan_no: str = Path(...)):
    try:
        result = await _run(get_challan_stats, challan_no)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/transit/add")
async def transit_add(request: Request):
    try:
        data = await request.json()
        result = await _run(add_to_transit, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/transit/remove/{transit_id}")
async def transit_remove(transit_id: str = Path(...)):
    try:
        result = await _run(remove_from_transit, transit_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/transit/bulk-remove")
async def transit_bulk_remove(request: Request):
    try:
        data = await request.json()
        result = await _run(bulk_remove_from_transit, data.get("transit_ids", []), data.get("challan_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/challan/transit/delivery-status")
async def transit_delivery_status(request: Request):
    try:
        data = await request.json()
        result = await _run(bulk_update_delivery_status, data.get("updates", []), data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Truck Trips  (static routes FIRST, then /{trip_id} dynamic routes) ───────

@app.get("/api/truck-trips/init")
async def truck_trip_init(branch_id: str = Query(None)):
    """Modal page-load: trucks + staff + unlinked challans in one call."""
    try:
        result = await _run(get_trip_init, branch_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips/create-with-challans")
async def truck_trip_create_with_challans(request: Request):
    """Atomic: create a trip and link challans in a single request."""
    try:
        data = await request.json()
        result = await _run(create_trip_with_challans, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/truck-trips")
async def truck_trips_list(
    branch_id: str = Query(None),
    truck_id:  str = Query(None),
    status:    str = Query(None),
    page:      int = Query(1),
    page_size: int = Query(40),
    search:    str = Query(None),
):
    try:
        result = await _run(list_trips, branch_id, truck_id, status, page, page_size, search)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips")
async def truck_trip_create(request: Request):
    try:
        data = await request.json()
        result = await _run(create_trip, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/truck-trips/{trip_id}")
async def truck_trip_get(trip_id: str = Path(...)):
    try:
        result = await _run(get_trip, trip_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/truck-trips/{trip_id}")
async def truck_trip_update(request: Request, trip_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(update_trip, trip_id, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/truck-trips/{trip_id}")
async def truck_trip_delete(trip_id: str = Path(...)):
    try:
        result = await _run(delete_trip, trip_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips/{trip_id}/dispatch")
async def truck_trip_dispatch(request: Request, trip_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(dispatch_trip, trip_id, data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips/{trip_id}/receive")
async def truck_trip_receive(request: Request, trip_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(receive_trip, trip_id, data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips/{trip_id}/link-challans")
async def truck_trip_link_challans(request: Request, trip_id: str = Path(...)):
    try:
        data = await request.json()
        challan_ids = data.get("challan_ids", [])
        result = await _run(link_challans, trip_id, challan_ids, data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips/{trip_id}/unlink-challan/{challan_id}")
async def truck_trip_unlink_challan(trip_id: str = Path(...), challan_id: str = Path(...)):
    try:
        result = await _run(unlink_challan, trip_id, challan_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/truck-trips/{trip_id}/add-challan/{challan_id}")
async def truck_trip_add_single_challan(
    request: Request,
    trip_id: str = Path(...),
    challan_id: str = Path(...),
):
    """Add one specific challan to an existing trip."""
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            pass
        result = await _run(add_challan_to_trip, trip_id, challan_id, data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Staff ─────────────────────────────────────────────────────────────────────

@app.get("/api/staff")
async def staff_list(
    post:        str  = Query(None),
    active_only: bool = Query(True),
    search:      str  = Query(None),
    page:        int  = Query(1),
    page_size:   int  = Query(100),
):
    try:
        result = await _run(list_staff, post, active_only, search, page, page_size)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/staff/{staff_id}")
async def staff_get(staff_id: str = Path(...)):
    try:
        result = await _run(get_staff_member, staff_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/staff")
async def staff_create(request: Request):
    try:
        data = await request.json()
        result = await _run(create_staff, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/staff/{staff_id}")
async def staff_update(request: Request, staff_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(update_staff, staff_id, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/staff/{staff_id}")
async def staff_deactivate(staff_id: str = Path(...)):
    try:
        result = await _run(deactivate_staff, staff_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Trucks ────────────────────────────────────────────────────────────────────

@app.get("/api/trucks")
async def trucks_list(
    active_only:    bool = Query(True),
    available_only: bool = Query(False),
    search:         str  = Query(None),
    page:           int  = Query(1),
    page_size:      int  = Query(100),
):
    try:
        result = await _run(list_trucks, active_only, available_only, search, page, page_size)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/trucks/{truck_id}")
async def truck_get(truck_id: str = Path(...)):
    try:
        result = await _run(get_truck, truck_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Crossing Bill (static routes first, then /{bill_id}) ─────────────────────

@app.get("/api/crossing-bill/pohonch")
async def crossing_bill_pohonch(
    transport_gstin: str = Query(None),
    transport_name:  str = Query(None),
    transport_id:    str = Query(None),
    from_date:       str = Query(None),
    to_date:         str = Query(None),
):
    """Return unbilled pohonch eligible for a new crossing bill."""
    try:
        result = await _run(get_unbilled_pohonch, transport_gstin, transport_name, transport_id, from_date, to_date)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/crossing-bill")
async def crossing_bill_list(
    transport_gstin: str = Query(None),
    transport_id:    str = Query(None),
    status:          str = Query(None),
    bill_month:      int = Query(None),
    bill_year:       int = Query(None),
    page:            int = Query(1),
    page_size:       int = Query(40),
):
    try:
        result = await _run(list_crossing_bills, transport_gstin, transport_id, status, bill_month, bill_year, page, page_size)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/crossing-bill")
async def crossing_bill_create(request: Request):
    """Create a crossing bill from selected pohonch numbers."""
    try:
        data = await request.json()
        result = await _run(create_crossing_bill, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/crossing-bill/{bill_id}")
async def crossing_bill_get(bill_id: str = Path(...)):
    try:
        result = await _run(get_crossing_bill, bill_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/crossing-bill/{bill_id}")
async def crossing_bill_update(request: Request, bill_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(update_bill, bill_id, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/crossing-bill/{bill_id}/transaction")
async def crossing_bill_add_transaction(request: Request, bill_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(add_transaction, bill_id, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/crossing-bill/{bill_id}/remove-pohonch/{pohonch_number}")
async def crossing_bill_remove_pohonch(
    request: Request,
    bill_id: str = Path(...),
    pohonch_number: str = Path(...),
):
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            pass
        result = await _run(remove_pohonch_from_bill, bill_id, pohonch_number, data.get("updated_by"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/crossing-bill/{bill_id}/cancel")
async def crossing_bill_cancel(request: Request, bill_id: str = Path(...)):
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            pass
        result = await _run(cancel_crossing_bill, bill_id, data.get("updated_by"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Challan by ID (catch-all — must be AFTER all /api/challan/... routes) ──

@app.get("/api/challan/{challan_id}")
async def challan_get(challan_id: str = Path(...)):
    try:
        result = await _run(get_challan, challan_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/challan/{challan_id}")
async def challan_update(request: Request, challan_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(update_challan, challan_id, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/{challan_id}/dispatch")
async def challan_dispatch(request: Request, challan_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(dispatch_challan, challan_id, data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/{challan_id}/undispatch")
async def challan_undispatch(challan_id: str = Path(...)):
    try:
        result = await _run(undispatch_challan, challan_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/challan/{challan_id}/hub-received")
async def challan_hub_received(request: Request, challan_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(mark_hub_received, challan_id, data.get("user_id"))
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/challan/{challan_id}")
async def challan_delete(challan_id: str = Path(...)):
    try:
        result = await _run(delete_challan, challan_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# GROUPED TRANSPORT PENDING BILTIES (by GSTIN, date range)
# ============================================================
from services.bilty.transport_pending_grouped_service import get_grouped_transport_pending_bilties

@app.post("/api/bilty/transport-pending-grouped")
async def transport_pending_grouped(request: Request):
    """
    Returns all bilties (across ALL transports, grouped by GSTIN) that are missing pohonch_no OR
    bilty_number in bilty_wise_kaat, for challans dispatched between given dates.
    Body: { "dispatch_date_from": "YYYY-MM-DD", "dispatch_date_to": "YYYY-MM-DD" }
    """
    try:
        data = await request.json()
        dispatch_date_from = data.get("dispatch_date_from")
        dispatch_date_to = data.get("dispatch_date_to")
        if not dispatch_date_from or not dispatch_date_to:
            return JSONResponse(content={"status": "error", "message": "dispatch_date_from and dispatch_date_to required"}, status_code=400)
        result = await _run(get_grouped_transport_pending_bilties, dispatch_date_from, dispatch_date_to)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# TRANSPORT PENDING BILTIES
# ============================================================


@app.get("/api/bilty/transport-pending")
async def transport_pending_bilties():
    """
    Returns all bilties (across ALL transports) that are missing pohonch_no OR
    bilty_number in bilty_wise_kaat.
    Grouped: transport → challan → serial-ordered bilties.
    """
    try:
        result = await _run(get_all_transport_pending_bilties)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# TRANSPORT BILTY REPORT  (must be BEFORE the {bilty_id} catch-all)
# ============================================================


@app.get("/api/bilty/transport-report")
async def transport_bilty_report(
    transport_gstin: str = Query(None, description="Transport GSTIN (exact match, preferred)"),
    transport_name:  str = Query(None, description="Transport name (partial match, fallback)"),
    from_date: str = Query(..., description="Start date inclusive  YYYY-MM-DD"),
    to_date:   str = Query(..., description="End date inclusive    YYYY-MM-DD"),
):
    """
    Fetch all bilties for a transport in a date range, merged from both
    `bilty` (type=regular) and `station_bilty_summary` (type=manual) tables.

    Response structure:
    {
      "with_pohonch": {
        "<pohonch_number>": {
          "regular": [ ...bilties from bilty table... ],
          "manual":  [ ...bilties from station_bilty_summary... ]
        }
      },
      "no_pohonch": {
        "<challan_no>": [ ...bilties... ],
        "UNKNOWN":      [ ...bilties with no challan... ]
      }
    }

    Each bilty includes: challan_no, challan_dispatch_date, pohonch_number,
    has_crossing_challan, crossing_challans, kaat fields.

    At least one of `transport_gstin` or `transport_name` is required.
    """
    if not transport_gstin and not transport_name:
        return JSONResponse(
            content={"status": "error", "message": "transport_gstin or transport_name is required"},
            status_code=400,
        )
    try:
        log.info(f"[TRANSPORT REPORT] Fetching bilties for transport_gstin={transport_gstin}, transport_name={transport_name}, from={from_date}, to={to_date}")
        result = await _run(
            get_transport_bilty_report,
            transport_gstin, transport_name, from_date, to_date,
        )
        import json
        # Log summary
        summary = result.get("summary", {})
        log.info(f"[TRANSPORT REPORT] Total bilties found: {summary.get('total')}, with_pohonch: {summary.get('with_pohonch')}, without_pohonch: {summary.get('without_pohonch')}")

        # Log sample bilty with content field
        sample_bilty = None
        with_p = result.get("with_pohonch", {})
        if with_p:
            for pohonch, groups in with_p.items():
                if groups.get("regular"):
                    sample_bilty = groups["regular"][0]
                    break

        if sample_bilty:
            log.info(f"[TRANSPORT REPORT] Sample bilty with content: gr_no={sample_bilty.get('gr_no')}, content={sample_bilty.get('contain')}, kaat={sample_bilty.get('kaat')}, payment_mode={sample_bilty.get('payment_mode')}")

        return _response(result)
    except Exception as e:
        log.exception("Error in transport_bilty_report: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# KAAT UPDATE ENDPOINTS
# ============================================================

class BulkKaatUpdateRequest(BaseModel):
    transport_gstin: str
    from_date: str
    to_date: str
    station_name: str
    new_kaat_rate: float
    new_kaat_dd: Optional[float] = None


class SingleGrKaatUpdateRequest(BaseModel):
    kaat_rate: Optional[float] = None
    kaat: Optional[float] = None
    kaat_dd: Optional[float] = None
    pf: Optional[float] = None


@app.post("/api/kaat/bulk-update")
async def kaat_bulk_update(body: BulkKaatUpdateRequest):
    """
    Bulk update kaat_rate (and optionally kaat_dd) for all bilties of a
    transport in a date range filtered by destination station.

    Body:
      transport_gstin  — exact GSTIN
      from_date        — YYYY-MM-DD (inclusive)
      to_date          — YYYY-MM-DD (inclusive)
      station_name     — partial city name, e.g. "SULTANPUR", "PRATAPGARH"
      new_kaat_rate    — new rate (kaat = weight * new_kaat_rate)
      new_kaat_dd      — optional, updates dd_chrg on each bilty
    """
    try:
        result = await _run(
            bulk_update_kaat_rate,
            body.transport_gstin, body.from_date, body.to_date,
            body.station_name, body.new_kaat_rate, body.new_kaat_dd,
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in kaat_bulk_update: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


class BulkKaatUpdateByGrRequest(BaseModel):
    gr_nos: list[str]
    new_kaat_rate: float
    new_kaat_dd: Optional[float] = None


@app.post("/api/kaat/bulk-update-by-grs")
async def kaat_bulk_update_by_grs(body: BulkKaatUpdateByGrRequest):
    """
    Bulk update kaat for an explicit list of GR numbers.

    Body:
      gr_nos        — list of GR numbers to update (bilty or station type)
      new_kaat_rate — new rate; kaat = weight * new_kaat_rate
      new_kaat_dd   — optional; updates dd_chrg on each bilty

    For each GR, weight/total are fetched from bilty or station_bilty_summary.
    kaat = weight * new_kaat_rate
    pf   = total - kaat - dd
    Both bilty_wise_kaat and pohonch.bilty_metadata are updated.
    """
    try:
        result = await _run(
            bulk_update_kaat_by_gr_nos,
            body.gr_nos, body.new_kaat_rate, body.new_kaat_dd,
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in kaat_bulk_update_by_grs: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


class KaatBillReportRequest(BaseModel):
    transport_gstin: str
    from_date: str
    to_date: str


@app.post("/api/kaat/bill-report")
async def kaat_bill_report(body: KaatBillReportRequest):
    """
    Generate kaat bill data for a transport over a date range.

    Body:
      transport_gstin  — exact GSTIN of the transport company
      from_date        — YYYY-MM-DD (inclusive)
      to_date          — YYYY-MM-DD (inclusive)

    Returns per-bilty: gr_no, pohonch nos, destination, payment_mode,
    delivery_type, pkgs, pvt_marks, weight, kaat_rate, dd, kaat,
    to_pay (null for paid), total (null for paid), pf ('PAID' for paid).
    Plus summary totals.
    """
    try:
        result = await _run(
            get_kaat_bill_report,
            body.transport_gstin, body.from_date, body.to_date,
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in kaat_bill_report: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.patch("/api/kaat/gr/{gr_no}")
async def kaat_single_gr_update(gr_no: str = Path(..., description="GR number to update"), body: SingleGrKaatUpdateRequest = None):
    """
    Update kaat fields for a single GR number.

    Body (all optional, at least one required):
      kaat_rate  — recalculates kaat = weight * kaat_rate, pf = total - kaat
      kaat       — set kaat directly; pf = total - kaat is recalculated
      kaat_dd    — update dd_chrg only
      pf         — override pf directly
    """
    if body is None:
        return JSONResponse(content={"status": "error", "message": "Request body is required"}, status_code=400)
    try:
        result = await _run(
            update_single_gr_kaat,
            gr_no, body.kaat_rate, body.kaat, body.kaat_dd, body.pf,
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in kaat_single_gr_update: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# BILTY GET (catch-all — must be AFTER all /api/bilty/... routes)
# ============================================================


@app.get("/api/bilty/{bilty_id}")
async def bilty_get(bilty_id: str = Path(...)):
    try:
        result = await _run(get_bilty_with_cities, bilty_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


# ============================================================
# PAYMENT TRACKING ENDPOINTS - Bilty & Station Summary
# ============================================================


@app.post("/api/bilty/payment/save")
async def save_bilty_payment_endpoint(request: Request):
    """
    Save payment details for a bilty.

    Body: {
        "bilty_id": "uuid",
        "payment_mode": "cash|online|partial|foc",
        "advance_amount": <numeric>,
        "payment_date": "2026-05-15" (optional),
        "payment_method": "cash|cheque|bank_transfer|upi" (optional),
        "reference_number": "CHQ123456" (optional),
        "notes": "payment notes" (optional),
        "add_transaction": {
            "amount": <numeric>,
            "method": "cash",
            "reference": "RECEIPT-001",
            "notes": "advance payment"
        } (optional)
    }
    """
    try:
        data = await request.json()
        bilty_id = data.get("bilty_id")
        if not bilty_id:
            return JSONResponse(content={"status": "error", "message": "bilty_id is required"}, status_code=400)

        payment_data = {
            "payment_mode": data.get("payment_mode"),
            "advance_amount": data.get("advance_amount", 0),
            "payment_date": data.get("payment_date"),
            "payment_method": data.get("payment_method"),
            "reference_number": data.get("reference_number"),
            "notes": data.get("notes"),
            "add_transaction": data.get("add_transaction")
        }

        result = await _run(save_bilty_payment, bilty_id, payment_data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/bilty/payment/{bilty_id}")
async def get_bilty_payment_endpoint(bilty_id: str = Path(...)):
    """Get payment details for a specific bilty."""
    try:
        log.info(f"[GET /api/bilty/payment] Fetching payment details for bilty_id: {bilty_id}")
        result = await _run(get_bilty_payment_details, bilty_id)
        import json
        log.info(f"[GET /api/bilty/payment] Response: {json.dumps(result, indent=2, default=str)}")
        return _response(result)
    except Exception as e:
        log.error(f"[GET /api/bilty/payment] Error: {str(e)}", exc_info=True)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.post("/api/station-bilty/payment/save")
async def save_station_bilty_payment_endpoint(request: Request):
    """
    Save payment details for a station_bilty_summary.

    Body: {
        "gr_no": "A00001",
        "payment_mode": "cash|online|partial|foc",
        "advance_amount": <numeric>,
        "payment_date": "2026-05-15" (optional),
        "payment_method": "cash|cheque|bank_transfer|upi" (optional),
        "reference_number": "CHQ123456" (optional),
        "notes": "payment notes" (optional),
        "add_transaction": {...} (optional)
    }
    """
    try:
        data = await request.json()
        gr_no = data.get("gr_no")
        if not gr_no:
            return JSONResponse(content={"status": "error", "message": "gr_no is required"}, status_code=400)

        payment_data = {
            "payment_mode": data.get("payment_mode"),
            "advance_amount": data.get("advance_amount", 0),
            "payment_date": data.get("payment_date"),
            "payment_method": data.get("payment_method"),
            "reference_number": data.get("reference_number"),
            "notes": data.get("notes"),
            "add_transaction": data.get("add_transaction")
        }

        result = await _run(save_station_bilty_payment, gr_no, payment_data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


@app.get("/api/station-bilty/payment/{gr_no}")
async def get_station_bilty_payment_endpoint(gr_no: str = Path(...)):
    """Get payment details for a station_bilty_summary by GR number."""
    try:
        log.info(f"[GET /api/station-bilty/payment] Fetching payment details for gr_no: {gr_no}")
        result = await _run(get_station_bilty_payment_details, gr_no)
        import json
        log.info(f"[GET /api/station-bilty/payment] Response: {json.dumps(result, indent=2, default=str)}")
        return _response(result)
    except Exception as e:
        log.error(f"[GET /api/station-bilty/payment] Error: {str(e)}", exc_info=True)
        return JSONResponse(content={"status": "error", "message": f"Internal server error: {str(e)}"}, status_code=500)


# ============================================================
# POHONCH ENDPOINTS
# ============================================================


@app.post("/api/pohonch/create")
async def pohonch_create(request: Request):
    """
    Create a pohonch from an explicit list of GR items.

    Body:
    {
      "transport_name":  "NEW INDIA EXPRESS TRANSPORT CO.",
      "transport_gstin": "09AACPY1378F3ZC",          // optional
      "challan_nos":     ["B00016"],
      "gr_items": [
        {"gr_no": "22789", "pohonch_bilty": "1031"},
        {"gr_no": "22790", "pohonch_bilty": "1031"},
        {"gr_no": "22791", "pohonch_bilty": "1031"},
        {"gr_no": "22803", "pohonch_bilty": "1031"}
      ],
      "pohonch_prefix":  "NIE",                       // optional – auto-derived if omitted
      "created_by":      "uuid"                        // optional
    }
    """
    try:
        data = await request.json()
        transport_name  = data.get("transport_name", "").strip()
        transport_gstin = data.get("transport_gstin", "")
        challan_nos     = data.get("challan_nos", [])
        gr_items        = data.get("gr_items", [])
        pohonch_prefix  = data.get("pohonch_prefix")
        created_by      = data.get("created_by")

        if not transport_name:
            return JSONResponse(content={"status": "error", "message": "transport_name is required"}, status_code=400)
        if not challan_nos:
            return JSONResponse(content={"status": "error", "message": "challan_nos array is required"}, status_code=400)
        if not gr_items:
            return JSONResponse(content={"status": "error", "message": "gr_items array is required"}, status_code=400)

        result = await _run(
            create_pohonch_from_gr_items,
            transport_name, transport_gstin, challan_nos,
            gr_items, pohonch_prefix, created_by,
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in pohonch_create: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/pohonch/list")
async def pohonch_list(
    transport_name: str = Query(None),
    transport_gstin: str = Query(None),
    is_signed: bool = Query(None),
    is_active: bool = Query(True),
    page: int = Query(1),
    page_size: int = Query(40),
    search: str = Query(None),
):
    """
    List pohonch records with optional filters.
    Filters: transport_name (partial), transport_gstin (exact), is_signed, is_active, search.
    """
    try:
        result = await _run(
            list_pohonch,
            transport_name, transport_gstin, is_signed, is_active,
            page, page_size, search,
        )
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/pohonch/number/{pohonch_number}")
async def pohonch_get_by_number(pohonch_number: str = Path(...)):
    """Get a single pohonch by its pohonch_number (e.g. JMST0001)."""
    try:
        result = await _run(get_pohonch_by_number, pohonch_number)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/pohonch/{pohonch_id}")
async def pohonch_update(request: Request, pohonch_id: str = Path(...)):
    """Update pohonch fields. Pass user_id in body for audit trail."""
    try:
        data = await request.json()
        user_id = data.pop("user_id", None)
        result = await _run(update_pohonch, pohonch_id, data, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/pohonch/{pohonch_id}/sign")
async def pohonch_sign(request: Request, pohonch_id: str = Path(...)):
    """Mark a pohonch as signed. Body: { user_id: '...' }"""
    try:
        data = await request.json()
        if not data.get("user_id"):
            return JSONResponse(content={"status": "error", "message": "Missing user_id"}, status_code=400)
        result = await _run(sign_pohonch, pohonch_id, data["user_id"])
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/pohonch/{pohonch_id}/unsign")
async def pohonch_unsign(request: Request, pohonch_id: str = Path(...)):
    """Revert a pohonch signature. Body: { user_id: '...' }"""
    try:
        data = await request.json()
        if not data.get("user_id"):
            return JSONResponse(content={"status": "error", "message": "Missing user_id"}, status_code=400)
        result = await _run(unsign_pohonch, pohonch_id, data["user_id"])
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/pohonch/{pohonch_id}")
async def pohonch_delete(pohonch_id: str = Path(...), user_id: str = Query(None)):
    """Hard-delete a pohonch record permanently (frees up the pohonch_number)."""
    try:
        result = await _run(delete_pohonch, pohonch_id, user_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.patch("/api/pohonch/{pohonch_id}/edit")
async def pohonch_edit(request: Request, pohonch_id: str = Path(...)):
    """
    Edit an existing pohonch — add/remove bilties and/or rename pohonch_number.

    Body (all fields optional — supply only what you want to change):
    {
      "add_gr_items": [{"gr_no": "22900", "challan_no": "B00020", "pohonch_bilty": "1045"}],
      "remove_gr_nos": ["22789"],
      "new_pohonch_number": "NIE0001",
      "challan_nos": ["B00016", "B00020"],
      "user_id": "uuid",
      "force": false
    }
    Note: Signed pohonch cannot be edited unless force=true is sent.
    """
    try:
        data = await request.json()
        result = await _run(
            edit_pohonch,
            pohonch_id,
            data.get("add_gr_items"),
            data.get("remove_gr_nos"),
            data.get("new_pohonch_number"),
            data.get("challan_nos"),
            data.get("user_id"),
            bool(data.get("force", False)),
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in pohonch_edit: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.patch("/api/pohonch/{pohonch_id}/gr/{gr_no}")
async def pohonch_update_gr(
    request: Request,
    pohonch_id: str = Path(...),
    gr_no: str = Path(...),
):
    """
    Patch any field(s) on a single GR entry inside pohonch.bilty_metadata.
    Recalculates pohonch totals after the update.

    Body (send only fields you want to change):
    {
      "destination":   "LUCKNOW",
      "kaat":          150.0,
      "pf":            350.0,
      "dd":            0,
      "kaat_rate":     2.5,
      "weight":        60.0,
      "packages":      3,
      "amount":        500.0,
      "pohonch_bilty": "12345",
      "e_way_bill":    "3312XXXXXXX",
      "is_paid":       false,
      "payment_mode":  "to-pay",
      "delivery_type": "godown",
      "user_id":       "uuid",
      "force":         false
    }
    """
    try:
        data = await request.json()
        user_id = data.pop("user_id", None)
        force   = bool(data.pop("force", False))
        result = await _run(update_gr_fields, pohonch_id, gr_no, data, user_id, force)
        return _response(result)
    except Exception as e:
        log.exception("Error in pohonch_update_gr: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/pohonch/bulk-recalculate")
async def pohonch_bulk_recalculate(request: Request):
    """
    Recalculate multiple pohonch in one call — re-fetches live data from
    bilty, bilty_wise_kaat, station_bilty_summary, and cities tables.

    Body (supply ONE selector):
    {
      "pohonch_ids":      ["uuid1", "uuid2"],
      "pohonch_numbers":  ["NIE0001", "NIE0002"],
      "transport_gstin":  "09XXXXX",
      "transport_name":   "NEW INDIA EXPRESS",
      "user_id": "uuid",
      "force": false
    }
    """
    try:
        data = await request.json()
        result = await _run(
            bulk_recalculate_pohonch,
            data.get("pohonch_ids"),
            data.get("pohonch_numbers"),
            data.get("transport_gstin"),
            data.get("transport_name"),
            data.get("user_id"),
            bool(data.get("force", False)),
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in pohonch_bulk_recalculate: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/pohonch/{pohonch_id}/recalculate")
async def pohonch_recalculate(request: Request, pohonch_id: str = Path(...)):
    """
    Re-fetch live data for every GR in this pohonch from bilty,
    bilty_wise_kaat, station_bilty_summary and cities tables, then
    rebuild bilty_metadata and recalculate all totals.

    Use this after:
      - Updating kaat rate via /api/kaat/* endpoints
      - Changing weight or freight on a bilty
      - Correcting a destination city

    Body:
    { "user_id": "uuid", "force": false }
    """
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            pass
        result = await _run(
            recalculate_pohonch,
            pohonch_id,
            data.get("user_id"),
            bool(data.get("force", False)),
        )
        return _response(result)
    except Exception as e:
        log.exception("Error in pohonch_recalculate: %s", e)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/pohonch/{pohonch_id}")
async def pohonch_get(pohonch_id: str = Path(...)):
    """Get a single pohonch by UUID."""
    try:
        result = await _run(get_pohonch, pohonch_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ============================================================
# HEALTH CHECK
# ============================================================


@app.get("/api/health")
async def health_check():
    return JSONResponse(
        content={"status": "success", "message": "API is running", "timestamp": datetime.now().isoformat()},
        status_code=200,
    )


# ── Startup ──


# ============================================================
# INVOICE ENDPOINTS
# ============================================================

# ── Skip auth for invoice routes ──────────────────────────────
# (handled in ensure_valid_token middleware — add /api/invoice to skip list)


# ── Tenants ───────────────────────────────────────────────────

@app.get("/api/invoice/tenants")
async def invoice_tenants_list(is_active: Optional[bool] = Query(None)):
    try:
        result = await _run(list_tenants, is_active)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/invoice/tenants/{tenant_id}")
async def invoice_tenant_get(tenant_id: str = Path(...)):
    try:
        result = await _run(get_tenant, tenant_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/invoice/tenants")
async def invoice_tenant_create(request: Request):
    try:
        result = await _run(create_tenant, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/invoice/tenants/{tenant_id}")
async def invoice_tenant_update(request: Request, tenant_id: str = Path(...)):
    try:
        result = await _run(update_tenant, tenant_id, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/invoice/tenants/{tenant_id}")
async def invoice_tenant_delete(tenant_id: str = Path(...)):
    try:
        result = await _run(delete_tenant, tenant_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Inventory (item catalog) ──────────────────────────────────

@app.get("/api/invoice/inventory")
async def invoice_inventory_list(
    tenant_id: str = Query(None),
    is_active: Optional[bool] = Query(None),
):
    try:
        result = await _run(list_inventory, tenant_id, is_active)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/invoice/inventory/{item_id}")
async def invoice_inventory_get(item_id: str = Path(...)):
    try:
        result = await _run(get_inventory_item, item_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/invoice/inventory")
async def invoice_inventory_create(request: Request):
    try:
        result = await _run(create_inventory_item, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/invoice/inventory/{item_id}")
async def invoice_inventory_update(request: Request, item_id: str = Path(...)):
    try:
        result = await _run(update_inventory_item, item_id, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/invoice/inventory/{item_id}")
async def invoice_inventory_delete(item_id: str = Path(...)):
    try:
        result = await _run(delete_inventory_item, item_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Receivers (buyers) ────────────────────────────────────────

@app.get("/api/invoice/receivers")
async def invoice_receivers_list(
    tenant_id: str = Query(None),
    is_active: Optional[bool] = Query(None),
):
    try:
        result = await _run(list_receivers, tenant_id, is_active)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/invoice/receivers/{receiver_id}")
async def invoice_receiver_get(receiver_id: str = Path(...)):
    try:
        result = await _run(get_receiver, receiver_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/invoice/receivers")
async def invoice_receiver_create(request: Request):
    try:
        result = await _run(create_receiver, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/invoice/receivers/{receiver_id}")
async def invoice_receiver_update(request: Request, receiver_id: str = Path(...)):
    try:
        result = await _run(update_receiver, receiver_id, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/invoice/receivers/{receiver_id}")
async def invoice_receiver_delete(receiver_id: str = Path(...)):
    try:
        result = await _run(delete_receiver, receiver_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Invoice Series ────────────────────────────────────────────

@app.get("/api/invoice/series")
async def invoice_series_list(tenant_id: str = Query(None)):
    try:
        result = await _run(list_series, tenant_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/invoice/series")
async def invoice_series_create(request: Request):
    try:
        result = await _run(create_series, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/invoice/series/{series_id}")
async def invoice_series_update(request: Request, series_id: str = Path(...)):
    try:
        result = await _run(update_series, series_id, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/invoice/series/{series_id}")
async def invoice_series_delete(series_id: str = Path(...)):
    try:
        result = await _run(delete_series, series_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Invoices (master + line items) ───────────────────────────

@app.post("/api/invoice/create")
async def invoice_create(request: Request):
    try:
        result = await _run(create_invoice, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/invoice/list")
async def invoice_list(
    tenant_id: str = Query(None),
    receiver_id: str = Query(None),
    status: str = Query(None),
    payment_status: str = Query(None),
    invoice_type: str = Query(None),
    from_date: str = Query(None),
    to_date: str = Query(None),
    gr_no: str = Query(None),
    transport_name: str = Query(None),
    page: int = Query(1),
    page_size: int = Query(50),
):
    try:
        result = await _run(
            list_invoices,
            tenant_id, receiver_id, status, payment_status,
            invoice_type, from_date, to_date, gr_no, transport_name,
            page, page_size,
        )
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/invoice/{invoice_id}")
async def invoice_get(invoice_id: str = Path(...)):
    try:
        result = await _run(get_invoice, invoice_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/invoice/{invoice_id}")
async def invoice_update(request: Request, invoice_id: str = Path(...)):
    try:
        result = await _run(update_invoice, invoice_id, await request.json())
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/invoice/{invoice_id}/cancel")
async def invoice_cancel(request: Request, invoice_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(
            cancel_invoice,
            invoice_id,
            data.get("cancelled_by", ""),
            data.get("cancel_reason"),
        )
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/invoice/{invoice_id}")
async def invoice_delete(invoice_id: str = Path(...)):
    try:
        result = await _run(delete_invoice, invoice_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.put("/api/invoice/{invoice_id}/line-items")
async def invoice_line_items_update(request: Request, invoice_id: str = Path(...)):
    try:
        data = await request.json()
        result = await _run(
            update_line_items,
            invoice_id,
            data.get("line_items", []),
            data.get("supply_type", "INTRA"),
        )
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Invoice Payments ──────────────────────────────────────────

@app.post("/api/invoice/{invoice_id}/payment")
async def invoice_payment_add(request: Request, invoice_id: str = Path(...)):
    try:
        data = await request.json()
        data["invoice_id"] = invoice_id
        result = await _run(add_payment, data)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/invoice/{invoice_id}/payment")
async def invoice_payment_list(invoice_id: str = Path(...)):
    try:
        result = await _run(list_payments, invoice_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.delete("/api/invoice/{invoice_id}/payment/{payment_id}")
async def invoice_payment_delete(
    invoice_id: str = Path(...),
    payment_id: str = Path(...),
):
    try:
        result = await _run(delete_payment, payment_id, invoice_id)
        return _response(result)
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ── Startup ──


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
