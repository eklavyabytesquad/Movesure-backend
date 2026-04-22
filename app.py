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
from services.ewaybill.generate_ewaybill_service import generate_ewaybill
from services.bilty.reference_data_service import get_reference_data
from services.bilty.bilty_save_service import save_bilty, get_bilty_with_cities
from services.bilty.consignor_rates_service import get_consignor_rates, get_default_rates, get_all_rates
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

# Thread pool for running blocking service calls without blocking the event loop
# Sized to handle concurrent requests — services reuse this pool instead of creating their own
_executor = ThreadPoolExecutor(max_workers=20)

# Semaphore limits concurrent blocking tasks — returns 503 immediately instead of hanging
_semaphore = asyncio.Semaphore(16)


async def _run(func, *args):
    """Run a blocking function in the thread pool with backpressure."""
    if _semaphore.locked():
        # All slots busy — fail fast instead of queueing and timing out
        raise _OverloadError()
    async with _semaphore:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_executor, func, *args)


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
    if path in SKIP_AUTH_PATHS or path.startswith("/api/bilty") or path.startswith("/api/challan"):
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
# HEALTH CHECK
# ============================================================


@app.get("/api/health")
async def health_check():
    return JSONResponse(
        content={"status": "success", "message": "API is running", "timestamp": datetime.now().isoformat()},
        status_code=200,
    )


# ── Startup ──


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
