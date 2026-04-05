"""
FastAPI Backend for E-Way Bill Management & Bilty Operations
"""
from fastapi import FastAPI, Request, Query, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Import service modules
from auth.auth_service import get_jwt_token, load_jwt_token
from services.ewaybill_service import get_ewaybill_details
from services.consolidated_ewaybill_service import create_consolidated_ewaybill
from services.transporter_id_service import update_transporter_id
from services.transporter_update_with_pdf_service import update_transporter_and_get_pdf
from services.extend_ewaybill_service import extend_ewaybill_validity
from services.distance_service import get_distance
from services.gstin_details_service import get_gstin_details
from services.transporter_details_service import get_transporter_details
from services.generate_ewaybill_service import generate_ewaybill
from services.reference_data_service import get_reference_data
from services.bilty_save_service import save_bilty, get_bilty_with_cities
from services.consignor_rates_service import get_consignor_rates, get_default_rates, get_all_rates


@asynccontextmanager
async def lifespan(app):
    # Startup
    print("=" * 70)
    print("🚀 STARTING E-WAY BILL API SERVER (FastAPI)")
    print("=" * 70)
    token = load_jwt_token()
    if token:
        print("✅ JWT Token loaded successfully")
    else:
        print("⚠️ Getting new JWT token...")
        token = get_jwt_token()
        if token:
            print("✅ JWT Token obtained successfully")
        else:
            print("❌ Failed to get JWT token. Server may not work properly.")
    print("=" * 70)
    print("📡 Server running at: http://localhost:5000")
    print("📋 Available Endpoints:")
    print("   - GET  /api/health")
    print("   - GET  /api/ewaybill?eway_bill_number=XXX&gstin=YYY")
    print("   - POST /api/consolidated-ewaybill")
    print("   - POST /api/transporter-update")
    print("   - POST /api/transporter-update-with-pdf (2 API calls)")
    print("   - POST /api/extend-ewaybill")
    print("   - GET  /api/distance?fromPincode=XXX&toPincode=YYY")
    print("   - GET  /api/gstin-details?userGstin=XXX&gstin=YYY")
    print("   - GET  /api/transporter-details?userGstin=XXX&gstin=YYY")
    print("   - POST /api/generate-ewaybill")
    print("   - POST /api/refresh-token")
    print("   - GET  /api/bilty/reference-data?branch_id=XXX&user_id=YYY")
    print("   - POST /api/bilty/save")
    print("   - GET  /api/bilty/{bilty_id}")
    print("   - GET  /api/bilty/rates/consignor/{consignor_id}")
    print("   - GET  /api/bilty/rates/default?branch_id=XXX")
    print("   - GET  /api/bilty/rates/all?consignor_id=XXX&branch_id=YYY")
    print("=" * 70)
    print("💡 Token auto-refresh enabled - Server will run continuously!")
    print("=" * 70)
    yield
    # Shutdown
    _executor.shutdown(wait=False)


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
_executor = ThreadPoolExecutor(max_workers=8)


async def _run(func, *args):
    """Run a blocking function in the thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, func, *args)


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
    if path in SKIP_AUTH_PATHS or path.startswith("/api/bilty"):
        return await call_next(request)

    print(f"🔍 Validating token for request: {request.method} {path}")
    token = load_jwt_token()
    if not token:
        print("⚠️ Token validation failed, attempting to refresh...")
        token = get_jwt_token()
        if not token:
            print("❌ Failed to obtain valid token")
            return JSONResponse(
                content={"status": "error", "message": "Authentication failed. Unable to obtain valid JWT token."},
                status_code=503,
            )
        print("✅ Successfully obtained new token")
    else:
        print("✅ Token validated successfully")

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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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
        print(f"❌ Exception occurred: {str(e)}")
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


@app.get("/api/bilty/{bilty_id}")
async def bilty_get(bilty_id: str = Path(...)):
    try:
        result = await _run(get_bilty_with_cities, bilty_id)
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
