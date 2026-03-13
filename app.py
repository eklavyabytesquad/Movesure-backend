"""
Flask API for E-Way Bill Management
Handles authentication, e-way bill retrieval, and consolidated e-way bill creation
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

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

# Flask App Configuration
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.before_request
def ensure_valid_token():
    """
    Middleware to ensure JWT token is valid before processing any request
    Automatically refreshes token if expired
    """
    # Skip token check for health endpoint and refresh-token endpoint
    if request.path in ['/api/health', '/api/refresh-token']:
        return None
    
    print(f"🔍 Validating token for request: {request.method} {request.path}")
    
    # Check and refresh token if needed
    token = load_jwt_token()
    if not token:
        print("⚠️ Token validation failed, attempting to refresh...")
        token = get_jwt_token()
        if not token:
            print("❌ Failed to obtain valid token")
            return jsonify({
                "status": "error",
                "message": "Authentication failed. Unable to obtain valid JWT token."
            }), 503
        else:
            print("✅ Successfully obtained new token")
    else:
        print("✅ Token validated successfully")

@app.route('/api/ewaybill', methods=['GET'])
def get_ewaybill():
    """
    API endpoint to get e-way bill details
    Query Parameters:
        - eway_bill_number: E-way bill number
        - gstin: GSTIN number
    """
    try:
        # Get query parameters
        eway_bill_number = request.args.get('eway_bill_number')
        gstin = request.args.get('gstin')
        
        # Validate required parameters
        if not eway_bill_number or not gstin:
            return jsonify({
                "status": "error",
                "message": "Missing required parameters: eway_bill_number and gstin"
            }), 400
        
        # Call service function
        result = get_ewaybill_details(eway_bill_number, gstin)
        
        # Return response
        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/consolidated-ewaybill', methods=['POST'])
def consolidated_ewaybill_endpoint():
    """
    API endpoint to create consolidated e-way bill
    Accepts JSON payload and uses JWT token from jwt_token.json
    """
    try:
        # Get request data
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "No data provided"
            }), 400
        
        # Call service function
        result = create_consolidated_ewaybill(data)
        
        # Return response
        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/refresh-token', methods=['POST'])
def refresh_token():
    """
    API endpoint to refresh JWT token
    """
    try:
        token = get_jwt_token()
        if token:
            return jsonify({
                "status": "success",
                "message": "JWT token refreshed successfully",
                "token": token
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to refresh JWT token"
            }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error: {str(e)}"
        }), 500

@app.route('/api/transporter-update', methods=['POST'])
def transporter_update():
    """
    API endpoint to update transporter ID for an e-way bill
    Accepts JSON payload:
    {
        "user_gstin": "05AAABB0639G1Z8",
        "eway_bill_number": "321009218808",
        "transporter_id": "05AAAAU6537D1ZO",
        "transporter_name": "MS Uttarayan"
    }
    """
    try:
        # Get request data
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "No data provided"
            }), 400
        
        # Validate required fields
        required_fields = ['user_gstin', 'eway_bill_number', 'transporter_id', 'transporter_name']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            return jsonify({
                "status": "error",
                "message": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        # Extract parameters
        user_gstin = data.get('user_gstin')
        eway_bill_number = data.get('eway_bill_number')
        transporter_id = data.get('transporter_id')
        transporter_name = data.get('transporter_name')
        
        # Call service function
        result = update_transporter_id(
            user_gstin=user_gstin,
            eway_bill_number=eway_bill_number,
            transporter_id=transporter_id,
            transporter_name=transporter_name
        )
        
        # Return response
        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/transporter-update-with-pdf', methods=['POST'])
def transporter_update_with_pdf():
    """
    API endpoint to update transporter ID and retrieve PDF
    Makes two API calls as per MastersGST support instructions:
    1. First call: Updates the transporter
    2. Second call: Retrieves the PDF
    
    Accepts JSON payload:
    {
        "user_gstin": "09AAACA2669Q1Z4",
        "eway_bill_number": "481646922017",
        "transporter_id": "09COVPS5556J1ZT",
        "transporter_name": "S S TRANSPORT CORPORATION"
    }
    """
    try:
        # Get request data
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "No data provided"
            }), 400
        
        # Validate required fields
        required_fields = ['user_gstin', 'eway_bill_number', 'transporter_id', 'transporter_name']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            return jsonify({
                "status": "error",
                "message": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        # Extract parameters
        user_gstin = data.get('user_gstin')
        eway_bill_number = data.get('eway_bill_number')
        transporter_id = data.get('transporter_id')
        transporter_name = data.get('transporter_name')
        
        # Call service function (makes 2 API calls)
        result = update_transporter_and_get_pdf(
            user_gstin=user_gstin,
            eway_bill_number=eway_bill_number,
            transporter_id=transporter_id,
            transporter_name=transporter_name
        )
        
        # Return response
        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/extend-ewaybill', methods=['POST'])
def extend_ewaybill():
    """
    API endpoint to extend the validity of an e-way bill.
    
    Accepts JSON payload:
    {
        "userGstin": "05AAABB0639G1Z8",
        "eway_bill_number": 311003430463,
        "vehicle_number": "KA12TR1234",
        "place_of_consignor": "Dehradun",
        "state_of_consignor": "UTTARAKHAND",
        "remaining_distance": 10,
        "transporter_document_number": "123",
        "transporter_document_date": "25/06/2023",
        "mode_of_transport": "5",
        "extend_validity_reason": "Natural Calamity",
        "extend_remarks": "Flood",
        "consignment_status": "T",
        "from_pincode": 248001,
        "transit_type": "W",
        "address_line1": "HUBLI",
        "address_line2": "HUBLI",
        "address_line3": "HUBLI"
    }

    mode_of_transport: 1=Road, 2=Rail, 3=Air, 4=Ship, 5=In Transit
    consignment_status: M (modes 1-4), T (mode 5)
    transit_type: R/W/O (only when mode_of_transport=5, blank otherwise)
    """
    try:
        # Get request data
        data = request.get_json()

        if not data:
            return jsonify({
                "status": "error",
                "message": "No data provided"
            }), 400

        # Call service function
        result = extend_ewaybill_validity(data)

        # Return response
        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/distance', methods=['GET'])
def distance():
    """
    API endpoint to get distance between two pincodes.
    Query Parameters:
        - fromPincode: Origin pincode (6 digits)
        - toPincode: Destination pincode (6 digits)
    """
    try:
        from_pincode = request.args.get('fromPincode')
        to_pincode = request.args.get('toPincode')

        if not from_pincode or not to_pincode:
            return jsonify({
                "status": "error",
                "message": "Missing required query parameters: fromPincode and toPincode"
            }), 400

        result = get_distance(from_pincode, to_pincode)

        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code

    except Exception as e:
        print(f"\u274c Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/gstin-details', methods=['GET'])
def gstin_details():
    """
    API endpoint to get GSTIN details.
    Query Parameters:
        - userGstin: Logged-in user's GSTIN
        - gstin: GSTIN to look up
    """
    try:
        user_gstin = request.args.get('userGstin')
        gstin = request.args.get('gstin')

        if not user_gstin or not gstin:
            return jsonify({
                "status": "error",
                "message": "Missing required query parameters: userGstin and gstin"
            }), 400

        result = get_gstin_details(user_gstin, gstin)

        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code

    except Exception as e:
        print(f"\u274c Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/transporter-details', methods=['GET'])
def transporter_details():
    """
    API endpoint to get transporter details.
    Query Parameters:
        - userGstin: Logged-in user's GSTIN
        - gstin: Transporter GSTIN to look up
    """
    try:
        user_gstin = request.args.get('userGstin')
        gstin = request.args.get('gstin')

        if not user_gstin or not gstin:
            return jsonify({
                "status": "error",
                "message": "Missing required query parameters: userGstin and gstin"
            }), 400

        result = get_transporter_details(user_gstin, gstin)

        if result.get("status") == "success":
            return jsonify(result), 200
        else:
            status_code = result.get("status_code", 500)
            return jsonify(result), status_code

    except Exception as e:
        print(f"\u274c Exception occurred: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """
    Health check endpoint
    """
    return jsonify({
        "status": "success",
        "message": "API is running",
        "timestamp": datetime.now().isoformat()
    }), 200

if __name__ == "__main__":
    # Ensure JWT token exists before starting server
    print("=" * 70)
    print("🚀 STARTING E-WAY BILL API SERVER")
    print("=" * 70)
    
    # Check and refresh token if needed
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
    print(f"📡 Server running at: http://localhost:5000")
    print(f"📋 Available Endpoints:")
    print(f"   - GET  /api/health")
    print(f"   - GET  /api/ewaybill?eway_bill_number=XXX&gstin=YYY")
    print(f"   - POST /api/consolidated-ewaybill")
    print(f"   - POST /api/transporter-update")
    print(f"   - POST /api/transporter-update-with-pdf (2 API calls)")
    print(f"   - POST /api/extend-ewaybill")
    print(f"   - GET  /api/distance?fromPincode=XXX&toPincode=YYY")
    print(f"   - GET  /api/gstin-details?userGstin=XXX&gstin=YYY")
    print(f"   - GET  /api/transporter-details?userGstin=XXX&gstin=YYY")
    print(f"   - POST /api/refresh-token")
    print("=" * 70)
    print("💡 Token auto-refresh enabled - Server will run continuously!")
    print("=" * 70)
    
    # Run Flask server
    app.run(debug=True, host='0.0.0.0', port=5000)
