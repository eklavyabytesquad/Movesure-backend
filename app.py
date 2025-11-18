"""
Flask API for E-Way Bill Management
Handles authentication, e-way bill retrieval, and consolidated e-way bill creation
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

# Import service modules
from auth_service import get_jwt_token, load_jwt_token
from ewaybill_service import get_ewaybill_details
from consolidated_ewaybill_service import create_consolidated_ewaybill
from transporter_id_service import update_transporter_id
from transporter_update_with_pdf_service import update_transporter_and_get_pdf

# Flask App Configuration
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.before_request
def ensure_valid_token():
    """
    Middleware to ensure JWT token is valid before processing any request
    Automatically refreshes token if expired
    """
    # Skip token check for health endpoint
    if request.path == '/api/health':
        return None
    
    # Check and refresh token if needed
    token = load_jwt_token()
    if not token:
        print("⚠️ Token validation failed, attempting to refresh...")
        token = get_jwt_token()
        if not token:
            return jsonify({
                "status": "error",
                "message": "Authentication failed. Unable to obtain valid JWT token."
            }), 503

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
    print(f"   - POST /api/refresh-token")
    print("=" * 70)
    print("💡 Token auto-refresh enabled - Server will run continuously!")
    print("=" * 70)
    
    # Run Flask server
    app.run(debug=True, host='0.0.0.0', port=5000)
