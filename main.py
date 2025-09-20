from flask import Flask, jsonify, request
from masters_auth import authenticate, get_token, force_refresh_token
from services import (
    get_eway_bill_data_service, 
    generate_eway_bill_service, 
    start_background_services, 
    stop_services,
    get_service_status
)
from templates import get_eway_bill_template
from datetime import datetime
import logging

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('movesure_api.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# API Routes
@app.route('/api/v1/getewaybilldata/<eway_bill_number>', methods=['GET'])
def get_eway_bill_api(eway_bill_number):
    """API endpoint to get E-way bill data"""
    try:
        gstin = request.args.get('gstin', '05AAABB0639G1Z8')
        logger.info(f"API Request: Get E-way Bill {eway_bill_number} for GSTIN {gstin}")
        
        result = get_eway_bill_data_service(eway_bill_number, gstin)
        
        if result["success"]:
            logger.info(f"E-way Bill {eway_bill_number} retrieved successfully")
            return jsonify({
                "status": "success",
                "eway_bill_number": eway_bill_number,
                "gstin": gstin,
                "data": result["data"]
            })
        else:
            logger.error(f"Failed to retrieve E-way Bill {eway_bill_number}: {result['error']}")
            return jsonify({
                "status": "error",
                "eway_bill_number": eway_bill_number,
                "error": result["error"]
            }), 400
            
    except Exception as e:
        logger.error(f"API Error: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/api/v1/generateewaybill', methods=['POST'])
def generate_eway_bill_api():
    """API endpoint to generate E-way bill"""
    try:
        eway_bill_data = request.get_json()
        
        if not eway_bill_data:
            return jsonify({
                "status": "error",
                "error": "No data provided"
            }), 400
        
        logger.info(f"API Request: Generate E-way Bill for document {eway_bill_data.get('document_number', 'Unknown')}")
        
        result = generate_eway_bill_service(eway_bill_data)
        
        if result["success"]:
            response_data = result["data"]
            eway_bill_number = None
            
            # Extract E-way bill number from response
            if "results" in response_data and "message" in response_data["results"]:
                message = response_data["results"]["message"]
                if isinstance(message, dict) and "ewayBillNo" in message:
                    eway_bill_number = message["ewayBillNo"]
            
            logger.info(f"E-way Bill generated successfully: {eway_bill_number}")
            
            return jsonify({
                "status": "success",
                "eway_bill_number": eway_bill_number,
                "document_number": eway_bill_data.get('document_number'),
                "data": response_data
            })
        else:
            logger.error(f"Failed to generate E-way Bill: {result['error']}")
            return jsonify({
                "status": "error",
                "error": result["error"]
            }), 400
            
    except Exception as e:
        logger.error(f"API Error: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/api/v1/generateewaybill/template', methods=['GET'])
def get_eway_bill_template_api():
    """API endpoint to get E-way bill template"""
    template = get_eway_bill_template()
    
    return jsonify({
        "status": "success",
        "message": "E-way bill template",
        "template": template
    })

@app.route('/api/v1/auth/status', methods=['GET'])
def auth_status():
    """API endpoint to check authentication status"""
    try:
        token = get_token()
        service_status = get_service_status()
        return jsonify({
            "status": "success",
            "authenticated": bool(token),
            "last_refresh": service_status["last_token_refresh"].isoformat() if service_status["last_token_refresh"] else None,
            "service_running": service_status["service_running"]
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/api/v1/auth/refresh', methods=['POST'])
def refresh_auth():
    """API endpoint to manually refresh authentication token"""
    try:
        new_token = force_refresh_token()
        if new_token:
            return jsonify({
                "status": "success",
                "message": "Token refreshed successfully",
                "refreshed_at": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "status": "error",
                "error": "Failed to refresh token"
            }), 400
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    """API endpoint for health check"""
    service_status = get_service_status()
    return jsonify({
        "status": "success",
        "service": "MoveSure E-way Bill API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "authentication": {
            "service_running": service_status["service_running"],
            "last_refresh": service_status["last_token_refresh"].isoformat() if service_status["last_token_refresh"] else None
        }
    })

@app.route('/api/v1/endpoints', methods=['GET'])
def list_endpoints():
    """API endpoint to list all available endpoints"""
    return jsonify({
        "status": "success",
        "endpoints": {
            "get_eway_bill": {
                "method": "GET",
                "url": "/api/v1/getewaybilldata/<eway_bill_number>",
                "description": "Get E-way bill data by number"
            },
            "generate_eway_bill": {
                "method": "POST",
                "url": "/api/v1/generateewaybill",
                "description": "Generate new E-way bill"
            },
            "get_template": {
                "method": "GET",
                "url": "/api/v1/generateewaybill/template",
                "description": "Get E-way bill template"
            },
            "auth_status": {
                "method": "GET",
                "url": "/api/v1/auth/status",
                "description": "Check authentication status"
            },
            "refresh_auth": {
                "method": "POST",
                "url": "/api/v1/auth/refresh",
                "description": "Manually refresh authentication token"
            },
            "health_check": {
                "method": "GET",
                "url": "/api/v1/health",
                "description": "Health check endpoint"
            },
            "list_endpoints": {
                "method": "GET",
                "url": "/api/v1/endpoints",
                "description": "List all available endpoints"
            }
        }
    })

if __name__ == "__main__":
    try:
        # Initial authentication
        logger.info("Starting MoveSure E-way Bill API v1.0.0")
        logger.info("Performing initial authentication")
        
        headers = authenticate()
        logger.info("Initial authentication successful")
        
        # Start background services
        start_background_services()
        
        logger.info("MoveSure API Server starting on port 5000")
        logger.info("Available endpoints: /api/v1/endpoints")
        
        # Start Flask app in production mode
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False,
            threaded=True
        )
        
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        stop_services()
        
    except Exception as e:
        logger.error(f"Server error: {e}")
        stop_services()
        
    finally:
        stop_services()
        logger.info("MoveSure API Server stopped")