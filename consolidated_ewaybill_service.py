"""
Consolidated E-Way Bill Service
Handles consolidated e-way bill operations
"""
import requests
import json
from auth_service import get_auth_headers

# API Configuration
CONSOLIDATED_EWB_URL = "https://prod-api.mastersindia.co/api/v1/consolidatedEwayBillsGenerate/"

def create_consolidated_ewaybill(data):
    """
    Create Consolidated E-Way Bill
    
    Args:
        data: Dictionary containing consolidated e-way bill data
        
    Returns:
        dict: Response data
    """
    # Validate required fields
    required_fields = [
        'userGstin', 'place_of_consignor', 'state_of_consignor', 
        'vehicle_number', 'mode_of_transport', 'transporter_document_number',
        'transporter_document_date', 'data_source', 'list_of_eway_bills'
    ]
    
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return {
            "status": "error",
            "message": f"Missing required fields: {', '.join(missing_fields)}"
        }
    
    # Transform list_of_eway_bills if it's an array of strings
    # API expects: [{"eway_bill_number": "123"}, {"eway_bill_number": "456"}]
    if data['list_of_eway_bills'] and isinstance(data['list_of_eway_bills'], list):
        # Check if it's already in correct format
        if len(data['list_of_eway_bills']) > 0 and isinstance(data['list_of_eway_bills'][0], str):
            # Transform from ["123", "456"] to [{"eway_bill_number": "123"}, {"eway_bill_number": "456"}]
            data['list_of_eway_bills'] = [
                {"eway_bill_number": str(ewb).strip()} 
                for ewb in data['list_of_eway_bills'] 
                if ewb and str(ewb).strip()
            ]
            print(f"📝 Transformed {len(data['list_of_eway_bills'])} e-way bills to correct format")
    
    # Get auth headers
    headers = get_auth_headers()
    if not headers:
        return {
            "status": "error",
            "message": "Failed to get authentication token"
        }
    
    try:
        print("🚛 Creating Consolidated E-Way Bill...")
        print(f"URL: {CONSOLIDATED_EWB_URL}")
        print(f"Payload: {json.dumps(data, indent=2)}")
        
        response = requests.post(CONSOLIDATED_EWB_URL, json=data, headers=headers)
        
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            print("✅ Consolidated E-Way Bill created successfully!")
            
            # Save response to file
            with open("consolidated_ewaybill_response.json", "w") as f:
                json.dump(result, f, indent=2)
            
            return {
                "status": "success",
                "message": "Consolidated E-Way Bill created successfully",
                "data": result
            }
        else:
            error_data = response.json() if response.text else {"error": "Unknown error"}
            print(f"❌ Failed to create consolidated e-way bill!")
            print(f"Status Code: {response.status_code}")
            print(f"Error: {json.dumps(error_data, indent=2)}")
            
            return {
                "status": "error",
                "message": "Failed to create consolidated e-way bill",
                "error": error_data,
                "status_code": response.status_code
            }
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return {
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }
