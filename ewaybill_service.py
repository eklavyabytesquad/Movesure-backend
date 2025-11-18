"""
E-Way Bill Service
Handles e-way bill operations
"""
import requests
import json
from auth_service import get_auth_headers

# API Configuration
BASE_URL = "https://prod-api.mastersindia.co/api/v1/"
EWB_ENDPOINT = "getEwayBillData/"

def get_ewaybill_details(eway_bill_number, gstin):
    """
    Get E-Way Bill Details using the JWT token
    
    Args:
        eway_bill_number: E-way bill number
        gstin: GSTIN number
        
    Returns:
        dict: Response data or None if failed
    """
    # Get auth headers
    headers = get_auth_headers()
    if not headers:
        return {
            "status": "error",
            "message": "Failed to get authentication token"
        }
    
    # Prepare URL with query parameters
    url = f"{BASE_URL}{EWB_ENDPOINT}"
    params = {
        "action": "GetEwayBill",
        "gstin": gstin,
        "eway_bill_number": eway_bill_number
    }
    
    try:
        print("=" * 70)
        print("🚛 GETTING E-WAY BILL DETAILS - REQUEST PAYLOAD")
        print("=" * 70)
        print(f"📍 Full URL: {url}")
        print(f"📋 Method: GET")
        print(f"📦 Query Parameters:")
        print(f"   - action: {params['action']}")
        print(f"   - gstin: {params['gstin']}")
        print(f"   - eway_bill_number: {params['eway_bill_number']}")
        print(f"🔐 Headers:")
        print(f"   - Content-Type: {headers.get('Content-Type')}")
        print(f"   - Authorization: JWT {headers.get('Authorization', '').replace('JWT ', '')[:50]}...")
        print(f"🌐 Complete Request URL with params:")
        print(f"   {url}?action={params['action']}&gstin={params['gstin']}&eway_bill_number={params['eway_bill_number']}")
        print("=" * 70)
        
        response = requests.get(url, params=params, headers=headers)
        
        print("=" * 70)
        print("📥 API RESPONSE")
        print("=" * 70)
        print(f"📊 Status Code: {response.status_code}")
        print(f"📋 Response Body:")
        
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))
            print("=" * 70)
            print("✅ E-Way Bill Details retrieved successfully!")
            
            # Save response to file
            with open("ewaybill_response.json", "w") as f:
                json.dump(data, f, indent=2)
            
            return {
                "status": "success",
                "message": "E-Way Bill details retrieved successfully",
                "data": data
            }
        else:
            print(f"\n❌ FAILED TO GET E-WAY BILL DETAILS!")
            print(f"   - Status Code: {response.status_code}")
            error_data = response.json() if response.text else {"error": "Unknown error"}
            print(f"\n📦 ERROR RESPONSE PAYLOAD:")
            print(json.dumps(error_data, indent=2))
            print("=" * 80)
            
            # Save error details for support team
            error_log = {
                "request": {
                    "method": "GET",
                    "url": url,
                    "full_url_with_params": f"{url}?action={params['action']}&gstin={params['gstin']}&eway_bill_number={params['eway_bill_number']}",
                    "headers": {
                        "Content-Type": headers.get("Content-Type"),
                        "Authorization": "JWT <token_hidden_for_security>"
                    },
                    "query_params": params
                },
                "response": {
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "error_data": error_data
                }
            }
            
            with open("ewaybill_error_log.json", "w") as f:
                json.dump(error_log, f, indent=2)
            print(f"💾 Error details saved to: ewaybill_error_log.json")
            
            return {
                "status": "error",
                "message": "Failed to retrieve E-Way Bill details",
                "error": error_data,
                "status_code": response.status_code
            }
            
    except Exception as e:
        print(f"❌ Request failed: {str(e)}")
        return {
            "status": "error",
            "message": f"Exception occurred: {str(e)}"
        }
