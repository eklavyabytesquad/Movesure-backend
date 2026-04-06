"""
Transporter Details Service
Fetches transporter details (trade name, legal name, address, status, etc.)
via the Masters India E-Way Bill API.
"""
import requests
import json
from auth.auth_service import get_auth_headers

# API Configuration
BASE_URL = "https://prod-api.mastersindia.co/api/v1/"
ENDPOINT = "getEwayBillData/"


def get_transporter_details(user_gstin, gstin):
    """
    Get Transporter Details for a given transporter GSTIN.

    Args:
        user_gstin: The logged-in user's GSTIN
        gstin:      The transporter's GSTIN to look up

    Returns:
        dict: Structured response with transporter details or error info
    """
    # Get auth headers
    headers = get_auth_headers()
    if not headers:
        return {
            "status": "error",
            "message": "Failed to get authentication token"
        }

    url = f"{BASE_URL}{ENDPOINT}"
    params = {
        "action": "GetGSTINDetails",
        "userGstin": user_gstin,
        "gstin": gstin,
    }

    try:
        print("=" * 70)
        print("🚚 GETTING TRANSPORTER DETAILS")
        print("=" * 70)
        print(f"📍 URL : {url}")
        print(f"📋 Params: {json.dumps(params, indent=2)}")
        print("=" * 70)

        response = requests.get(url, params=params, headers=headers)

        print(f"📊 Status Code: {response.status_code}")
        print(f"📦 Response   : {response.text[:1000]}")
        print("=" * 70)

        if response.status_code in (200, 201):
            result = response.json()

            api_message = result.get("results", {}).get("message", {})
            api_status  = result.get("results", {}).get("status", "")
            api_code    = result.get("results", {}).get("code", 0)

            # Masters India may return HTTP 200 but with error inside body
            if api_status == "No Content" or api_code == 204:
                nic_code = result.get("results", {}).get("nic_code", "")
                print(f"⚠️ API error inside 200: {api_message}")
                return {
                    "status": "error",
                    "message": api_message if isinstance(api_message, str) else "Could not retrieve transporter details",
                    "nic_code": nic_code,
                    "data": result,
                }

            # Success — extract flat fields
            details = api_message if isinstance(api_message, dict) else {}
            print(f"✅ Transporter details retrieved for {details.get('gstin_of_taxpayer', gstin)}")

            return {
                "status": "success",
                "message": "Transporter details retrieved successfully",
                "gstin_of_taxpayer": details.get("gstin_of_taxpayer", ""),
                "trade_name": details.get("trade_name", ""),
                "legal_name_of_business": details.get("legal_name_of_business", ""),
                "address1": details.get("address1", ""),
                "address2": details.get("address2", ""),
                "state_name": details.get("state_name", ""),
                "pincode": details.get("pincode", ""),
                "taxpayer_type": details.get("taxpayer_type", ""),
                "taxpayer_status": details.get("status", ""),
                "block_status": details.get("block_status", ""),
                "data": result,
            }
        else:
            error_data = response.json() if response.text else {"error": "Unknown error"}
            print(f"❌ Failed! Status {response.status_code}")
            print(f"Error: {json.dumps(error_data, indent=2)}")
            return {
                "status": "error",
                "message": "Failed to retrieve transporter details",
                "error": error_data,
                "status_code": response.status_code,
            }

    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return {
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }
