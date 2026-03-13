"""
Distance Service
Fetches pincode-to-pincode distance via MastersIndia API
"""
import requests
import json
from auth.auth_service import load_jwt_token

# API Configuration
DISTANCE_URL = "https://prod-api.mastersindia.co/api/v1/distance/"


def get_distance(from_pincode, to_pincode):
    """
    Get distance between two pincodes.

    Args:
        from_pincode (str/int): Origin pincode
        to_pincode (str/int): Destination pincode

    Returns:
        dict: Response with status and distance data
    """
    try:
        print("=" * 70)
        print("📏 FETCHING DISTANCE BETWEEN PINCODES")
        print("=" * 70)

        # Validate pincodes
        from_pin = str(from_pincode).strip()
        to_pin = str(to_pincode).strip()

        if not from_pin or not from_pin.isdigit() or len(from_pin) != 6:
            return {
                "status": "error",
                "message": f"Invalid fromPincode '{from_pincode}'. Must be a 6-digit number.",
                "status_code": 400
            }

        if not to_pin or not to_pin.isdigit() or len(to_pin) != 6:
            return {
                "status": "error",
                "message": f"Invalid toPincode '{to_pincode}'. Must be a 6-digit number.",
                "status_code": 400
            }

        # Load JWT token
        jwt_token = load_jwt_token()
        if not jwt_token:
            return {
                "status": "error",
                "message": "Failed to load JWT token",
                "status_code": 401
            }

        # Prepare headers
        headers = {
            "Authorization": f"JWT {jwt_token}",
            "Content-Type": "application/json"
        }

        # Query parameters
        params = {
            "fromPincode": from_pin,
            "toPincode": to_pin
        }

        print(f"📤 Request URL: {DISTANCE_URL}")
        print(f"📋 Params: fromPincode={from_pin}, toPincode={to_pin}")

        # Make API request
        response = requests.get(
            DISTANCE_URL,
            headers=headers,
            params=params,
            timeout=30
        )

        print(f"📥 Response Status Code: {response.status_code}")

        if response.status_code == 200:
            response_data = response.json()
            results = response_data.get("results", {})
            result_code = results.get("code", 200)
            result_status = results.get("status", "")

            # Error inside 200 response
            if result_code == 204 or result_status == "No Content":
                error_msg = results.get("message", "Unknown error")
                print(f"❌ API Error: {error_msg}")
                return {
                    "status": "error",
                    "message": error_msg,
                    "results": results,
                    "status_code": 204
                }

            # Success
            distance = results.get("distance")
            print(f"✅ Distance: {distance} km")
            return {
                "status": "success",
                "message": f"Distance fetched successfully: {distance} km",
                "results": results,
                "status_code": 200
            }

        else:
            error_text = response.text
            print(f"❌ Error Response: {error_text}")
            try:
                error_data = response.json()
                return {
                    "status": "error",
                    "message": error_data.get("message", "Failed to fetch distance"),
                    "results": error_data.get("results", {}),
                    "status_code": response.status_code
                }
            except Exception:
                return {
                    "status": "error",
                    "message": f"Failed to fetch distance: {error_text}",
                    "status_code": response.status_code
                }

    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return {
            "status": "error",
            "message": "Request timed out",
            "status_code": 408
        }

    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {str(e)}")
        return {
            "status": "error",
            "message": f"Request failed: {str(e)}",
            "status_code": 500
        }

    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "status_code": 500
        }
