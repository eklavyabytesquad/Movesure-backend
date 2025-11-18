"""
Transporter ID Update Service
Handles transporter ID updates for e-way bills
"""
import requests
import json
from auth_service import load_jwt_token

# API Configuration
TRANSPORTER_UPDATE_URL = "https://prod-api.mastersindia.co/api/v1/transporterIdUpdate/"

def update_transporter_id(user_gstin, eway_bill_number, transporter_id, transporter_name):
    """
    Update transporter ID for an e-way bill
    
    Args:
        user_gstin (str): User's GSTIN number
        eway_bill_number (str/int): E-way bill number
        transporter_id (str): New transporter ID (GSTIN)
        transporter_name (str): Transporter name
    
    Returns:
        dict: Response with status and results
    """
    try:
        print("=" * 70)
        print("🔄 UPDATING TRANSPORTER ID")
        print("=" * 70)
        
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
        
        # Prepare payload
        payload = {
            "userGstin": user_gstin,
            "eway_bill_number": int(eway_bill_number),
            "transporter_id": transporter_id,
            "transporter_name": transporter_name
        }
        
        print(f"📤 Request URL: {TRANSPORTER_UPDATE_URL}")
        print(f"📋 Payload: {json.dumps(payload, indent=2)}")
        
        # Make API request
        response = requests.post(
            TRANSPORTER_UPDATE_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"📥 Response Status Code: {response.status_code}")
        
        # Parse response
        if response.status_code == 200:
            response_data = response.json()
            
            # Check if the response contains an error inside results
            results = response_data.get("results", {})
            result_code = results.get("code", 200)
            result_status = results.get("status", "")
            
            # Check for error conditions in the response body
            if result_code == 204 or result_status == "No Content":
                error_message = results.get("message", "Unknown error")
                print(f"❌ API Error (Code {result_code}): {error_message}")
                
                return {
                    "status": "error",
                    "message": error_message,
                    "results": results,
                    "status_code": 204
                }
            
            # Check if the message field contains an error
            message_field = results.get("message", {})
            if isinstance(message_field, dict):
                if message_field.get("error", False):
                    error_message = "Operation failed"
                    print(f"❌ Operation Error: {error_message}")
                    
                    return {
                        "status": "error",
                        "message": error_message,
                        "results": results,
                        "status_code": 400
                    }
                else:
                    # Success case
                    print(f"✅ Success: {json.dumps(response_data, indent=2)}")
                    
                    return {
                        "status": "success",
                        "message": "Transporter ID updated successfully",
                        "results": results,
                        "status_code": 200
                    }
            else:
                # If message is a string and contains "Error:"
                if isinstance(message_field, str) and "Error:" in message_field:
                    print(f"❌ API Error: {message_field}")
                    
                    return {
                        "status": "error",
                        "message": message_field,
                        "results": results,
                        "status_code": 400
                    }
                else:
                    print(f"✅ Success: {json.dumps(response_data, indent=2)}")
                    
                    return {
                        "status": "success",
                        "message": "Transporter ID updated successfully",
                        "results": results,
                        "status_code": 200
                    }
        
        elif response.status_code == 204:
            response_data = response.json()
            error_message = response_data.get("results", {}).get("message", "Unknown error")
            print(f"⚠️ No Content: {error_message}")
            
            return {
                "status": "error",
                "message": error_message,
                "results": response_data.get("results", {}),
                "status_code": 204
            }
        
        else:
            error_text = response.text
            print(f"❌ Error Response: {error_text}")
            
            try:
                error_data = response.json()
                return {
                    "status": "error",
                    "message": error_data.get("message", "Failed to update transporter ID"),
                    "results": error_data.get("results", {}),
                    "status_code": response.status_code
                }
            except:
                return {
                    "status": "error",
                    "message": f"Failed to update transporter ID: {error_text}",
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
