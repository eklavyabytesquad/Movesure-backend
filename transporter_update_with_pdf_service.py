"""
Transporter ID Update Service with PDF Retrieval
Handles transporter ID updates and subsequent PDF retrieval
"""
import requests
import json
import base64
from auth_service import load_jwt_token

# API Configuration
TRANSPORTER_UPDATE_URL = "https://prod-api.mastersindia.co/api/v1/transporterIdUpdate/"

def update_transporter_and_get_pdf(user_gstin, eway_bill_number, transporter_id, transporter_name):
    """
    Update transporter ID and retrieve the PDF in two API calls
    
    Args:
        user_gstin (str): User's GSTIN number
        eway_bill_number (str/int): E-way bill number
        transporter_id (str): New transporter ID (GSTIN)
        transporter_name (str): Transporter name
    
    Returns:
        dict: Response with status, update result, and PDF data
    """
    try:
        print("=" * 70)
        print("🔄 STEP 1: UPDATING TRANSPORTER ID")
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
        
        # FIRST API CALL - Update transporter
        response1 = requests.post(
            TRANSPORTER_UPDATE_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"📥 First Response Status Code: {response1.status_code}")
        
        if response1.status_code != 200:
            error_text = response1.text
            print(f"❌ First call failed: {error_text}")
            return {
                "status": "error",
                "message": f"Failed to update transporter: {error_text}",
                "status_code": response1.status_code
            }
        
        response1_data = response1.json()
        print(f"✅ First Response: {json.dumps(response1_data, indent=2)}")
        
        # Check if first call was successful
        results1 = response1_data.get("results", {})
        if results1.get("code") == 204:
            return {
                "status": "error",
                "message": results1.get("message", "Update failed"),
                "status_code": 204
            }
        
        # SECOND API CALL - Get PDF (as per MastersGST support)
        print("\n" + "=" * 70)
        print("📄 STEP 2: RETRIEVING PDF")
        print("=" * 70)
        print(f"📤 Request URL: {TRANSPORTER_UPDATE_URL}")
        print(f"📋 Payload: {json.dumps(payload, indent=2)}")
        
        response2 = requests.post(
            TRANSPORTER_UPDATE_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"📥 Second Response Status Code: {response2.status_code}")
        
        if response2.status_code == 200:
            response2_data = response2.json()
            print(f"✅ Second Response received")
            
            # Check if response contains PDF data
            results2 = response2_data.get("results", {})
            message2 = results2.get("message", {})
            
            # Save both responses for analysis
            complete_result = {
                "first_call": {
                    "status_code": response1.status_code,
                    "data": response1_data
                },
                "second_call": {
                    "status_code": response2.status_code,
                    "data": response2_data
                }
            }
            
            with open("transporter_update_with_pdf_result.json", "w") as f:
                json.dump(complete_result, f, indent=2)
            print("💾 Complete result saved to: transporter_update_with_pdf_result.json")
            
            # Check if PDF is in response
            if "pdf" in str(response2_data).lower() or "base64" in str(response2_data).lower():
                print("✅ PDF data found in response!")
                return {
                    "status": "success",
                    "message": "Transporter updated and PDF retrieved",
                    "update_result": response1_data,
                    "pdf_result": response2_data,
                    "status_code": 200
                }
            else:
                print("⚠️ No PDF data found in second response")
                return {
                    "status": "success",
                    "message": "Transporter updated but no PDF in response",
                    "update_result": response1_data,
                    "second_call_result": response2_data,
                    "note": "Check transporter_update_with_pdf_result.json for full response",
                    "status_code": 200
                }
        else:
            error_text = response2.text
            print(f"❌ Second call failed: {error_text}")
            return {
                "status": "partial_success",
                "message": "Transporter updated but PDF retrieval failed",
                "update_result": response1_data,
                "pdf_error": error_text,
                "status_code": response2.status_code
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
