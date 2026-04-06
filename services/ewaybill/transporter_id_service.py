"""
Transporter ID Update Service
Handles transporter ID updates for e-way bills
"""
import requests
import json
import re
from auth.auth_service import load_jwt_token


def _parse_nic_error(error_message):
    """
    Parse NIC error strings like "338: You cannot update transporter details..."
    Returns (error_code, error_description) tuple.
    """
    if isinstance(error_message, str):
        match = re.match(r'^(\d+):\s*(.+)$', error_message.strip())
        if match:
            return match.group(1), match.group(2).strip()
    return None, str(error_message)

# API Configuration
TRANSPORTER_UPDATE_URL = "https://prod-api.mastersindia.co/api/v1/transporterIdUpdate/"


def update_transporter_id(user_gstin, eway_bill_number, transporter_id, transporter_name):
    """
    Update transporter ID for an e-way bill.

    Returns structured error with error_code + error_description on failure,
    or pdf_url + update_date on success.
    """
    try:
        print("=" * 70)
        print("🔄 UPDATING TRANSPORTER ID")
        print("=" * 70)

        jwt_token = load_jwt_token()
        if not jwt_token:
            return {
                "status": "error",
                "message": "Failed to load JWT token",
                "status_code": 401
            }

        headers = {
            "Authorization": f"JWT {jwt_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "userGstin": user_gstin,
            "eway_bill_number": int(eway_bill_number),
            "transporter_id": transporter_id,
            "transporter_name": transporter_name
        }

        print(f"📤 Request URL: {TRANSPORTER_UPDATE_URL}")
        print(f"📋 Payload: {json.dumps(payload, indent=2)}")

        response = requests.post(
            TRANSPORTER_UPDATE_URL,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(f"📥 Response Status Code: {response.status_code}")

        if response.status_code == 200:
            response_data = response.json()
            results = response_data.get("results", {})
            result_code = results.get("code", 200)
            result_status = results.get("status", "")
            message_field = results.get("message", {})

            # ── NIC error wrapped inside HTTP 200 ───────────────────────────
            if result_code == 204 or result_status == "No Content":
                raw_error = message_field if isinstance(message_field, str) else str(message_field)
                nic_code, nic_description = _parse_nic_error(raw_error)
                print(f"❌ NIC Error {nic_code}: {nic_description}")
                return {
                    "status": "error",
                    "message": nic_description,
                    "error_code": nic_code,
                    "error_description": nic_description,
                    "raw_error": raw_error,
                    "results": results,
                    "status_code": 422
                }

            # ── message is a dict (standard success shape) ──────────────────
            if isinstance(message_field, dict):
                if message_field.get("error", False):
                    raw_error = message_field.get("message", "Operation failed")
                    nic_code, nic_description = _parse_nic_error(raw_error)
                    print(f"❌ NIC Error {nic_code}: {nic_description}")
                    return {
                        "status": "error",
                        "message": nic_description,
                        "error_code": nic_code,
                        "error_description": nic_description,
                        "raw_error": raw_error,
                        "results": results,
                        "status_code": 422
                    }

                # ✅ SUCCESS
                print(f"✅ Success: {json.dumps(response_data, indent=2)}")
                return {
                    "status": "success",
                    "message": "Transporter ID updated successfully",
                    "eway_bill_number": message_field.get("ewayBillNo"),
                    "transporter_id": message_field.get("transporterId"),
                    "update_date": message_field.get("transUpdateDate"),
                    "pdf_url": message_field.get("url"),
                    "results": results,
                    "status_code": 200
                }

            # ── message is a plain string (could be NIC error or EWB no.) ───
            if isinstance(message_field, str):
                nic_code, nic_description = _parse_nic_error(message_field)
                if nic_code:
                    print(f"❌ NIC Error {nic_code}: {nic_description}")
                    return {
                        "status": "error",
                        "message": nic_description,
                        "error_code": nic_code,
                        "error_description": nic_description,
                        "raw_error": message_field,
                        "results": results,
                        "status_code": 422
                    }
                print(f"✅ Success: {json.dumps(response_data, indent=2)}")
                return {
                    "status": "success",
                    "message": "Transporter ID updated successfully",
                    "results": results,
                    "status_code": 200
                }

        elif response.status_code == 204:
            response_data = response.json()
            raw_error = response_data.get("results", {}).get("message", "Unknown error")
            nic_code, nic_description = _parse_nic_error(raw_error)
            print(f"⚠️ NIC Error {nic_code}: {nic_description}")
            return {
                "status": "error",
                "message": nic_description,
                "error_code": nic_code,
                "error_description": nic_description,
                "raw_error": raw_error,
                "results": response_data.get("results", {}),
                "status_code": 422
            }

        else:
            error_text = response.text
            print(f"❌ Error Response: {error_text}")
            try:
                error_data = response.json()
                raw_error = error_data.get("results", {}).get("message", error_text)
                nic_code, nic_description = _parse_nic_error(raw_error)
                return {
                    "status": "error",
                    "message": nic_description,
                    "error_code": nic_code,
                    "error_description": nic_description,
                    "raw_error": raw_error,
                    "status_code": response.status_code
                }
            except Exception:
                return {
                    "status": "error",
                    "message": f"Failed to update transporter ID: {error_text}",
                    "status_code": response.status_code
                }

    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return {
            "status": "error",
            "message": "Request timed out. Please try again.",
            "status_code": 408
        }

    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {str(e)}")
        return {
            "status": "error",
            "message": f"Network error: {str(e)}",
            "status_code": 500
        }

    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "status_code": 500
        }
