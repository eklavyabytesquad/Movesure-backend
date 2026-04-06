"""
Extend E-Way Bill Validity Service
Handles extension of e-way bill validity period via MastersIndia API

Validations:
- mode_of_transport 1-4: consignment_status must be "M", transit_type must be ""
- mode_of_transport 5: consignment_status must be "T", transit_type can be "R", "W", or "O"
- Vehicle number required for road transport (mode 1), optional transport doc number
- Transport document number required for rail/air/ship (modes 2, 3, 4)
- Remaining distance must not exceed original distance on the e-way bill
- Validity can only be extended between 8 hours before and 8 hours after expiry
- Only the current transporter (or generator if no transporter assigned) can extend
"""
import requests
import json
import re
from auth.auth_service import load_jwt_token

# API Configuration
EXTEND_VALIDITY_URL = "https://prod-api.mastersindia.co/api/v1/ewayBillValidityExtend/"

# Valid options
VALID_TRANSPORT_MODES = {"1", "2", "3", "4", "5"}
VALID_TRANSIT_TYPES = {"R", "W", "O"}
VALID_CONSIGNMENT_STATUSES = {"M", "T"}
VEHICLE_NUMBER_PATTERN = re.compile(r'^[A-Z]{2}\d{1,2}[A-Z]{0,3}\d{4}$')


def validate_extend_payload(data):
    """
    Validate the extend e-way bill payload based on business rules.

    Args:
        data (dict): Request payload

    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    # Required fields
    required_fields = [
        'userGstin', 'eway_bill_number', 'vehicle_number',
        'place_of_consignor', 'state_of_consignor', 'remaining_distance',
        'mode_of_transport', 'extend_validity_reason', 'extend_remarks',
        'consignment_status', 'from_pincode', 'address_line1',
        'address_line2', 'address_line3'
    ]
    missing = [f for f in required_fields if f not in data or data[f] is None]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"

    mode = str(data.get('mode_of_transport', ''))
    consignment_status = str(data.get('consignment_status', ''))
    transit_type = str(data.get('transit_type', ''))
    vehicle_number = str(data.get('vehicle_number', '')).upper().replace(' ', '').replace('-', '')

    # Validate mode_of_transport
    if mode not in VALID_TRANSPORT_MODES:
        return False, f"Invalid mode_of_transport '{mode}'. Must be one of: 1 (Road), 2 (Rail), 3 (Air), 4 (Ship), 5 (In Transit)"

    # Mode 1-4: consignment_status must be M, transit_type must be blank
    if mode in {"1", "2", "3", "4"}:
        if consignment_status != "M":
            return False, f"For mode_of_transport {mode}, consignment_status must be 'M' (Moving). Got '{consignment_status}'"
        if transit_type != "":
            return False, f"For mode_of_transport {mode}, transit_type must be blank (''). Got '{transit_type}'"

    # Mode 5: consignment_status must be T, transit_type must be R/W/O
    if mode == "5":
        if consignment_status != "T":
            return False, f"For mode_of_transport 5, consignment_status must be 'T' (In Transit). Got '{consignment_status}'"
        if transit_type not in VALID_TRANSIT_TYPES:
            return False, f"For mode_of_transport 5, transit_type must be one of: R (Road), W (Warehouse), O (Others). Got '{transit_type}'"

    # Vehicle number validation for road transport (mode 1)
    if mode == "1":
        if not vehicle_number:
            return False, "Vehicle number is required for Road transport (mode 1)"
        if not VEHICLE_NUMBER_PATTERN.match(vehicle_number):
            return False, f"Invalid vehicle number format '{vehicle_number}'. Expected format: e.g., KA12TR1234"

    # Transport document number required for rail/air/ship (modes 2, 3, 4)
    if mode in {"2", "3", "4"}:
        transport_doc = data.get('transporter_document_number', '')
        if not transport_doc:
            return False, f"transporter_document_number is required for mode_of_transport {mode} (Rail/Air/Ship)"

    # Remaining distance validation
    remaining_distance = data.get('remaining_distance')
    try:
        remaining_distance = int(remaining_distance)
        if remaining_distance <= 0:
            return False, "remaining_distance must be a positive number"
    except (ValueError, TypeError):
        return False, "remaining_distance must be a valid number"

    return True, None


def extend_ewaybill_validity(data):
    """
    Extend the validity of an e-way bill.

    Args:
        data (dict): Request payload containing:
            - userGstin (str): User's GSTIN
            - eway_bill_number (int/str): E-way bill number
            - vehicle_number (str): Vehicle number
            - place_of_consignor (str): Current place of consignment
            - state_of_consignor (str): State of current location
            - remaining_distance (int): Remaining distance to destination
            - transporter_document_number (str, optional): Transport doc number
            - transporter_document_date (str, optional): Transport doc date (DD/MM/YYYY)
            - mode_of_transport (str): 1=Road, 2=Rail, 3=Air, 4=Ship, 5=In Transit
            - extend_validity_reason (str): Reason for extension
            - extend_remarks (str): Additional remarks
            - consignment_status (str): M=Moving (modes 1-4), T=In Transit (mode 5)
            - from_pincode (int/str): Pincode of current location
            - transit_type (str): R=Road, W=Warehouse, O=Others (only for mode 5, blank otherwise)
            - address_line1 (str): Address line 1
            - address_line2 (str): Address line 2
            - address_line3 (str): Address line 3

    Returns:
        dict: Response with status and results
    """
    try:
        print("=" * 70)
        print("📝 EXTENDING E-WAY BILL VALIDITY")
        print("=" * 70)

        # Validate payload
        is_valid, error_message = validate_extend_payload(data)
        if not is_valid:
            print(f"❌ Validation failed: {error_message}")
            return {
                "status": "error",
                "message": error_message,
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

        # Build payload - ensure correct types
        mode = str(data.get('mode_of_transport', ''))
        transit_type = str(data.get('transit_type', '')) if mode == "5" else ""

        payload = {
            "userGstin": data['userGstin'],
            "eway_bill_number": int(data['eway_bill_number']),
            "vehicle_number": str(data.get('vehicle_number', '')).upper().replace(' ', '').replace('-', ''),
            "place_of_consignor": data['place_of_consignor'],
            "state_of_consignor": data['state_of_consignor'],
            "remaining_distance": int(data['remaining_distance']),
            "transporter_document_number": str(data.get('transporter_document_number', '')),
            "transporter_document_date": str(data.get('transporter_document_date', '')),
            "mode_of_transport": mode,
            "extend_validity_reason": data['extend_validity_reason'],
            "extend_remarks": data['extend_remarks'],
            "consignment_status": data['consignment_status'],
            "from_pincode": int(data['from_pincode']),
            "transit_type": transit_type,
            "address_line1": data['address_line1'],
            "address_line2": data['address_line2'],
            "address_line3": data['address_line3']
        }

        print(f"📤 Request URL: {EXTEND_VALIDITY_URL}")
        print(f"📋 Payload: {json.dumps(payload, indent=2)}")

        # Make API request
        response = requests.post(
            EXTEND_VALIDITY_URL,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(f"📥 Response Status Code: {response.status_code}")

        # Parse response
        if response.status_code == 200:
            response_data = response.json()
            results = response_data.get("results", {})
            result_code = results.get("code", 200)
            result_status = results.get("status", "")

            # Check for error conditions (204 / No Content)
            if result_code == 204 or result_status == "No Content":
                error_msg = results.get("message", "Unknown error")
                nic_code = results.get("nic_code", "")
                print(f"❌ API Error (Code {result_code}, NIC: {nic_code}): {error_msg}")

                return {
                    "status": "error",
                    "message": error_msg,
                    "results": results,
                    "status_code": 204
                }

            # Check message field for errors
            message_field = results.get("message", {})
            if isinstance(message_field, dict):
                if message_field.get("error", False):
                    print(f"❌ Operation Error: {json.dumps(message_field, indent=2)}")
                    return {
                        "status": "error",
                        "message": "E-Way Bill validity extension failed",
                        "results": results,
                        "status_code": 400
                    }
                else:
                    # Success
                    ewb_no = message_field.get("ewayBillNo", "")
                    valid_upto = message_field.get("validUpto", "")
                    pdf_url = message_field.get("url", "")
                    print(f"✅ E-Way Bill {ewb_no} validity extended until {valid_upto}")
                    if pdf_url:
                        print(f"📄 PDF URL: {pdf_url}")

                    return {
                        "status": "success",
                        "message": "E-Way Bill validity extended successfully",
                        "results": results,
                        "status_code": 200
                    }
            else:
                # String message - could be error or info
                if isinstance(message_field, str) and ("Error" in message_field or "Invalid" in message_field):
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
                        "message": "E-Way Bill validity extended successfully",
                        "results": results,
                        "status_code": 200
                    }

        elif response.status_code == 204:
            response_data = response.json()
            error_msg = response_data.get("results", {}).get("message", "Unknown error")
            print(f"⚠️ No Content: {error_msg}")

            return {
                "status": "error",
                "message": error_msg,
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
                    "message": error_data.get("message", "Failed to extend e-way bill validity"),
                    "results": error_data.get("results", {}),
                    "status_code": response.status_code
                }
            except Exception:
                return {
                    "status": "error",
                    "message": f"Failed to extend e-way bill validity: {error_text}",
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
