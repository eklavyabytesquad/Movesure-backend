"""
Generate E-Way Bill Service
Handles e-way bill generation via MastersIndia API
"""
import requests
import json
from auth.auth_service import get_auth_headers

# API Configuration
GENERATE_EWB_URL = "https://prod-api.mastersindia.co/api/v1/ewayBillsGenerate/"

# Required fields at the top level
REQUIRED_FIELDS = [
    'userGstin', 'supply_type', 'sub_supply_type', 'document_type',
    'document_number', 'document_date', 'gstin_of_consignor',
    'gstin_of_consignee', 'pincode_of_consignor', 'state_of_consignor',
    'pincode_of_consignee', 'state_of_supply', 'taxable_amount',
    'total_invoice_value', 'transportation_mode', 'transportation_distance',
    'itemList'
]

# Required fields per item in itemList
REQUIRED_ITEM_FIELDS = [
    'product_name', 'hsn_code', 'quantity', 'unit_of_product',
    'taxable_amount', 'cgst_rate', 'sgst_rate', 'igst_rate'
]


import re

GSTIN_REGEX = re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$')
VEHICLE_REGEX = re.compile(r'^[A-Z]{2}[0-9]{1,2}[A-Z]{0,3}[0-9]{4}$|^TM[A-Z0-9]{6}$', re.IGNORECASE)
DATE_REGEX = re.compile(r'^\d{2}/\d{2}/\d{4}$')

VALID_SUPPLY_TYPES = ['outward', 'inward']
VALID_DOC_TYPES = ['Tax Invoice', 'Bill of Supply', 'Bill of Entry', 'Delivery Challan', 'Credit Note', 'Others']
VALID_TRANSPORT_MODES = ['Road', 'Rail', 'Air', 'Ship', 'In Transit']
VALID_VEHICLE_TYPES = ['Regular', 'ODC']


def validate_payload(data):
    """
    Validate the e-way bill generation payload.
    Returns (is_valid, error_message, error_field)
    """
    errors = []

    # --- Check required fields ---
    missing = [f for f in REQUIRED_FIELDS if f not in data or data[f] is None]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}", 'missing_fields'

    # --- userGstin ---
    user_gstin = str(data.get('userGstin', '')).strip()
    if not GSTIN_REGEX.match(user_gstin):
        errors.append("userGstin: Must be a valid 15-character GSTIN (e.g. 09COVPS5556J1ZT)")

    # --- document_number: max 16 chars, alphanumeric + / and - only ---
    doc_num = str(data.get('document_number', '')).strip()
    if len(doc_num) == 0:
        errors.append("document_number: Cannot be empty")
    elif len(doc_num) > 16:
        errors.append(f"document_number: Max 16 characters allowed, you provided {len(doc_num)} characters ('{doc_num}')")
    elif not re.match(r'^[A-Za-z0-9/\-]+$', doc_num):
        errors.append("document_number: Only alphanumeric characters, '/' and '-' are allowed")

    # --- document_date: dd/mm/yyyy ---
    doc_date = str(data.get('document_date', '')).strip()
    if not DATE_REGEX.match(doc_date):
        errors.append(f"document_date: Must be in dd/mm/yyyy format, got '{doc_date}'")

    # --- supply_type ---
    supply_type = str(data.get('supply_type', '')).strip().lower()
    if supply_type not in VALID_SUPPLY_TYPES:
        errors.append(f"supply_type: Must be 'outward' or 'inward', got '{data.get('supply_type')}'")

    # --- document_type ---
    doc_type = str(data.get('document_type', '')).strip()
    if doc_type not in VALID_DOC_TYPES:
        errors.append(f"document_type: Must be one of {VALID_DOC_TYPES}, got '{doc_type}'")

    # --- Consignor GSTIN ---
    from_gstin = str(data.get('gstin_of_consignor', '')).strip()
    if from_gstin != 'URP' and not GSTIN_REGEX.match(from_gstin):
        errors.append("gstin_of_consignor: Must be a valid 15-char GSTIN or 'URP' for unregistered")

    # --- Consignee GSTIN ---
    to_gstin = str(data.get('gstin_of_consignee', '')).strip()
    if to_gstin != 'URP' and not GSTIN_REGEX.match(to_gstin):
        errors.append("gstin_of_consignee: Must be a valid 15-char GSTIN or 'URP' for unregistered")

    # --- Pincodes: 6-digit numbers ---
    for pin_field in ['pincode_of_consignor', 'pincode_of_consignee']:
        pin = data.get(pin_field)
        if pin is not None and not re.match(r'^\d{6}$', str(pin)):
            errors.append(f"{pin_field}: Must be a 6-digit number, got '{pin}'")

    # --- transportation_mode ---
    transport_mode = str(data.get('transportation_mode', '')).strip()
    if transport_mode not in VALID_TRANSPORT_MODES:
        errors.append(f"transportation_mode: Must be one of {VALID_TRANSPORT_MODES}, got '{transport_mode}'")

    # --- vehicle_number: required for Road mode ---
    if transport_mode == 'Road':
        vehicle = str(data.get('vehicle_number', '')).strip()
        if not vehicle:
            errors.append("vehicle_number: Required when transportation_mode is 'Road'")
        elif not VEHICLE_REGEX.match(vehicle):
            errors.append(f"vehicle_number: Invalid format '{vehicle}'. Use format like KA12BL4567 or TMXXXXXX for temp")

    # --- transporter_document_number: required for Rail/Air/Ship ---
    if transport_mode in ['Rail', 'Air', 'Ship']:
        trans_doc = str(data.get('transporter_document_number', '')).strip()
        if not trans_doc:
            errors.append(f"transporter_document_number: Required when transportation_mode is '{transport_mode}'")

    # --- transportation_distance: 0–4000 ---
    try:
        dist = int(data.get('transportation_distance', 0))
        if dist < 0 or dist > 4000:
            errors.append(f"transportation_distance: Must be between 0 and 4000 km, got {dist}")
    except (ValueError, TypeError):
        errors.append("transportation_distance: Must be a valid number")

    # --- vehicle_type ---
    v_type = str(data.get('vehicle_type', 'Regular')).strip()
    if v_type and v_type not in VALID_VEHICLE_TYPES:
        errors.append(f"vehicle_type: Must be 'Regular' or 'ODC', got '{v_type}'")

    # --- Amounts ---
    taxable = float(data.get('taxable_amount', 0))
    cgst = float(data.get('cgst_amount', 0))
    sgst = float(data.get('sgst_amount', 0))
    igst = float(data.get('igst_amount', 0))
    cess = float(data.get('cess_amount', 0))
    other = float(data.get('other_value', 0))
    cess_na = float(data.get('cess_nonadvol_value', 0))
    total_inv = float(data.get('total_invoice_value', 0))

    computed = taxable + cgst + sgst + igst + cess + other + cess_na
    if computed > total_inv + 2:
        errors.append(
            f"Amount mismatch: taxable({taxable}) + cgst({cgst}) + sgst({sgst}) + igst({igst}) "
            f"+ cess({cess}) + other({other}) + cessNonAdvol({cess_na}) = {computed:.2f}, "
            f"but total_invoice_value is {total_inv}. Sum must be <= total_invoice_value (+Rs.2 grace)"
        )

    # --- itemList ---
    item_list = data.get('itemList')
    if not isinstance(item_list, list) or len(item_list) == 0:
        errors.append("itemList: Must be a non-empty array of items")
    elif len(item_list) > 250:
        errors.append(f"itemList: Maximum 250 items allowed, you provided {len(item_list)}")
    else:
        for idx, item in enumerate(item_list):
            item_num = idx + 1
            missing_item = [f for f in REQUIRED_ITEM_FIELDS if f not in item]
            if missing_item:
                errors.append(f"Item #{item_num}: Missing fields: {', '.join(missing_item)}")
                continue

            # Auto-fill product_name from product_description if empty
            if not item.get('product_name') and item.get('product_description'):
                item['product_name'] = item['product_description']

            # HSN code must be numeric, 4-8 digits
            hsn = str(item.get('hsn_code', '')).strip()
            if not re.match(r'^\d{4,8}$', hsn):
                errors.append(f"Item #{item_num}: hsn_code must be 4-8 digits, got '{hsn}'")

            # Quantity must be > 0
            qty = item.get('quantity', 0)
            if float(qty) <= 0:
                errors.append(f"Item #{item_num}: quantity must be greater than 0")

    if errors:
        return False, errors[0] if len(errors) == 1 else errors, 'validation'

    return True, None, None


def normalize_payload(data):
    """
    Normalize field names from snake_case (frontend) to expected format.
    Handles: user_gstin -> userGstin, item_list -> itemList
    """
    # Map of snake_case -> expected camelCase/mixedCase field names
    field_map = {
        'user_gstin': 'userGstin',
        'item_list': 'itemList',
    }
    for snake, camel in field_map.items():
        if snake in data and camel not in data:
            data[camel] = data.pop(snake)
    return data


def generate_ewaybill(data):
    """
    Generate a new E-Way Bill.

    Args:
        data: Dictionary containing the full e-way bill generation payload.

    Returns:
        dict: Response with status, message, and data.
    """
    # Normalize field names (accept both snake_case and camelCase from frontend)
    data = normalize_payload(data)

    # Validate payload
    is_valid, error_msg, error_type = validate_payload(data)
    if not is_valid:
        return {
            "status": "error",
            "message": error_msg if isinstance(error_msg, str) else "Validation failed",
            "errors": error_msg if isinstance(error_msg, list) else [error_msg],
            "error_type": error_type,
            "status_code": 400
        }

    # Get auth headers
    headers = get_auth_headers()
    if not headers:
        return {
            "status": "error",
            "message": "Failed to get authentication token"
        }

    try:
        print("=" * 70)
        print("🚛 GENERATING E-WAY BILL - REQUEST PAYLOAD")
        print("=" * 70)
        print(f"📍 URL: {GENERATE_EWB_URL}")
        print(f"📋 Method: POST")
        print(f"📦 Payload Keys: {list(data.keys())}")
        print(f"📦 Items Count: {len(data.get('itemList', []))}")
        print(f"📦 Document Number: {data.get('document_number')}")
        print(f"📦 User GSTIN: {data.get('userGstin')}")
        print(f"📦 Supply Type: {data.get('supply_type')}")
        print(f"📦 Total Invoice Value: {data.get('total_invoice_value')}")
        print("=" * 70)

        response = requests.post(GENERATE_EWB_URL, json=data, headers=headers)

        print("=" * 70)
        print("📥 API RESPONSE")
        print("=" * 70)
        print(f"📊 Status Code: {response.status_code}")

        if response.status_code in (200, 201):
            result = response.json()
            print(f"📦 Response: {json.dumps(result, indent=2)}")

            # Save response to file for debugging
            with open("generate_ewaybill_response.json", "w") as f:
                json.dump(result, f, indent=2)

            # Parse nested response
            api_results = result.get("results", {})
            api_message = api_results.get("message", {})
            api_status = api_results.get("status", "")
            api_code = api_results.get("code", 0)

            # Check for error inside a 200 response
            if api_status == "No Content" or api_code == 204:
                nic_code = api_results.get("nic_code", "")
                print(f"⚠️ API returned error: {api_message}")
                return {
                    "status": "error",
                    "message": api_message if isinstance(api_message, str) else "API returned an error",
                    "nic_code": nic_code,
                    "data": result
                }

            # Success — extract key fields
            ewb_no = api_message.get("ewayBillNo", "") if isinstance(api_message, dict) else ""
            ewb_date = api_message.get("ewayBillDate", "") if isinstance(api_message, dict) else ""
            valid_upto = api_message.get("validUpto", "") if isinstance(api_message, dict) else ""
            alert = api_message.get("alert", "") if isinstance(api_message, dict) else ""
            pdf_url = api_message.get("url", "") if isinstance(api_message, dict) else ""

            # Prefix URL if needed
            if pdf_url and not pdf_url.startswith("http"):
                pdf_url = f"https://{pdf_url}"

            print(f"✅ E-Way Bill generated successfully!")
            print(f"📋 EWB Number: {ewb_no}")
            print(f"📅 EWB Date: {ewb_date}")
            print(f"📅 Valid Upto: {valid_upto}")
            if alert:
                print(f"⚠️ Alert: {alert}")
            if pdf_url:
                print(f"🔗 PDF URL: {pdf_url}")
            print("=" * 70)

            return {
                "status": "success",
                "message": "E-Way Bill generated successfully",
                "ewayBillNo": ewb_no,
                "ewayBillDate": ewb_date,
                "validUpto": valid_upto,
                "alert": alert,
                "url": pdf_url,
                "data": result
            }
        else:
            error_data = response.json() if response.text else {"error": "Unknown error"}
            print(f"❌ Failed to generate E-Way Bill!")
            print(f"Status Code: {response.status_code}")
            print(f"Error: {json.dumps(error_data, indent=2)}")
            print("=" * 70)

            return {
                "status": "error",
                "message": "Failed to generate E-Way Bill",
                "error": error_data,
                "status_code": response.status_code
            }

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return {
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }
