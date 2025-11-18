"""
Payload Capture Script - For MastersGST Support Team
This script captures the exact request and response details for debugging the 325 error
"""
import requests
import json
from auth_service import load_jwt_token

def capture_request_payload():
    """
    Capture complete request payload to share with MastersGST support
    """
    # Configuration
    BASE_URL = "https://prod-api.mastersindia.co/api/v1/"
    EWB_ENDPOINT = "getEwayBillData/"
    
    # Test parameters - Update these with your values
    GSTIN = "09COVPS5556J1ZT"
    EWAY_BILL_NUMBER = "431646987772"  # Change to the e-way bill you're testing
    
    # Load JWT token
    token = load_jwt_token()
    if not token:
        print("❌ No JWT token found!")
        return
    
    # Prepare request details
    url = f"{BASE_URL}{EWB_ENDPOINT}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"JWT {token}"
    }
    params = {
        "action": "GetEwayBill",
        "gstin": GSTIN,
        "eway_bill_number": EWAY_BILL_NUMBER
    }
    
    # Make request
    print("=" * 100)
    print("🔍 CAPTURING REQUEST & RESPONSE FOR MASTERS GST SUPPORT")
    print("=" * 100)
    
    try:
        response = requests.get(url, params=params, headers=headers)
        
        # Prepare complete payload information
        from datetime import datetime
        payload_info = {
            "timestamp": datetime.now().isoformat(),
            "request_details": {
                "method": "GET",
                "base_url": BASE_URL,
                "endpoint": EWB_ENDPOINT,
                "full_url": url,
                "complete_url_with_params": f"{url}?action={params['action']}&gstin={params['gstin']}&eway_bill_number={params['eway_bill_number']}",
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": f"JWT <YOUR_JWT_TOKEN>"  # Hidden for security
                },
                "query_parameters": {
                    "action": params['action'],
                    "gstin": params['gstin'],
                    "eway_bill_number": params['eway_bill_number']
                }
            },
            "response_details": {
                "status_code": response.status_code,
                "response_headers": dict(response.headers),
                "response_body": response.json() if response.text else None
            },
            "curl_command": f"""curl -X GET '{url}?action={params['action']}&gstin={params['gstin']}&eway_bill_number={params['eway_bill_number']}' \\
  -H 'Content-Type: application/json' \\
  -H 'Authorization: JWT <YOUR_JWT_TOKEN>'""",
            "postman_details": {
                "method": "GET",
                "url": f"{url}?action={params['action']}&gstin={params['gstin']}&eway_bill_number={params['eway_bill_number']}",
                "headers": [
                    {"key": "Content-Type", "value": "application/json"},
                    {"key": "Authorization", "value": "JWT <YOUR_JWT_TOKEN>"}
                ]
            }
        }
        
        # Save to file
        with open("payload_for_support.json", "w") as f:
            json.dump(payload_info, f, indent=2)
        
        # Print formatted output
        print("\n📋 REQUEST PAYLOAD:")
        print(f"Method: {payload_info['request_details']['method']}")
        print(f"URL: {payload_info['request_details']['complete_url_with_params']}")
        print(f"\nHeaders:")
        print(f"  Content-Type: application/json")
        print(f"  Authorization: JWT <YOUR_JWT_TOKEN>")
        print(f"\nQuery Parameters:")
        print(f"  action: {params['action']}")
        print(f"  gstin: {params['gstin']}")
        print(f"  eway_bill_number: {params['eway_bill_number']}")
        
        print("\n" + "=" * 100)
        print("📥 RESPONSE RECEIVED:")
        print(f"Status Code: {response.status_code}")
        print(f"\nResponse Body:")
        print(json.dumps(payload_info['response_details']['response_body'], indent=2))
        
        print("\n" + "=" * 100)
        print("✅ Payload details saved to: payload_for_support.json")
        print("=" * 100)
        
        # Create email template
        email_template = f"""
Subject: Error 325 - Could not retrieve data for E-Way Bill API

Dear MastersGST Support Team,

I am experiencing Error 325 ("Could not retrieve data") when calling the GetEwayBill API endpoint.

REQUEST DETAILS:
----------------
Method: GET
URL: {payload_info['request_details']['complete_url_with_params']}

Headers:
  Content-Type: application/json
  Authorization: JWT <token>

Query Parameters:
  - action: {params['action']}
  - gstin: {params['gstin']}
  - eway_bill_number: {params['eway_bill_number']}

RESPONSE RECEIVED:
-----------------
Status Code: {response.status_code}
Response: {json.dumps(payload_info['response_details']['response_body'], indent=2)}

The e-way bill number {params['eway_bill_number']} belongs to GSTIN {params['gstin']}.
I am using the production API endpoint: https://prod-api.mastersindia.co/api/v1/

Please help me understand why this error is occurring.

Best regards,
Your Name
"""
        
        with open("email_template_for_support.txt", "w") as f:
            f.write(email_template)
        
        print("\n📧 Email template created: email_template_for_support.txt")
        print("=" * 100)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    capture_request_payload()
