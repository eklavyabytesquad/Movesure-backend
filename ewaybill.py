import requests
import json
import os

# EWB API Configuration
BASE_URL = "https://prod-api.mastersindia.co/api/v1/"
EWB_ENDPOINT = "getEwayBillData/"

def load_jwt_token():
    """
    Load JWT token from jwt_token.json file
    """
    try:
        if os.path.exists("jwt_token.json"):
            with open("jwt_token.json", "r") as f:
                token_data = json.load(f)
                return token_data.get("token")
        else:
            print("❌ jwt_token.json file not found! Please run App.py first to get the token.")
            return None
    except Exception as e:
        print(f"❌ Error loading token: {str(e)}")
        return None

def get_ewaybill_details(eway_bill_number, gstin):
    """
    Get EWB Details using the JWT token
    """
    # Load JWT token
    jwt_token = load_jwt_token()
    if not jwt_token:
        return None
    
    # Prepare headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"JWT {jwt_token}"
    }
    
    # Prepare URL with query parameters
    url = f"{BASE_URL}{EWB_ENDPOINT}"
    params = {
        "action": "GetEwayBill",
        "gstin": gstin,
        "eway_bill_number": eway_bill_number
    }
    
    try:
        print("🚛 Getting EWB Details...")
        print(f"URL: {url}")
        print(f"Method: GET")
        print(f"EWB Number: {eway_bill_number}")
        print(f"GSTIN: {gstin}")
        print("-" * 50)
        
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ EWB Details retrieved successfully!")
            print(f"Response: {json.dumps(data, indent=2)}")
            
            # Save response to file
            with open("ewaybill_response.json", "w") as f:
                json.dump(data, f, indent=2)
            print("💾 Response saved to: ewaybill_response.json")
            
            return data
        else:
            print(f"❌ Failed to get EWB details!")
            print(f"Status Code: {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error Response: {json.dumps(error_data, indent=2)}")
            except:
                print(f"Error Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Request failed: {str(e)}")
        return None

def main():
    """
    Main function - Get EWB details
    """
    print("=" * 60)
    print("EWB DETAILS FETCHER")
    print("=" * 60)
    
    # EWB Configuration
    EWAYBILL_NUMBER = "451629889107"
    GSTIN = "09COVPS5556J1ZT"
    
    print(f"🎯 EWB Number: {EWAYBILL_NUMBER}")
    print(f"🏢 GSTIN: {GSTIN}")
    print()
    
    # Get EWB details
    result = get_ewaybill_details(EWAYBILL_NUMBER, GSTIN)
    
    if result:
        print("\n✅ Success! EWB details retrieved and saved to 'ewaybill_response.json'")
        file_path = os.path.abspath("ewaybill_response.json")
        print(f"📁 File location: {file_path}")
    else:
        print("\n❌ Failed to get EWB details")
        print("💡 Make sure you have a valid JWT token in jwt_token.json")

if __name__ == "__main__":
    main()