"""
Quick Test Script for Transporter ID Update API
Tests the new endpoint locally
"""
import requests
import json

BASE_URL = "http://localhost:5000"

def test_transporter_update():
    """Test the transporter ID update endpoint"""
    print("=" * 70)
    print("🧪 TESTING TRANSPORTER ID UPDATE API")
    print("=" * 70)
    
    url = f"{BASE_URL}/api/transporter-update"
    
    # Test data
    payload = {
        "user_gstin": "09COVPS5556J1ZT",
        "eway_bill_number": "481629240895",
        "transporter_id": "09ABEFS7095Q1Z4",
        "transporter_name": "SWASTIK TRANSPORT"
    }
    
    print(f"\n📤 Sending POST request to: {url}")
    print(f"📋 Payload:")
    print(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        
        print(f"\n📥 Response Status: {response.status_code}")
        print(f"📋 Response Body:")
        print(json.dumps(response.json(), indent=2))
        
        if response.status_code == 200:
            print("\n✅ Test PASSED - Transporter ID updated successfully!")
        else:
            print(f"\n⚠️ Test completed with status code: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Could not connect to the server.")
        print("Make sure the Flask server is running: python App.py")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    
    print("=" * 70)

def test_health_check():
    """Test the health check endpoint"""
    print("\n🏥 Testing Health Check...")
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ Server is running!")
        else:
            print(f"⚠️ Unexpected status: {response.status_code}")
    except:
        print("❌ Server is not running!")

if __name__ == "__main__":
    # First check if server is running
    test_health_check()
    
    # Then test the transporter update
    test_transporter_update()
