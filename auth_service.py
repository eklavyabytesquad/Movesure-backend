"""
Authentication Service
Handles JWT token authentication and management
"""
import requests
import json
import os
from datetime import datetime, timedelta

# API Configuration
AUTH_URL = "https://prod-api.mastersindia.co/api/v1/token-auth/"
USERNAME = "eklavyasingh9870@gmail.com"
PASSWORD = "3Mw@esRcnk3DM@C"
TOKEN_FILE = "jwt_token.json"
TOKEN_EXPIRY_HOURS = 23  # Assume token expires in 23 hours

def load_jwt_token():
    """
    Load JWT token from jwt_token.json file
    Returns token if valid, None if expired or not found
    """
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                token_data = json.load(f)
                
                # Check if token has expired
                if is_token_expired(token_data):
                    print("⚠️ Token has expired, refreshing...")
                    return get_jwt_token()
                
                return token_data.get("token")
        else:
            print("⚠️ jwt_token.json file not found! Getting new token...")
            return get_jwt_token()
    except Exception as e:
        print(f"❌ Error loading token: {str(e)}")
        return get_jwt_token()

def is_token_expired(token_data):
    """
    Check if token has expired based on timestamp
    """
    try:
        timestamp_str = token_data.get("timestamp")
        if not timestamp_str:
            return True
        
        token_time = datetime.fromisoformat(timestamp_str)
        current_time = datetime.now()
        time_diff = current_time - token_time
        
        # Token expires after TOKEN_EXPIRY_HOURS
        return time_diff > timedelta(hours=TOKEN_EXPIRY_HOURS)
    except Exception as e:
        print(f"⚠️ Error checking token expiry: {str(e)}")
        return True

def get_jwt_token():
    """
    Get JWT authentication token and save it to a JSON file
    """
    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }
    
    headers = {"Content-Type": "application/json"}
    
    try:
        print("🔐 Authenticating...")
        print(f"URL: {AUTH_URL}")
        
        response = requests.post(AUTH_URL, json=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            token = data.get("token")
            
            # Prepare token data to save
            token_data = {
                "token": token,
                "timestamp": datetime.now().isoformat(),
                "username": USERNAME,
                "status": "success"
            }
            
            # Save token to JSON file
            with open(TOKEN_FILE, "w") as f:
                json.dump(token_data, f, indent=2)
            
            print("✅ Authentication successful!")
            print(f"🎯 Token saved to: {TOKEN_FILE}")
            
            return token
            
        else:
            error_data = response.json()
            print("❌ Authentication failed!")
            print(f"Error: {json.dumps(error_data, indent=2)}")
            return None
            
    except Exception as e:
        print(f"❌ Request failed: {str(e)}")
        return None

def get_auth_headers():
    """
    Get headers with valid JWT token
    """
    token = load_jwt_token()
    if not token:
        return None
    
    return {
        "Content-Type": "application/json",
        "Authorization": f"JWT {token}"
    }
