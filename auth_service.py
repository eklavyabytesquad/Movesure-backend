"""
Authentication Service
Handles JWT token authentication and management
"""
import requests
import json
import os
import base64
from datetime import datetime, timedelta

# API Configuration
AUTH_URL = "https://prod-api.mastersindia.co/api/v1/token-auth/"
USERNAME = "eklavyasingh9870@gmail.com"
PASSWORD = "3Mw@esRcnk3DM@C"
TOKEN_FILE = "jwt_token.json"
TOKEN_EXPIRY_HOURS = 23  # Assume token expires in 23 hours

# In-memory token cache (survives across requests in same process)
_token_cache = {
    "token": None,
    "expires_at": None
}

def decode_jwt_expiry(token):
    """
    Decode JWT token and extract expiry timestamp
    Returns expiry datetime or None if unable to decode
    """
    try:
        # JWT format: header.payload.signature
        parts = token.split('.')
        if len(parts) != 3:
            return None
        
        # Decode payload (add padding if needed)
        payload = parts[1]
        # Add padding to make length multiple of 4
        payload += '=' * (4 - len(payload) % 4)
        
        decoded = base64.urlsafe_b64decode(payload)
        payload_data = json.loads(decoded)
        
        # Get 'exp' claim (Unix timestamp)
        exp_timestamp = payload_data.get('exp')
        if exp_timestamp:
            return datetime.fromtimestamp(exp_timestamp)
        
        return None
    except Exception as e:
        print(f"⚠️ Error decoding JWT: {str(e)}")
        return None

def is_token_valid(token):
    """
    Check if token is still valid by checking JWT exp claim
    Returns True if valid, False if expired or invalid
    """
    if not token:
        return False
    
    try:
        expiry = decode_jwt_expiry(token)
        if not expiry:
            print("⚠️ Could not decode token expiry")
            return False
        
        # Add 5 minute buffer before expiry
        buffer_time = timedelta(minutes=5)
        now = datetime.now()
        
        if now >= (expiry - buffer_time):
            print(f"⚠️ Token expired or expiring soon (expiry: {expiry})")
            return False
        
        print(f"✅ Token valid until {expiry}")
        return True
    except Exception as e:
        print(f"⚠️ Error validating token: {str(e)}")
        return False

def load_jwt_token():
    """
    Load JWT token from memory cache or file, validate it, and refresh if expired
    Returns token if valid, None if unable to get valid token
    """
    try:
        # First check in-memory cache
        if _token_cache["token"] and is_token_valid(_token_cache["token"]):
            print("✅ Using cached token")
            return _token_cache["token"]
        
        # Try loading from file
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                token_data = json.load(f)
                token = token_data.get("token")
                
                # Validate token from file
                if token and is_token_valid(token):
                    # Update cache
                    _token_cache["token"] = token
                    expiry = decode_jwt_expiry(token)
                    _token_cache["expires_at"] = expiry
                    print("✅ Loaded valid token from file")
                    return token
                else:
                    print("⚠️ Token from file is expired or invalid, refreshing...")
                    return get_jwt_token()
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
    Get JWT authentication token, save it to memory cache and file
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
            
            # Get token expiry
            expiry = decode_jwt_expiry(token)
            
            # Update in-memory cache FIRST (this persists across requests)
            _token_cache["token"] = token
            _token_cache["expires_at"] = expiry
            
            # Prepare token data to save
            token_data = {
                "token": token,
                "timestamp": datetime.now().isoformat(),
                "expires_at": expiry.isoformat() if expiry else None,
                "username": USERNAME,
                "status": "success"
            }
            
            # Try to save token to JSON file (may fail on ephemeral filesystems)
            try:
                with open(TOKEN_FILE, "w") as f:
                    json.dump(token_data, f, indent=2)
                print(f"✅ Token saved to file: {TOKEN_FILE}")
            except Exception as file_error:
                print(f"⚠️ Could not save token to file (ephemeral storage): {file_error}")
                print("✅ Token cached in memory (will work for this session)")
            
            print("✅ Authentication successful!")
            if expiry:
                print(f"🕐 Token valid until: {expiry}")
            
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
