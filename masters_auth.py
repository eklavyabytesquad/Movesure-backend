import requests
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

class MastersIndiaAuth:
    def __init__(self):
        self.auth_url = "https://sandb-api.mastersindia.co/api/v1/token-auth/"
        self.credentials = {
            "username": "eklavyasingh9870@gmail.com",
            "password": "Support@0987#!"
        }
        self.token_file = "auth_token.json"
        self.token = None
        self.token_expires_at = None
        
    def load_cached_token(self) -> Optional[str]:
        """Load token from JSON file if it exists and is not expired"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, 'r') as f:
                    token_data = json.load(f)
                
                # Check if token is expired
                expires_at = datetime.fromisoformat(token_data.get('expires_at', '2000-01-01'))
                if datetime.now() < expires_at:
                    self.token = token_data.get('token')
                    self.token_expires_at = expires_at
                    print(f"✅ Using cached token (expires: {expires_at})")
                    return self.token
                else:
                    print("⏰ Cached token expired, will fetch new one")
                    
        except Exception as e:
            print(f"⚠️  Error loading cached token: {e}")
        
        return None
    
    def save_token_to_file(self, token: str, expires_in: int = 3600):
        """Save token to JSON file with expiry time"""
        try:
            # Calculate expiry time (default 1 hour, with 5 min buffer)
            expires_at = datetime.now() + timedelta(seconds=expires_in - 300)
            
            token_data = {
                "token": token,
                "expires_at": expires_at.isoformat(),
                "created_at": datetime.now().isoformat()
            }
            
            with open(self.token_file, 'w') as f:
                json.dump(token_data, f, indent=2)
                
            print(f"💾 Token saved to {self.token_file}")
            print(f"⏰ Token expires at: {expires_at}")
            
        except Exception as e:
            print(f"❌ Error saving token: {e}")
    
    def get_new_token(self) -> Optional[str]:
        """Fetch new authentication token from Masters India API"""
        print("🔐 Fetching new authentication token...")
        
        try:
            response = requests.post(
                self.auth_url,
                json=self.credentials,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            print(f"Auth Status Code: {response.status_code}")
            
            if response.status_code == 200:
                auth_data = response.json()
                token = auth_data.get('token')
                
                if token:
                    print(f"✅ New token obtained: {token[:50]}...")
                    
                    # Save token to file (assuming 1 hour expiry)
                    self.save_token_to_file(token, 3600)
                    
                    self.token = token
                    self.token_expires_at = datetime.now() + timedelta(hours=1)
                    
                    return token
                else:
                    print("❌ No token in API response")
                    print(f"Response: {response.text}")
            else:
                print(f"❌ API Error {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            
        return None
    
    def get_valid_token(self) -> Optional[str]:
        """Get a valid token (from cache or fetch new one)"""
        # First try to load cached token
        cached_token = self.load_cached_token()
        if cached_token:
            return cached_token
            
        # If no valid cached token, fetch new one
        return self.get_new_token()
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get headers with valid authentication token"""
        token = self.get_valid_token()
        
        if token:
            return {
                "Authorization": f"JWT {token}",
                "Content-Type": "application/json"
            }
        else:
            raise Exception("Failed to obtain authentication token")

# Create global instance for easy import
masters_auth = MastersIndiaAuth()

def authenticate() -> Dict[str, str]:
    """Simple function to get authentication headers"""
    return masters_auth.get_auth_headers()

def get_token() -> Optional[str]:
    """Simple function to get just the token"""
    return masters_auth.get_valid_token()

def force_refresh_token() -> Optional[str]:
    """Force refresh token (ignore cache)"""
    return masters_auth.get_new_token()

if __name__ == "__main__":
    # Test the authentication
    print("🧪 Testing Masters India Authentication...")
    try:
        headers = authenticate()
        print(f"✅ Authentication successful!")
        print(f"📋 Headers: {json.dumps(headers, indent=2)}")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")