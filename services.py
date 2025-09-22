import requests
import time
import threading
from datetime import datetime
import logging
from masters_auth import authenticate, force_refresh_token

logger = logging.getLogger(__name__)

# Global variables for authentication service
service_running = True
last_token_refresh = None
refresh_interval = 3000  # 50 minutes

def refresh_token_periodically():
    """Background thread function to refresh token periodically"""
    global last_token_refresh
    
    while service_running:
        try:
            logger.info("Refreshing authentication token...")
            new_token = force_refresh_token()
            
            if new_token:
                last_token_refresh = datetime.now()
                logger.info("Token refreshed successfully")
            else:
                logger.error("Failed to refresh token")
                
        except Exception as e:
            logger.error(f"Error during token refresh: {e}")
        
        # Wait for the specified interval
        for _ in range(refresh_interval):
            if not service_running:
                break
            time.sleep(1)

def get_eway_bill_data_service(eway_bill_number, gstin="05AAABB0639G1Z8"):
    """Service function to get E-way bill data"""
    base_url = "https://sandb-api.mastersindia.co/api/v1/getEwayBillData/"
    url = f"{base_url}?action=GetEwayBill&gstin={gstin}&eway_bill_number={eway_bill_number}"
    
    try:
        headers = authenticate()
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": f"API Error {response.status_code}: {response.text}"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

def generate_eway_bill_service(eway_bill_data):
    """Service function to generate E-way bill"""
    url = "https://sandb-api.mastersindia.co/api/v1/ewayBillsGenerate/"
    
    try:
        headers = authenticate()
        response = requests.post(url, headers=headers, json=eway_bill_data, timeout=30)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": f"API Error {response.status_code}: {response.text}"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

def generate_consolidated_eway_bill_service(consolidated_data):
    """Service function to generate consolidated E-way bill"""
    url = "https://sandb-api.mastersindia.co/api/v1/consolidatedEwayBillsGenerate/"
    
    try:
        headers = authenticate()
        response = requests.post(url, headers=headers, json=consolidated_data, timeout=30)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": f"API Error {response.status_code}: {response.text}"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_consolidated_eway_bill_service(consolidated_eway_bill_number, gstin="05AAABB0639G1Z8"):
    """Service function to get consolidated E-way bill data (Trip Sheet)"""
    base_url = "https://sandb-api.mastersindia.co/api/v1/getEwayBillData/"
    url = f"{base_url}?action=GetTripSheet&gstin={gstin}&consolidated_eway_bill_number={consolidated_eway_bill_number}"
    
    try:
        headers = authenticate()
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": f"API Error {response.status_code}: {response.text}"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

def start_background_services():
    """Start background authentication service"""
    logger.info("Starting background authentication service")
    refresh_thread = threading.Thread(target=refresh_token_periodically, daemon=True)
    refresh_thread.start()
    logger.info("Background authentication service started")

def stop_services():
    """Stop background services"""
    global service_running
    service_running = False

def get_service_status():
    """Get current service status"""
    return {
        "service_running": service_running,
        "last_token_refresh": last_token_refresh
    }