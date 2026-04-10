"""
Supabase Connection Service
Provides a thread-safe singleton Supabase client for database operations.
Uses threading.Lock to prevent duplicate client creation under concurrent access.
"""
import os
import threading
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

_supabase_client: Client = None
_lock = threading.Lock()


def get_supabase() -> Client:
    """Get or create the Supabase client singleton (thread-safe)."""
    global _supabase_client
    if _supabase_client is None:
        with _lock:
            # Double-checked locking — re-check after acquiring lock
            if _supabase_client is None:
                url = os.environ.get("SUPABASE_URL")
                key = os.environ.get("SUPABASE_SERVICE_KEY")
                if not url or not key:
                    raise RuntimeError(
                        "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env"
                    )
                _supabase_client = create_client(url, key)
    return _supabase_client
