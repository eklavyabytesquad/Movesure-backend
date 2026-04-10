"""
Shared thread pool for parallel DB queries inside services.
All services reuse this single pool instead of creating their own
ThreadPoolExecutor on every request (which caused thread explosion
and 'resources unavailable' errors on Render).
"""
from concurrent.futures import ThreadPoolExecutor

# Single shared pool — max 10 worker threads for internal parallelism.
# This is separate from the FastAPI _executor (20 workers) in app.py.
# Total worst case: 20 (outer) + 10 (inner) = 30 threads — safe for Render.
shared_pool = ThreadPoolExecutor(max_workers=10)
