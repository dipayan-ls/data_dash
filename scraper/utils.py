import os
import random
import time
import json
import logging
from datetime import datetime, timedelta
import requests
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Proxy Configuration Removed ---
# The user will rely on a PC-level VPN going forward.

def request_with_retry(url, method="GET", max_retries=5, **kwargs):
    """
    Executes a request with retries.
    Used specifically for unstable APIs like TikTok.
    Forces bypass of local OS Proxy variables so VPN routes directly.
    """
    # Force requests to ignore system proxies (HTTP_PROXY/HTTPS_PROXY)
    if 'proxies' not in kwargs:
        kwargs['proxies'] = {"http": None, "https": None}
        
    for attempt in range(max_retries):
        try:
            if method.upper() == "GET":
                response = requests.get(url, **kwargs)
            else:
                response = requests.post(url, **kwargs)
                
            response.raise_for_status()
            return response
        except RequestException as e:
            logger.warning(f"Request failed (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1)) # Exponential backoff
            else:
                logger.error(f"Max retries reached for {url}")
                raise e

def split_date_range(start_date, end_date, chunk_size_days=30):
    """Splits a date range into smaller chunks."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_size_days - 1), end)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end + timedelta(days=1)
    return chunks

def normalize_date(date_val):
    """Normalizes date to YYYY-MM-DD string."""
    if not date_val:
        return None
    return str(date_val)[:10]
