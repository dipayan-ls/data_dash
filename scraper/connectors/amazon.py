import requests
import json
import logging
import time
import gzip
import io
from ..utils import split_date_range

logger = logging.getLogger(__name__)

AMAZON_API_BASE = "https://advertising-api.amazon.com"
AMAZON_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

def get_access_token(client_id, client_secret, refresh_token):
    """Exchanges refresh token for access token."""
    try:
        resp = requests.post(
            AMAZON_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret
            },
            timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception as e:
        logger.error(f"Amazon Token Refresh Failed: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        return None

def fetch_amazon_data(credentials, start_date, end_date, client_id, client_secret):
    """
    Fetches Amazon Ads data.
    Requires client_id and client_secret to be passed (usually from env vars).
    """
    if not client_id or not client_secret:
        logger.error("Amazon Client ID/Secret missing. Skipping Amazon.")
        return []

    all_rows = []
    
    # Group profiles by refresh token to minimize token requests
    grouped = {}
    for cred in credentials:
        rt = cred.get("refresh_token") or cred.get("token")
        pid = cred.get("account_id") # Profile ID
        if rt and pid:
            grouped.setdefault(rt, []).append(pid)

    for refresh_token, profile_ids in grouped.items():
        access_token = get_access_token(client_id, client_secret, refresh_token)
        if not access_token:
            continue
            
        for profile_id in profile_ids:
            logger.info(f"Fetching Amazon data for profile: {profile_id}")
            
            # Amazon reports max 90 days
            chunks = split_date_range(start_date, end_date, chunk_size_days=90)
            
            for chunk_start, chunk_end in chunks:
                try:
                    # 1. Request Report
                    report_body = {
                        "name": f"SP Daily {chunk_start}",
                        "startDate": chunk_start,
                        "endDate": chunk_end,
                        "configuration": {
                            "adProduct": "SPONSORED_PRODUCTS",
                            "reportTypeId": "spCampaigns",
                            "groupBy": ["campaign"],
                            "columns": ["campaignId", "date", "impressions", "clicks", "cost"],
                            "timeUnit": "DAILY",
                            "format": "GZIP_JSON"
                        }
                    }
                    
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Amazon-Advertising-API-ClientId": client_id,
                        "Amazon-Advertising-API-Scope": profile_id,
                        "Content-Type": "application/vnd.createasyncreportrequest.v3+json"
                    }
                    
                    req = requests.post(f"{AMAZON_API_BASE}/reporting/reports", headers=headers, json=report_body)
                    if req.status_code == 401:
                        # Token might have expired during processing, refresh and retry once
                        access_token = get_access_token(client_id, client_secret, refresh_token)
                        headers["Authorization"] = f"Bearer {access_token}"
                        req = requests.post(f"{AMAZON_API_BASE}/reporting/reports", headers=headers, json=report_body)
                        
                    req.raise_for_status()
                    report_id = req.json().get("reportId")
                    
                    # 2. Poll
                    status = "PENDING"
                    download_url = None
                    for _ in range(20): # Max 100 seconds wait
                        time.sleep(5)
                        poll = requests.get(f"{AMAZON_API_BASE}/reporting/reports/{report_id}", headers=headers)
                        poll_data = poll.json()
                        status = poll_data.get("status")
                        if status == "COMPLETED":
                            download_url = poll_data.get("url")
                            break
                        if status == "FAILED":
                            logger.error(f"Amazon report failed: {poll_data.get('failureReason')}")
                            break
                    
                    if download_url:
                        # 3. Download & Parse
                        dl = requests.get(download_url)
                        with gzip.GzipFile(fileobj=io.BytesIO(dl.content)) as gz:
                            content = gz.read().decode('utf-8')
                            # Amazon GZIP_JSON is a list of dicts
                            records = json.loads(content)
                            
                            for r in records:
                                all_rows.append({
                                    "date": r.get("date"),
                                    "source": "amazon",
                                    "ad_account_id": profile_id,
                                    "impressions": r.get("impressions", 0),
                                    "clicks": r.get("clicks", 0),
                                    "spend": r.get("cost", 0.0)
                                })
                                
                except Exception as e:
                    logger.error(f"Amazon fetch error for {profile_id} ({chunk_start}): {e}")

    return all_rows
