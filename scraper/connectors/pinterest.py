import requests
import logging
import time
from ..utils import request_with_retry

logger = logging.getLogger(__name__)

PINTEREST_API_BASE = "https://api.pinterest.com/v5"

def fetch_pinterest_data(credentials, start_date, end_date):
    """
    Fetches Pinterest Ads data using v5 API.
    """
    all_rows = []
    
    for cred in credentials:
        # Pinterest usually uses a Bearer token (access token) directly
        token = cred.get("token") or cred.get("refresh_token")
        account_id = cred.get("account_id")
        
        if not token or not account_id:
            continue
            
        logger.info(f"Fetching Pinterest data for account: {account_id}")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Pinterest Async Report
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "DAY",
            "level": "CAMPAIGN",
            "columns": ["SPEND_IN_DOLLAR", "TOTAL_IMPRESSION", "TOTAL_CLICKTHROUGH", "DATE"],
            "report_format": "JSON"
        }
        
        try:
            # 1. Request Report
            req = requests.post(f"{PINTEREST_API_BASE}/ad_accounts/{account_id}/reports", headers=headers, json=payload)
            
            if req.status_code == 401:
                logger.error(f"Pinterest Unauthorized for {account_id}. Token may be expired.")
                continue
            
            req.raise_for_status()
            token_report = req.json().get("token")
            
            # 2. Poll
            download_url = None
            for _ in range(30): # 2.5 mins max
                time.sleep(5)
                poll = requests.get(f"{PINTEREST_API_BASE}/ad_accounts/{account_id}/reports?token={token_report}", headers=headers)
                poll_data = poll.json()
                status = poll_data.get("report_status")
                
                if status == "FINISHED":
                    download_url = poll_data.get("url")
                    break
                if status == "FAILED":
                    logger.error(f"Pinterest report failed for {account_id}")
                    break
            
            if download_url:
                # 3. Download
                dl = requests.get(download_url)
                report_data = dl.json()
                
                # Pinterest sometimes returns a dict with 'data' or just the list
                # It depends on the version/endpoint nuances, but usually it's the JSON content directly if format=JSON
                # The report_data might be a list of dicts
                
                rows = report_data if isinstance(report_data, list) else report_data.get("data", [])
                
                for r in rows:
                    all_rows.append({
                        "date": r.get("DATE"),
                        "source": "pinterest",
                        "ad_account_id": account_id,
                        "impressions": r.get("TOTAL_IMPRESSION", 0),
                        "clicks": r.get("TOTAL_CLICKTHROUGH", 0),
                        "spend": r.get("SPEND_IN_DOLLAR", 0.0)
                    })
                    
        except Exception as e:
            logger.error(f"Pinterest error for {account_id}: {e}")

    return all_rows
