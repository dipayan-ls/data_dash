import json
import logging
from ..utils import request_with_retry, split_date_range, normalize_date

logger = logging.getLogger(__name__)

def fetch_tiktok_data(credentials, start_date, end_date):
    """
    Fetches TikTok Ads data with robust retries (proxy rotation removed, relies on local VPN).
    """
    all_rows = []
    
    # Group by access token to avoid duplicate fetches if multiple accounts share token (though usually 1:1)
    # Actually, credentials list is flattened by account_id in services.py, so we iterate.
    
    for cred in credentials:
        access_token = cred.get("token") or cred.get("refresh_token") # Fallback, usually token is access token
        # Note: In production, you might need to exchange refresh_token for access_token if expired.
        # Assuming 'token' is a valid access token for now, or the user provides it via env var override.
        
        advertiser_id = cred.get("account_id")
        if not access_token or not advertiser_id:
            logger.warning(f"Skipping TikTok account {advertiser_id}: Missing token or ID")
            continue

        logger.info(f"Fetching TikTok data for advertiser: {advertiser_id}")
        
        # TikTok has a 30-day limit per request
        chunks = split_date_range(start_date, end_date, chunk_size_days=30)
        
        url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        headers = {"Access-Token": access_token}

        for chunk_start, chunk_end in chunks:
            params = {
                "advertiser_id": advertiser_id,
                "service_type": "AUCTION",
                "report_type": "BASIC",
                "data_level": "AUCTION_CAMPAIGN",
                "dimensions": json.dumps(["stat_time_day"]),
                "metrics": json.dumps(["impressions", "clicks", "spend"]),
                "time_granularity": "DAILY",
                "start_date": chunk_start,
                "end_date": chunk_end,
                "page": 1,
                "page_size": 1000
            }
            
            try:
                # Use the robust retry function
                response = request_with_retry(url, headers=headers, params=params)
                data = response.json()
                
                if data.get("code") != 0:
                    logger.error(f"TikTok API Error for {advertiser_id}: {data.get('message')}")
                    continue
                
                list_data = data.get("data", {}).get("list", [])
                for item in list_data:
                    dims = item.get("dimensions", {})
                    metrics = item.get("metrics", {})
                    
                    all_rows.append({
                        "date": normalize_date(dims.get("stat_time_day")),
                        "source": "tiktok",
                        "ad_account_id": advertiser_id,
                        "impressions": int(metrics.get("impressions", 0)),
                        "clicks": int(metrics.get("clicks", 0)),
                        "spend": float(metrics.get("spend", 0.0))
                    })
                    
            except Exception as e:
                logger.error(f"Failed to fetch TikTok chunk {chunk_start}-{chunk_end} for {advertiser_id}: {e}")
                
    return all_rows
