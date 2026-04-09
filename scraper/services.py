import os
import json
from google.cloud import bigquery
from google.cloud import datastore
from google.oauth2 import service_account

# --- Configuration ---
PROJECT_ID = "moda-platform-prd"

# Resolve project root (parent of this scraper/ directory)
_SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRAPER_DIR)

# Try to find credentials in priority order
CREDENTIAL_FILES = [
    os.getenv("BIGQUERY_SERVICE_ACCOUNT_FILE"),
    os.path.join(_PROJECT_ROOT, "credential_file.json"),  # project root
    os.path.join(_SCRAPER_DIR, "credential_file.json"),   # scraper/
    "credential_file.json",
    "credentials.json"
]

def get_credentials():
    """Loads Google Cloud credentials."""
    # 1. Try Env Var JSON content
    json_env = os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON")
    if json_env:
        try:
            info = json.loads(json_env)
            return service_account.Credentials.from_service_account_info(info)
        except json.JSONDecodeError:
            pass

    # 2. Try File Paths
    for path in CREDENTIAL_FILES:
        if path and os.path.exists(path):
            try:
                # Handle nested format if necessary
                with open(path, 'r') as f:
                    data = json.load(f)
                    if "bigquery_credential_file" in data:
                        return service_account.Credentials.from_service_account_info(data["bigquery_credential_file"])
                return service_account.Credentials.from_service_account_file(path)
            except Exception:
                continue
                
    raise RuntimeError("Could not load Google Cloud credentials. Please set BIGQUERY_SERVICE_ACCOUNT_JSON or BIGQUERY_SERVICE_ACCOUNT_FILE.")

def get_bq_client():
    creds = get_credentials()
    return bigquery.Client(credentials=creds, project=creds.project_id)

def get_datastore_client():
    creds = get_credentials()
    return datastore.Client(credentials=creds, project=PROJECT_ID)

def fetch_channel_credentials(workspace_id, channel_key):
    """
    Fetches credentials for a specific channel from Datastore.
    Handles both 'tokenactiveaccount' and 'common_auth_active_account' (for Reddit).
    """
    client = get_datastore_client()
    creds_list = []

    # 1. Query tokenactiveaccount (Standard)
    query = client.query(kind="tokenactiveaccount", namespace=workspace_id)
    results = list(query.fetch())
    
    # Mapping for connector names
    CONNECTOR_MAP = {
        "tiktok": ["tiktok", "tiktok_ads"],
        "amazon": ["amazon", "amazon_ads"],
        "pinterest": ["pinterest"],
        "meta": ["meta", "facebook", "facebook_ads", "instagram"],
        "google": ["google", "google_ads", "google_youtube", "youtube"],
        "microsoft": ["microsoft", "bing", "microsoft_ads"],
        "snapchat": ["snapchat"],
        "twitter": ["twitter", "twitter_x", "x"],
        "reddit": ["reddit", "reddit_ads"],
        "vibe" : ["vibe"]
    }
    
    target_connectors = CONNECTOR_MAP.get(channel_key, [channel_key])

    for entity in results:
        connector = (entity.get("connector") or "").lower()
        if connector in target_connectors:
            # Parse active_ad_account_details if present
            details_raw = entity.get("active_ad_account_details")
            details = []
            if details_raw:
                if isinstance(details_raw, list):
                    details = details_raw
                elif isinstance(details_raw, str):
                    try:
                        details = json.loads(details_raw)
                    except:
                        pass
            
            if details:
                for d in details:
                    creds_list.append({
                        **dict(entity),
                        "account_id": d.get("account_id"),
                        "login_customer_id": d.get("login_customer_id"),
                        "customer_id": d.get("customer_id")
                    })
            else:
                # Fallback to top-level fields
                if entity.get("account_id"):
                    creds_list.append(dict(entity))

    # 2. Special handling for Reddit (common_auth_active_account)
    if channel_key == "reddit":
        query_reddit = client.query(kind="common_auth_active_account", namespace=workspace_id)
        query_reddit.add_filter("integration_name", "=", "reddit")
        reddit_results = list(query_reddit.fetch())
        
        for entity in reddit_results:
            # Parse account_details
            acc_details_raw = entity.get("account_details")
            acc_details = []
            if acc_details_raw:
                if isinstance(acc_details_raw, list):
                    acc_details = acc_details_raw
                elif isinstance(acc_details_raw, str):
                    try:
                        acc_details = json.loads(acc_details_raw)
                    except:
                        pass
            
            if acc_details:
                for d in acc_details:
                    creds_list.append({
                        "connector": "reddit",
                        "refresh_token": entity.get("refresh_token"),
                        "token": entity.get("refresh_token"), # Reddit uses refresh token as base
                        "account_id": d.get("account_id")
                    })
            elif entity.get("account_id"):
                 creds_list.append({
                        "connector": "reddit",
                        "refresh_token": entity.get("refresh_token"),
                        "token": entity.get("refresh_token"),
                        "account_id": entity.get("account_id")
                    })

    return creds_list

def get_workspace_channel_status(workspace_id: str):
    """
    Query Datastore to determine which channels are integrated for a workspace
    and whether they are fully active or missing fields (reconnect).
    Returns a dict mapping connector_key -> status ("active" | "reconnect").
    """
    client = get_datastore_client()
    status_map = {}

    # 1. Check standard tokenactiveaccount
    query = client.query(kind="tokenactiveaccount", namespace=workspace_id)
    for entity in query.fetch():
        connector = (entity.get("connector") or "").lower().strip()
        if not connector:
            continue
            
        # Basic check for presence of token/refresh_token
        has_token = bool(entity.get("token") or entity.get("refresh_token"))
        # Check active_ad_account_details
        details_raw = entity.get("active_ad_account_details")
        has_accounts = False
        
        if details_raw:
            if isinstance(details_raw, list) and len(details_raw) > 0:
                has_accounts = True
            elif isinstance(details_raw, str):
                try:
                    parsed = json.loads(details_raw)
                    if parsed and isinstance(parsed, list):
                        has_accounts = True
                except:
                    pass
        elif entity.get("account_id"):
             has_accounts = True

        status = "active" if (has_token and has_accounts) else "reconnect"
        
        # If already active from another entity, keep it active
        if status_map.get(connector) != "active":
            status_map[connector] = status

    # 2. Check Reddit in common_auth_active_account
    query_reddit = client.query(kind="common_auth_active_account", namespace=workspace_id)
    query_reddit.add_filter("integration_name", "=", "reddit")
    for entity in query_reddit.fetch():
        has_token = bool(entity.get("refresh_token"))
        
        details_raw = entity.get("account_details")
        has_accounts = False
        
        if details_raw:
            if isinstance(details_raw, list) and len(details_raw) > 0:
                has_accounts = True
            elif isinstance(details_raw, str):
                try:
                    parsed = json.loads(details_raw)
                    if parsed and isinstance(parsed, list):
                        has_accounts = True
                except:
                    pass
        elif entity.get("account_id"):
             has_accounts = True
             
        status = "active" if (has_token and has_accounts) else "reconnect"
        
        if status_map.get("reddit") != "active":
             status_map["reddit"] = status

    return status_map
