"""
Credential loading for BigQuery, Datastore, and per-workspace channel credentials.

Priority order for GCP service account:
  1. BIGQUERY_SERVICE_ACCOUNT_JSON env var (full JSON string)
  2. BIGQUERY_SERVICE_ACCOUNT_FILE env var (path to JSON key)
  3. credential_file.json in project root
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import Dict, List, Mapping, Optional, Tuple

from google.cloud import bigquery, datastore
from google.oauth2 import service_account

PROJECT_ID = "moda-platform-prd"
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_bq_credentials() -> service_account.Credentials:
    """Load BigQuery / Datastore service-account credentials."""
    json_env = os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON")
    file_env = os.getenv("BIGQUERY_SERVICE_ACCOUNT_FILE")
    default_cred_file = os.path.join(_PROJECT_ROOT, "credential_file.json")

    if json_env:
        info = json.loads(json_env)
        if "bigquery_credential_file" in info:
            info = info["bigquery_credential_file"]
        return service_account.Credentials.from_service_account_info(info)

    if file_env and os.path.exists(file_env):
        with open(file_env, "r") as f:
            cred_data = json.load(f)
            if "bigquery_credential_file" in cred_data:
                return service_account.Credentials.from_service_account_info(
                    cred_data["bigquery_credential_file"]
                )
        return service_account.Credentials.from_service_account_file(file_env)

    if os.path.exists(default_cred_file):
        with open(default_cred_file, "r") as f:
            cred_data = json.load(f)
            if "bigquery_credential_file" in cred_data:
                return service_account.Credentials.from_service_account_info(
                    cred_data["bigquery_credential_file"]
                )
        return service_account.Credentials.from_service_account_file(default_cred_file)

    raise RuntimeError(
        "BigQuery service account not configured. "
        "Set BIGQUERY_SERVICE_ACCOUNT_JSON or BIGQUERY_SERVICE_ACCOUNT_FILE, "
        f"or place credential_file.json in: {_PROJECT_ROOT}"
    )


def get_bq_client() -> bigquery.Client:
    creds = load_bq_credentials()
    return bigquery.Client(credentials=creds, project=creds.project_id)


def get_datastore_client() -> datastore.Client:
    creds = load_bq_credentials()
    return datastore.Client(project=PROJECT_ID, credentials=creds)


# ── Datastore helpers ────────────────────────────────────────────────────────

def _parse_active_details(raw) -> list:
    """Parse active_ad_account_details which may be list[dict] or a JSON string."""
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return []
    return []


def get_workspace_channel_credentials(workspace_id: str) -> List[Dict[str, Optional[str]]]:
    """
    Fetch channel-level credentials from Datastore (kind: tokenactiveaccount).
    Returns list of dicts with keys: connector, login_customer_id, customer_id,
    account_id, refresh_token, token.
    """
    client = get_datastore_client()
    query = client.query(kind="tokenactiveaccount", namespace=workspace_id)
    entities = list(query.fetch())

    if not entities:
        print(f"  No tokenactiveaccount entities found for workspace '{workspace_id}'.")
        return []

    rows: List[Dict[str, Optional[str]]] = []

    for e in entities:
        connector = e.get("connector")
        refresh_token = e.get("refresh_token")
        token = e.get("token")
        details = _parse_active_details(e.get("active_ad_account_details"))

        if not details:
            acc = e.get("account_id")
            login = e.get("login_customer_id")
            cust = e.get("customer_id")
            if acc:
                rows.append({
                    "connector": connector,
                    "login_customer_id": login,
                    "customer_id": cust,
                    "account_id": acc,
                    "refresh_token": refresh_token,
                    "token": token,
                })
            continue

        for d in details:
            rows.append({
                "connector": connector,
                "login_customer_id": d.get("login_customer_id"),
                "customer_id": d.get("customer_id"),
                "account_id": d.get("account_id"),
                "refresh_token": refresh_token,
                "token": token,
            })

    print(f"  Loaded {len(rows)} credential row(s) from Datastore for '{workspace_id}'.")
    return rows


def get_reddit_credentials(workspace_id: str) -> List[Dict[str, Optional[str]]]:
    """Fetch Reddit Ads credentials from Datastore (kind: common_auth_active_account)."""
    client = get_datastore_client()
    query = client.query(kind="common_auth_active_account", namespace=workspace_id)
    entities = list(query.fetch())

    rows: List[Dict[str, Optional[str]]] = []

    for e in entities:
        integration = str(e.get("integration_name") or "").strip().lower()
        if integration != "reddit":
            continue

        refresh_token = str(e.get("refresh_token") or "").strip()
        account_details = _parse_active_details(e.get("account_details") or "[]")

        if not account_details:
            acc_id = str(e.get("account_id") or "").strip()
            if acc_id:
                rows.append({
                    "connector": "reddit",
                    "account_id": acc_id,
                    "refresh_token": refresh_token,
                    "token": refresh_token,
                    "login_customer_id": None,
                    "customer_id": None,
                })
            continue

        for d in account_details:
            acc_id = str(d.get("account_id") or "").strip()
            if not acc_id:
                continue
            rows.append({
                "connector": "reddit",
                "account_id": acc_id,
                "refresh_token": refresh_token,
                "token": refresh_token,
                "login_customer_id": None,
                "customer_id": None,
            })

    if rows:
        print(f"  Reddit: loaded {len(rows)} account(s) for workspace '{workspace_id}'.")
    return rows


def get_workspace_channel_status(workspace_id: str) -> Dict[str, str]:
    """
    Returns a dict mapping connector_key -> status ("active" | "reconnect")
    for a given workspace.
    """
    client = get_datastore_client()
    status_map: Dict[str, str] = {}

    # Standard tokenactiveaccount
    query = client.query(kind="tokenactiveaccount", namespace=workspace_id)
    for entity in query.fetch():
        connector = (entity.get("connector") or "").lower().strip()
        if not connector:
            continue

        has_token = bool(entity.get("token") or entity.get("refresh_token"))
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
                except Exception:
                    pass
        elif entity.get("account_id"):
            has_accounts = True

        status = "active" if (has_token and has_accounts) else "reconnect"
        if status_map.get(connector) != "active":
            status_map[connector] = status

    # Reddit via common_auth_active_account
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
                except Exception:
                    pass
        elif entity.get("account_id"):
            has_accounts = True

        status = "active" if (has_token and has_accounts) else "reconnect"
        if status_map.get("reddit") != "active":
            status_map["reddit"] = status

    return status_map
