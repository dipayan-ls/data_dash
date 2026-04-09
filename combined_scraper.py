"""
Unified multi-workspace ads scraper + BigQuery validator.

Goal:
- Multi-Workspace Support: Query and combine data for multiple workspaces in one run.
- Fetch API data per selected channel (sequentially, to avoid overload).
- Fetch BigQuery data for all selected channels in a single query.
- Reddit Credentials: Fetch from `common_auth_active_account` Datastore kind.
- Polling + Status: Detailed live updates via `ProgressReporter`.
- Output: Single combined CSV with schema:
  date,workspace_name,source,ad_account_id,api_impressions,api_clicks,api_spends,
  bigquery_impressions,bigquery_clicks,bigquery_spends

Notes / Assumptions:
- BigQuery:
  - Uses table:  `moda-platform-prd.{workspace_id}.adspends_mat_view`
  - Uses column: `ad_account_id` for the ad-account identifier (hardcoded, as requested)
  - Uses the same service account for all channels.
  - The service account JSON is NOT hardcoded here for security.
    Provide it via:
      - env var BIGQUERY_SERVICE_ACCOUNT_JSON (full JSON), OR
      - env var BIGQUERY_SERVICE_ACCOUNT_FILE (path to JSON file).

- API:
  - Channels are fetched one after another (sequentially).
  - BigQuery is queried once for all selected channels.
  - To keep this script secure and maintainable, API credentials are NOT hardcoded.
    They are collected via input() prompts or can be wired to environment variables.

- All channels are now fully implemented:
  - Google Ads (Google + YouTube) - ✅
  - Meta (Facebook + Instagram) - ✅
  - Microsoft / Bing - ✅
  - Pinterest - ✅
  - Snapchat - ✅
  - TikTok - ✅
  - Twitter / X - ✅
  - Amazon Ads - ✅
  - Reddit Ads - ✅
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

import decimal
import requests
from google.cloud import bigquery
from google.cloud import datastore
from google.oauth2 import service_account

# Optional nice CLI checkboxes. Falls back to text input if unavailable.
try:
    import questionary  # type: ignore
except Exception:  # pragma: no cover - purely optional
    questionary = None


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

DateStr = str   # "YYYY-MM-DD"
SourceStr = str  # e.g. "google", "youtube", "facebook"
AccountStr = str  # ad account ID (cleaned, no dashes/act_ prefix)


@dataclass
class DailyMetrics:
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0

    def add(self, imp: int, clk: int, spend: float) -> None:
        self.impressions += int(imp or 0)
        self.clicks += int(clk or 0)
        self.spend += float(spend or 0.0)


# Key is (date, source, account_id) — account_id may be "" if unknown.
DailyMetricsMap = Dict[Tuple[DateStr, SourceStr, AccountStr], DailyMetrics]


# ---------------------------------------------------------------------------
# Channel registry
# ---------------------------------------------------------------------------

CHANNEL_LABELS = {
    "google_youtube": "Google Ads (Google + YouTube)",
    "meta": "Meta (Facebook + Instagram)",
    "microsoft": "Microsoft / Bing",
    "pinterest": "Pinterest",
    "snapchat": "Snapchat",
    "tiktok": "TikTok",
    "twitter_x": "Twitter / X",
    "amazon": "Amazon Ads",
    "reddit": "Reddit Ads",
    "vibe": "Vibe",
}

# Map Datastore "connector" values to internal channel keys used in this script.
# Keys are lowercase connector names; values are keys from CHANNEL_LABELS.
CONNECTOR_TO_CHANNEL = {
    # Google Ads
    "google": "google_youtube",
    "google_ads": "google_youtube",
    "google_youtube": "google_youtube",
    "google_customer_match": "google_youtube",
    # Meta / Facebook
    "meta": "meta",
    "facebook": "meta",
    "facebook_ads": "meta",
    "facebook_custom_audience": "meta",
    # Microsoft / Bing
    "microsoft": "microsoft",
    "bing": "microsoft",
    "microsoft_bing": "microsoft",
    "microsoft_ads": "microsoft",
    # Pinterest
    "pinterest": "pinterest",
    # Snapchat
    "snapchat": "snapchat",
    # TikTok
    "tiktok": "tiktok",
    "tiktok_ads": "tiktok",
    # Twitter / X
    "twitter": "twitter_x",
    "twitter_x": "twitter_x",
    "x": "twitter_x",
    # Amazon Ads
    "amazon": "amazon",
    "amazon_ads": "amazon",
    # Reddit Ads
    "reddit": "reddit",
    "reddit_ads": "reddit",
    # Vibe
    "vibe": "vibe",
}


def _load_workspace_metadata(
    csv_path: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """
    Load workspace_id and workspace_name from Workspace_information.csv.
    Returns list of (workspace_id, workspace_name).
    """
    if csv_path is None:
        base_dir = os.path.dirname(__file__)
        csv_path = os.path.join(base_dir, "Workspace_information.csv")

    workspaces: List[Tuple[str, str]] = []
    if not os.path.exists(csv_path):
        raise RuntimeError(f"Workspace information CSV not found at: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ws_id = (row.get("workspace_id") or "").strip()
            ws_name = (row.get("workspace_name") or "").strip()
            if ws_id:
                workspaces.append((ws_id, ws_name or ws_id))

    if not workspaces:
        raise RuntimeError("No workspace entries found in Workspace_information.csv.")

    return workspaces


def _ask_workspaces() -> List[Tuple[str, str]]:
    """
    Ask user to pick one or more workspaces and return List[(workspace_id, workspace_name)].
    Supports:
    - questionary checkbox (if installed)
    - Text fallback: comma-separated numbers, or 'all'
    """
    workspaces = _load_workspace_metadata()

    if questionary is not None:
        choices = [
            questionary.Choice(
                title=f"{name} ({ws_id})",
                value=(ws_id, name),
            )
            for ws_id, name in workspaces
        ]
        selected = questionary.checkbox(
            "Select workspace(s) (space = select, enter = confirm):",
            choices=choices,
        ).ask()
        if not selected:
            raise SystemExit("No workspace selected. Exiting.")
        return list(selected)

    # Fallback: numeric selection (comma-separated or 'all')
    print("Available workspaces:")
    for idx, (ws_id, name) in enumerate(workspaces, start=1):
        print(f"{idx}. {name} ({ws_id})")
    raw = input("Enter workspace number(s), comma-separated (or 'all'): ").strip()
    if raw.lower() == "all":
        return list(workspaces)

    selected: List[Tuple[str, str]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            idx = int(part)
        except ValueError:
            raise SystemExit(f"Invalid selection: {part!r}")
        if not (1 <= idx <= len(workspaces)):
            raise SystemExit(f"Selection out of range: {idx}")
        selected.append(workspaces[idx - 1])

    if not selected:
        raise SystemExit("No workspace selected. Exiting.")
    return selected


def _ask_channels() -> List[str]:
    """Return list of selected internal channel keys."""
    keys = list(CHANNEL_LABELS.keys())
    if questionary is not None:
        choices = [questionary.Choice(CHANNEL_LABELS[k], value=k) for k in keys]
        selected = questionary.checkbox(
            "Select channels to include (space = select, enter = confirm):",
            choices=choices,
        ).ask()
        if not selected:
            raise SystemExit("No channels selected. Exiting.")
        return list(selected)

    # Fallback: simple comma-separated text
    print("Available channels:")
    for k in keys:
        print(f"- {k}: {CHANNEL_LABELS[k]}")
    raw = input(
        "Enter comma-separated channel keys to include "
        "(e.g. google_youtube,meta,pinterest): "
    )
    selected = [p.strip() for p in raw.split(",") if p.strip()]
    for s in selected:
        if s not in CHANNEL_LABELS:
            raise SystemExit(f"Unknown channel key: {s}")
    if not selected:
        raise SystemExit("No channels selected. Exiting.")
    return selected

# ---------------------------------------------------------------------------
# Progress reporter (pure-print, no external deps)
# ---------------------------------------------------------------------------

class ProgressReporter:
    """
    Lightweight progress / status reporter that uses plain print() calls.
    No external dependencies (no tqdm, no rich).

    Indentation levels:
      ── [w/W] Workspace: Name
        ├─ Channel: Meta (Facebook + Instagram)
        │  Account [a/A]: act_123 ...
        │    ✓ 142 rows fetched
        │    ✗ 403 Forbidden — skipped
        ├─ BigQuery query...
        │  ✓ 12,400 rows
        └─ CSV rows written: 8,712
      ── [w/W] Done ✅
    """

    def __init__(self, total_workspaces: int) -> None:
        self.total_workspaces = total_workspaces
        self._ws_idx = 0
        self._api_rows: int = 0
        self._csv_rows: int = 0
        self._ts_fmt = "%H:%M:%S"

    def _ts(self) -> str:
        return dt.datetime.now().strftime(self._ts_fmt)

    # ---- workspace level ----

    def start_workspace(self, ws_id: str, ws_name: str) -> None:
        self._ws_idx += 1
        self._api_rows = 0
        print(
            f"\n{'='*60}\n"
            f"── [{self._ws_idx}/{self.total_workspaces}] Workspace: {ws_name} ({ws_id})  [{self._ts()}]\n"
            f"{'='*60}"
        )

    def done_workspace(self, ws_name: str, csv_rows: int) -> None:
        print(
            f"── [{self._ws_idx}/{self.total_workspaces}] Done: {ws_name}  "
            f"{csv_rows:,} CSV rows written  [{self._ts()}] ✅"
        )

    # ---- channel level ----

    def start_channel(self, label: str, total_accounts: int) -> None:
        suffix = f"  ({total_accounts} account(s))" if total_accounts else ""
        print(f"\n  ├─ Channel: {label}{suffix}  [{self._ts()}]")

    def done_channel(self, label: str, rows: int) -> None:
        print(f"  ├─ Done: {label}  {rows:,} rows  [{self._ts()}]")

    # ---- account level ----

    def start_account(self, account_id: str, idx: int, total: int) -> None:
        print(f"  │  Fetching account [{idx}/{total}]: {account_id}  [{self._ts()}]")

    def account_done(self, account_id: str, rows: int) -> None:
        print(f"  │    ✓ {rows:,} rows returned for {account_id}")
        self._api_rows += rows

    def account_error(self, account_id: str, status: int, msg: str) -> None:
        print(f"  │    ✗ HTTP {status} for {account_id}: {msg} — skipped")

    def account_retry(self, account_id: str, attempt: int, max_retries: int, wait: float, reason: str) -> None:
        print(f"  │    ↻ Retry {attempt}/{max_retries} for {account_id} in {wait:.0f}s ({reason})")

    def chunk_progress(self, account_id: str, chunk_start: str, chunk_end: str, page: int) -> None:
        """Called per-page/chunk so long-running fetches are visible."""
        print(f"  │    · {account_id}  {chunk_start}→{chunk_end}  page {page}  [{self._ts()}]")

    # ---- BigQuery level ----

    def bq_query_start(self, workspace_id: str, account_count: int) -> None:
        print(
            f"\n  ├─ BigQuery query for '{workspace_id}'  "
            f"({account_count} account ID(s))  [{self._ts()}]"
        )

    def bq_query_done(self, rows: int) -> None:
        print(f"  │  ✓ {rows:,} BigQuery rows fetched  [{self._ts()}]")

    # ---- CSV level ----

    def csv_rows_written(self, n: int) -> None:
        print(f"  └─ CSV rows written: {n:,}  [{self._ts()}]")

    # ---- daily polling (Reddit / long channels) ----

    def day_progress(self, account_id: str, date_str: str, total_days: int, done_days: int) -> None:
        if done_days % 10 == 0 or done_days == 1:
            print(
                f"  │    · {account_id}  day {done_days}/{total_days}: {date_str}  [{self._ts()}]"
            )


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _load_bq_credentials() -> service_account.Credentials:
    """Load BigQuery service-account credentials from environment or default file.

    Priority order:
    1. BIGQUERY_SERVICE_ACCOUNT_JSON: full JSON string (env var)
    2. BIGQUERY_SERVICE_ACCOUNT_FILE: file path to JSON key (env var)
    3. Default file: /Users/dipayandhar/dashboard_project/credential_file.json
    """
    json_env = os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON")
    file_env = os.getenv("BIGQUERY_SERVICE_ACCOUNT_FILE")
    default_cred_file = os.path.join(os.path.dirname(__file__), "credential_file.json")

    if json_env:
        info = json.loads(json_env)
        return service_account.Credentials.from_service_account_info(info)

    if file_env:
        if os.path.exists(file_env):
            # Check if it's a nested structure (like credential_file.json)
            with open(file_env, 'r') as f:
                cred_data = json.load(f)
                if "bigquery_credential_file" in cred_data:
                    info = cred_data["bigquery_credential_file"]
                    return service_account.Credentials.from_service_account_info(info)
            # Otherwise, try as direct service account file
            return service_account.Credentials.from_service_account_file(file_env)

    # Try default credential file
    if os.path.exists(default_cred_file):
        with open(default_cred_file, 'r') as f:
            cred_data = json.load(f)
            if "bigquery_credential_file" in cred_data:
                info = cred_data["bigquery_credential_file"]
                return service_account.Credentials.from_service_account_info(info)
        # Fallback: try as direct service account file
        return service_account.Credentials.from_service_account_file(default_cred_file)

    raise RuntimeError(
        "BigQuery service account not configured. "
        "Set BIGQUERY_SERVICE_ACCOUNT_JSON or BIGQUERY_SERVICE_ACCOUNT_FILE, "
        f"or ensure default file exists: {default_cred_file}"
    )


def _load_datastore_client() -> datastore.Client:
    """
    Create a Datastore client using the same service-account credentials
    used for BigQuery. Project is fixed to moda-platform-prd.
    """
    credentials = _load_bq_credentials()
    return datastore.Client(project="moda-platform-prd", credentials=credentials)


def _parse_active_details(raw):
    """
    Datastore may store active_ad_account_details as:
    - list[dict]
    - JSON string
    """
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


def _get_workspace_channel_credentials(workspace_id: str) -> List[Dict[str, Optional[str]]]:
    """
    Fetch channel-level credentials for a workspace from Datastore.

    Datastore namespace matches workspace_id.
    Kind: tokenactiveaccount

    Returns a list of dicts with keys:
    - connector
    - login_customer_id
    - customer_id
    - account_id
    - refresh_token
    - token
    """
    client = _load_datastore_client()
    query = client.query(kind="tokenactiveaccount", namespace=workspace_id)
    entities = list(query.fetch())

    if not entities:
        print(f"\n⚠️  No tokenactiveaccount entities found for workspace '{workspace_id}'.")
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
                rows.append(
                    {
                        "connector": connector,
                        "login_customer_id": login,
                        "customer_id": cust,
                        "account_id": acc,
                        "refresh_token": refresh_token,
                        "token": token,
                    }
                )
            continue

        for d in details:
            rows.append(
                {
                    "connector": connector,
                    "login_customer_id": d.get("login_customer_id"),
                    "customer_id": d.get("customer_id"),
                    "account_id": d.get("account_id"),
                    "refresh_token": refresh_token,
                    "token": token,
                }
            )

    print(f"\nLoaded {len(rows)} channel credential row(s) from Datastore for workspace '{workspace_id}'.")
    return rows


def _get_reddit_credentials(workspace_id: str) -> List[Dict[str, Optional[str]]]:
    """
    Fetch Reddit Ads credentials from Datastore.

    Kind:      common_auth_active_account
    Namespace: workspace_id
    Filter:    integration_name == "reddit"

    Each entity may have multiple ad accounts via the account_details JSON field.
    Returns rows in the same format used by _get_workspace_channel_credentials, so
    fetch_reddit_api_daily can consume them without modification:
      connector, account_id, refresh_token, token

    Reddit client_id/client_secret (needed for Basic-auth token refresh) are read from:
      REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET env vars  (preferred)
    """
    client = _load_datastore_client()
    query = client.query(kind="common_auth_active_account", namespace=workspace_id)
    entities = list(query.fetch())

    rows: List[Dict[str, Optional[str]]] = []

    for e in entities:
        integration = str(e.get("integration_name") or "").strip().lower()
        if integration != "reddit":
            continue

        refresh_token = str(e.get("refresh_token") or "").strip()

        # account_details is a JSON-encoded list of account dicts
        account_details_raw = e.get("account_details") or "[]"
        account_details = _parse_active_details(account_details_raw)

        if not account_details:
            # Fall back: treat entity-level account_id if present
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
        print(f"  Reddit: loaded {len(rows)} account(s) from 'common_auth_active_account' for workspace '{workspace_id}'.")
    else:
        print(f"  Reddit: no entities found in 'common_auth_active_account' for workspace '{workspace_id}'.")

    return rows


def fetch_bigquery_metrics(
    workspace_id: str,
    start_date: DateStr,
    end_date: DateStr,
    account_ids: Iterable[str],
    selected_channels: Iterable[str],
) -> DailyMetricsMap:
    """
    Fetch daily metrics from BigQuery for all selected channels in ONE query.

    - Table:  `moda-platform-prd.{workspace_id}.adspends_mat_view`
    - Filter:
        - date between start_date/end_date
        - ad_account_id IN account_ids
        - lower(source) in selected BQ source names inferred from channels
    - Output: map[(date, source, account_id)] -> DailyMetrics

    Large-data notes:
    - Results are fetched in pages (page_size=1000) to avoid loading all rows
      into memory at once.
    - allow_large_results=True lets BigQuery handle very large result sets.
    - The aggregated DailyMetrics dict is much smaller than raw rows because
      rows are already GROUP BY date, source, ad_account_id in the SQL.
    """
    credentials = _load_bq_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Map internal channel keys to expected BQ "source" values (lowercased)
    channel_to_sources: Mapping[str, List[str]] = {
        "google_youtube": ["google", "youtube"],
        "meta": ["facebook", "instagram", "fb", "insta"],
        "microsoft": ["microsoft", "bing"],
        "pinterest": ["pinterest"],
        "snapchat": ["snapchat"],
        "tiktok": ["tiktok"],
        "twitter_x": ["twitter", "x"],
        "amazon": ["amazon"],
        "reddit": ["reddit"],
        "vibe": ["vibe"],
    }

    wanted_sources: Set[str] = set()
    for ch in selected_channels:
        wanted_sources.update(channel_to_sources.get(ch, []))
    if not wanted_sources:
        return {}

    # Clean account IDs (strip spaces/dashes, keep as strings)
    cleaned_ids = [acc.replace("-", "").strip() for acc in account_ids if acc.strip()]

    table_ref = f"`moda-platform-prd.{workspace_id}.adspends_mat_view`"

    source_list_sql = ", ".join([f"'{src}'" for src in wanted_sources])

    if cleaned_ids:
        account_id_list_sql = ", ".join([f"'{aid}'" for aid in cleaned_ids])
        query = f"""
            SELECT
                date,
                LOWER(source) AS source,
                CAST(REPLACE(CAST(ad_account_id AS STRING), '-', '') AS STRING) AS account_id,
                CAST(SUM(impressions) AS FLOAT64) AS impressions,
                CAST(SUM(clicks) AS FLOAT64) AS clicks,
                CAST(SUM(spend) AS FLOAT64) AS spend
            FROM {table_ref}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
              AND REPLACE(CAST(ad_account_id AS STRING), '-', '') IN ({account_id_list_sql})
              AND LOWER(source) IN ({source_list_sql})
            GROUP BY date, source, account_id
            ORDER BY date, source, account_id
        """
    else:
        query = f"""
            SELECT
                date,
                LOWER(source) AS source,
                CAST(REPLACE(CAST(ad_account_id AS STRING), '-', '') AS STRING) AS account_id,
                CAST(SUM(impressions) AS FLOAT64) AS impressions,
                CAST(SUM(clicks) AS FLOAT64) AS clicks,
                CAST(SUM(spend) AS FLOAT64) AS spend
            FROM {table_ref}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
              AND LOWER(source) IN ({source_list_sql})
            GROUP BY date, source, account_id
            ORDER BY date, source, account_id
        """

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        use_query_cache=True,
    )

    progress: Optional[ProgressReporter] = getattr(
        fetch_bigquery_metrics, "_progress", None
    )

    if progress:
        progress.bq_query_start(workspace_id, len(account_ids))

    metrics: DailyMetricsMap = {}

    query_job = client.query(query, job_config=job_config)
    row_count = 0
    for row in query_job.result(page_size=1000):
        row_count += 1
        raw_date = row["date"]
        if hasattr(raw_date, "strftime"):
            date_str = raw_date.strftime("%Y-%m-%d")
        else:
            date_str = str(raw_date)[:10]

        src = str(row["source"]).lower()
        acct = str(row["account_id"] or "")
        key = (date_str, src, acct)
        dm = metrics.setdefault(key, DailyMetrics())
        dm.add(
            int(row["impressions"] or 0),
            int(row["clicks"] or 0),
            float(row["spend"] or 0.0),
        )

    if progress:
        progress.bq_query_done(row_count)

    return metrics


# ---------------------------------------------------------------------------
# Google + YouTube API scraper (adapted & simplified)
# ---------------------------------------------------------------------------

# Google Ads API Constants (from google_and_youtube.py)
GOOGLE_DEVELOPER_TOKEN = "yM_9tlY1lgbgVTdIDbtcpQ"
GOOGLE_API_URL_TEMPLATE = "https://googleads.googleapis.com/v20/customers/{customer_id}/googleAds:searchStream"
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

GOOGLE_METRIC_IMPRESSIONS = "impressions"
GOOGLE_METRIC_CLICKS = "clicks"
GOOGLE_METRIC_COST_MICROS = "cost_micros"


def _google_get_access_token(refresh_token: str) -> str:
    """Get Google Ads access token using refresh token and hardcoded credentials."""
    payload = {
        "grant_type": "refresh_token",
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("Failed to obtain Google Ads access token.")
    return access_token


def _google_fetch_campaign_data_for_account(
    login_customer_id: str,
    access_token: str,
    customer_id: str,
    start_date: DateStr,
    end_date: DateStr,
) -> List[Mapping]:
    """
    Fetch Google Ads campaign data for a single account and date range.
    Returns raw API JSON list of response chunks.
    On HTTP errors (403 Forbidden, 401 Unauthorized, etc.) returns [] so the
    caller can continue with the next account instead of crashing.
    """
    clean_login = login_customer_id.replace("-", "")
    clean_customer = customer_id.replace("-", "")
    url = GOOGLE_API_URL_TEMPLATE.format(customer_id=clean_customer)

    headers = {
        "developer-token": GOOGLE_DEVELOPER_TOKEN,
        "login-customer-id": clean_login,
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.advertising_channel_sub_type,
            segments.ad_network_type,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY segments.date ASC
    """

    resp = requests.post(url, headers=headers, json={"query": query}, timeout=120)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        status_code = resp.status_code
        if status_code == 403:
            print(
                f"  ⚠️  403 Forbidden for account {customer_id} — "
                "account may not be accessible with this token/developer-token. Skipping."
            )
        elif status_code == 401:
            print(
                f"  ⚠️  401 Unauthorized for account {customer_id} — "
                "access token may be expired or invalid. Skipping."
            )
        elif status_code == 429:
            print(
                f"  ⚠️  429 Rate Limited for account {customer_id} — "
                "too many requests. Skipping this account for now."
            )
        else:
            print(
                f"  ⚠️  HTTP {status_code} error for account {customer_id}: {http_err}. Skipping."
            )
        return []
    data = resp.json()
    if not isinstance(data, list):
        return []
    return data


def _google_aggregate_api_daily(
    raw_chunks: Iterable[Mapping],
) -> Dict[str, Dict[str, DailyMetrics]]:
    """
    Aggregate raw Google Ads API chunks into daily metrics split into
    'google' and 'youtube' sources.

    Returns:
        {
          "google": {date: DailyMetrics, ...},
          "youtube": {date: DailyMetrics, ...}
        }
    """
    yt_daily: Dict[str, DailyMetrics] = {}
    gg_daily: Dict[str, DailyMetrics] = {}

    for chunk in raw_chunks:
        results = chunk.get("results", [])
        for res in results:
            metrics = res.get("metrics", {}) or {}
            segments = res.get("segments", {}) or {}
            date = segments.get("date")
            if not date:
                continue

            ad_network_type = (segments.get("adNetworkType") or "").upper()
            impressions = int(metrics.get("impressions", 0) or 0)
            clicks = int(metrics.get("clicks", 0) or 0)
            cost_micros_raw = metrics.get("costMicros", 0) or 0
            cost_micros = decimal.Decimal(str(cost_micros_raw))
            spend = float(cost_micros / decimal.Decimal("1000000"))

            if "YOUTUBE" in ad_network_type:
                bucket = yt_daily.setdefault(date, DailyMetrics())
            else:
                bucket = gg_daily.setdefault(date, DailyMetrics())

            bucket.add(impressions, clicks, spend)

    return {"google": gg_daily, "youtube": yt_daily}


def fetch_google_youtube_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """
    Fetch and aggregate Google + YouTube daily metrics across one or more ad
    accounts using channel credentials from Datastore.
    """
    # channel_credentials is loaded from Datastore via workspace; we expect:
    # - account_id: customer account ID
    # - login_customer_id: manager account ID
    # - refresh_token or token: OAuth2 refresh token
    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_google_youtube_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_google_youtube_api_daily, "_progress", None
    )
    if not channel_credentials:
        print("No Google Ads credentials found for this workspace; skipping Google Ads.")
        return {}

    # Group by (refresh_token, login_customer_id) so that each group can be fetched with one access token
    grouped_accounts: Dict[Tuple[str, str], List[str]] = {}
    for row in channel_credentials:
        # Values from Datastore can be strings, ints, or None; always coerce to str
        refresh_token_raw = (row.get("refresh_token") or row.get("token") or "")
        login_customer_id_raw = (row.get("login_customer_id") or row.get("customer_id") or "")
        account_id_raw = (row.get("account_id") or "")

        refresh_token = str(refresh_token_raw).strip()
        login_customer_id = str(login_customer_id_raw).strip()
        account_id = str(account_id_raw).strip()

        if not refresh_token or not login_customer_id or not account_id:
            continue

        key = (refresh_token, login_customer_id)
        grouped_accounts.setdefault(key, []).append(account_id)

    if not grouped_accounts:
        print(
            "Google Ads credentials found in Datastore but missing required fields; skipping Google Ads."
        )
        return {}

    # Combined metrics for all accounts in this channel
    combined: DailyMetricsMap = {}
    total_unique_accounts = sum(len(accs) for accs in grouped_accounts.values())
    if progress:
        progress.start_channel("Google Ads (Google + YouTube)", total_unique_accounts)

    current_account_idx = 0
    for (refresh_token, login_customer_id), account_ids in grouped_accounts.items():
        try:
            access_token = _google_get_access_token(refresh_token)
        except Exception as token_err:
            print(f"  ⚠️  Failed to obtain Google Ads access token: {token_err}. Skipping group.")
            continue

        for acc in account_ids:
            current_account_idx += 1
            if progress:
                progress.start_account(acc, current_account_idx, total_unique_accounts)
            else:
                print(f"\nFetching Google Ads data for account {acc} ...")
            
            clean_acc = acc.replace("-", "").replace("act_", "").strip()
            
            # Collect for BigQuery
            if account_ids_collector is not None:
                if clean_acc:
                    account_ids_collector.add(clean_acc)

            try:
                raw_chunks = _google_fetch_campaign_data_for_account(
                    login_customer_id=login_customer_id,
                    access_token=access_token,
                    customer_id=acc,
                    start_date=start_date,
                    end_date=end_date,
                )
                per_source = _google_aggregate_api_daily(raw_chunks)
                acct_rows = 0
                for source, daily in per_source.items():
                    for date, dm in daily.items():
                        key = (date, source, clean_acc)
                        bucket = combined.setdefault(key, DailyMetrics())
                        bucket.add(dm.impressions, dm.clicks, dm.spend)
                        acct_rows += 1
                
                if progress:
                    progress.account_done(acc, acct_rows)
            except Exception as acc_err:
                if progress:
                    progress.account_error(acc, 0, str(acc_err))
                else:
                    print(f"  ⚠️  Unexpected error for account {acc}: {acc_err}. Skipping.")

    if progress:
        progress.done_channel("Google Ads (Google + YouTube)", len(combined))
    return combined


# ---------------------------------------------------------------------------
# Meta (Facebook + Instagram) API scraper
# ---------------------------------------------------------------------------

# Meta API Constants (from meta_fb_and_insta.py)
META_CLIENT_ID = os.getenv("META_CLIENT_ID", "")
META_CLIENT_SECRET = os.getenv("META_CLIENT_SECRET", "")
META_API_VERSION = "v22.0"


def fetch_meta_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Meta (Facebook + Instagram) API data and normalize to daily metrics.
    Adapted from meta_fb_and_insta.py

    Error handling:
    - ReadTimeout: retried up to META_MAX_RETRIES times with exponential backoff.
    - 429 Rate Limit: waits META_RATE_LIMIT_SLEEP seconds then retries.
    - 403/401: chunk is skipped with a warning (token expired or account inaccessible).
    - Other HTTP errors: chunk is skipped with a warning.
    """
    from datetime import datetime, timedelta
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError
    import time

    META_TIMEOUT = 90           # seconds per request (increased from 30)
    META_MAX_RETRIES = 3        # max retries on timeout/connection error
    META_RETRY_BACKOFF = 10     # seconds backoff base (doubles each retry)
    META_RATE_LIMIT_SLEEP = 60  # seconds to wait on 429

    print("\n=== Meta (Facebook + Instagram) configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_meta_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_meta_api_daily, "_progress", None
    )

    if not channel_credentials:
        print("No Meta credentials found for this workspace; skipping Meta.")
        return {}

    # Group by access_token because multiple integrations may exist
    grouped_accounts: Dict[str, List[str]] = {}
    for row in channel_credentials:
        token_raw = row.get("token") or row.get("refresh_token") or ""
        acc_raw = row.get("account_id") or ""

        at = os.getenv("META_ACCESS_TOKEN") or str(token_raw).strip()
        acc = str(acc_raw).strip().replace("act_", "")

        if at and acc:
            grouped_accounts.setdefault(at, []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required Meta configuration (access token or account IDs). Skipping Meta.")
        return {}

    # Collect all account IDs for BigQuery filter
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for base_id in accs:
                account_ids_collector.add(base_id)

    combined: DailyMetricsMap = {}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    CHUNK_DAYS = 6  # 7-day chunks

    # urllib3-level retry only covers connection-level issues; we handle
    # application-level retries (timeout, 429) manually below.
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504]),
    )
    session.mount("https://", adapter)

    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0
    if progress:
        progress.start_channel("Meta (Facebook + Instagram)", total_accounts)

    for access_token, base_ids in grouped_accounts.items():
        for base_id in base_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(base_id, current_acct_idx, total_accounts)
            
            api_id = "act_" + base_id
            current = start_dt
            acct_rows = 0

            while current <= end_dt:
                chunk_end = min(current + timedelta(days=CHUNK_DAYS), end_dt)
                chunk_start_str = current.strftime("%Y-%m-%d")
                chunk_end_str = chunk_end.strftime("%Y-%m-%d")

                url = f"https://graph.facebook.com/{META_API_VERSION}/{api_id}/insights"
                fields_list = [
                    "campaign_name", "campaign_id", "adset_name", "ad_name", "ad_id",
                    "date_start", "date_stop", "impressions", "spend", "inline_link_clicks"
                ]
                params = {
                    "access_token": access_token,
                    "time_range": json.dumps({"since": chunk_start_str, "until": chunk_end_str}),
                    "fields": ",".join(fields_list),
                    "breakdowns": "publisher_platform",
                    "level": "ad",
                    "limit": 500,
                    "time_increment": "1",
                }

                next_url = url
                current_params = params.copy()

                while next_url:
                    attempt = 0
                    success = False
                    while attempt <= META_MAX_RETRIES:
                        try:
                            r = session.get(next_url, params=current_params, timeout=META_TIMEOUT)

                            # --- 429 Rate Limit ---
                            if r.status_code == 429:
                                wait = META_RATE_LIMIT_SLEEP * (attempt + 1)
                                if progress:
                                    progress.account_retry(base_id, attempt + 1, META_MAX_RETRIES, wait, "Rate Limit (429)")
                                else:
                                    print(f"  ⚠️  Meta 429 Rate Limit for {base_id} ({chunk_start_str} to {chunk_end_str}). Waiting {wait}s...")
                                time.sleep(wait)
                                attempt += 1
                                continue

                            # --- 403 / 401 ---
                            if r.status_code in (401, 403):
                                msg = "token may be expired or account inaccessible"
                                if progress:
                                    progress.account_error(base_id, r.status_code, msg)
                                else:
                                    print(f"  ⚠️  Meta HTTP {r.status_code} for {api_id}: {msg}. Skipping chunk.")
                                next_url = None
                                success = True
                                break

                            r.raise_for_status()
                            data = r.json()
                            rows_data = data.get("data", [])

                            if progress:
                                progress.chunk_progress(base_id, chunk_start_str, chunk_end_str, 1)
                            
                            for row in rows_data:
                                platform_raw = str(row.get("publisher_platform", "")).lower().strip()
                                date_str = row.get("date_start")
                                if not date_str:
                                    continue
                                source = "instagram" if platform_raw == "instagram" else "facebook"

                                imp = int(row.get("impressions", 0) or 0)
                                clk = int(row.get("inline_link_clicks", 0) or 0)
                                spd = float(row.get("spend", 0) or 0)

                                key = (date_str, source, base_id)
                                dm = combined.setdefault(key, DailyMetrics())
                                dm.add(imp, clk, spd)
                                acct_rows += 1

                            next_url = data.get("paging", {}).get("next")
                            current_params = {}
                            success = True
                            break

                        except (ReadTimeout, RequestsConnectionError) as timeout_err:
                            wait = META_RETRY_BACKOFF * (2 ** attempt)
                            if progress:
                                progress.account_retry(base_id, attempt + 1, META_MAX_RETRIES, wait, str(timeout_err))
                            else:
                                print(f"  ⚠️  Meta timeout for {api_id}: {timeout_err}. Retry {attempt + 1}...")
                            time.sleep(wait)
                            attempt += 1

                        except Exception as e:
                            if progress:
                                progress.account_error(base_id, 0, str(e))
                            else:
                                print(f"  ⚠️  Meta error for {api_id}: {e}. Skipping chunk.")
                            next_url = None
                            success = True
                            break

                    if not success:
                        if progress:
                            progress.account_error(base_id, 408, f"Max retries reached ({META_MAX_RETRIES})")
                        next_url = None

                current = chunk_end + timedelta(days=1)
                time.sleep(0.5)

            if progress:
                progress.account_done(base_id, acct_rows)

    if progress:
        progress.done_channel("Meta (Facebook + Instagram)", len(combined))
    return combined


# ---------------------------------------------------------------------------
# Microsoft / Bing API scraper
# ---------------------------------------------------------------------------

# Microsoft API Constants (from microsoft_bing.py)
MICROSOFT_CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID", "")
MICROSOFT_CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET", "")
MICROSOFT_DEVELOPER_TOKEN = "120JJ68Q4B056855"
MICROSOFT_SCOPE = "https://ads.microsoft.com/msads.manage"
MICROSOFT_TOKEN_ENDPOINT = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
MICROSOFT_REPORT_SUBMIT_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit"
MICROSOFT_REPORT_POLL_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll"


def fetch_microsoft_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Microsoft Ads API data and normalize to daily metrics.
    Adapted from microsoft_bing.py
    """
    import html
    import zipfile
    import io
    import threading
    import time
    from datetime import datetime

    print("\n=== Microsoft Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_microsoft_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_microsoft_api_daily, "_progress", None
    )

    if not channel_credentials:
        if progress:
            progress.skip_channel("Microsoft Ads")
        else:
            print("No Microsoft Ads credentials found for this workspace; skipping Microsoft Ads.")
        return {}

    if progress:
        progress.start_channel("Microsoft Ads", len(channel_credentials))

    # Group by (refresh_token, login_customer_id) because multiple manager IDs may exist
    grouped_accounts: Dict[Tuple[str, str], List[str]] = {}
    for row in channel_credentials:
        rt_raw = row.get("refresh_token") or row.get("token") or ""
        lc_raw = row.get("login_customer_id") or row.get("customer_id") or ""
        acc_raw = row.get("account_id") or ""

        rt = os.getenv("MICROSOFT_REFRESH_TOKEN") or str(rt_raw).strip()
        lc = str(lc_raw).strip()
        acc = str(acc_raw).strip()

        if rt and lc and acc:
            grouped_accounts.setdefault((rt, lc), []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required Microsoft Ads configuration (refresh token, login customer ID, or account IDs). Skipping Microsoft.")
        return {}

    # Collect all account IDs for BigQuery filter
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for acc in accs:
                cleaned = acc.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    # Token manager
    class TokenManager:
        def __init__(self, refresh_token):
            self.refresh_token = refresh_token
            self.access_token = None
            self.token_expiry = 0
            self.lock = threading.Lock()

        def get_token(self):
            with self.lock:
                if time.time() >= self.token_expiry - 300:
                    self._refresh_token()
                return self.access_token

        def _refresh_token(self):
            payload = {
                "client_id": MICROSOFT_CLIENT_ID,
                "client_secret": MICROSOFT_CLIENT_SECRET,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
                "scope": MICROSOFT_SCOPE
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            resp = requests.post(MICROSOFT_TOKEN_ENDPOINT, data=payload, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            self.access_token = data.get("access_token")
            self.token_expiry = time.time() + data.get("expires_in", 3600)

    token_managers: Dict[str, TokenManager] = {}
    combined: DailyMetricsMap = {}
    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0

    for (refresh_token, login_customer_id), account_ids in grouped_accounts.items():
        if refresh_token not in token_managers:
            token_managers[refresh_token] = TokenManager(refresh_token)
        tm = token_managers[refresh_token]

        for account_id in account_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(account_id, current_acct_idx, total_accounts)
            else:
                print(f"\nFetching Microsoft Ads data for account {account_id}...")
            
            # Submit report
            try:
                access_token = tm.get_token()
            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, f"Token error: {e}")
                else:
                    print(f"  ⚠️  Failed to refresh Microsoft token: {e}")
                continue
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "DeveloperToken": MICROSOFT_DEVELOPER_TOKEN,
                "CustomerId": login_customer_id,
                "CustomerAccountId": account_id,
                "Content-Type": "application/json",
            }
        
            y1, m1, d1 = map(int, start_date.split("-"))
            y2, m2, d2 = map(int, end_date.split("-"))
            payload = {
                "ReportRequest": {
                    "ExcludeColumnHeaders": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": False,
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": f"FullReport_{account_id}",
                    "ReturnOnlyCompleteData": True,
                    "Type": "AdPerformanceReportRequest",
                    "Aggregation": "Daily",
                    "Columns": ["CampaignId", "CampaignName", "TimePeriod", "Impressions", "Clicks", "Spend"],
                    "Scope": {"AccountIds": [account_id]},
                    "Time": {
                        "CustomDateRangeStart": {"Year": y1, "Month": m1, "Day": d1},
                        "CustomDateRangeEnd": {"Year": y2, "Month": m2, "Day": d2}
                    }
                }
            }
            
            try:
                resp = requests.post(MICROSOFT_REPORT_SUBMIT_URL, headers=headers, json=payload, timeout=30)
                resp.raise_for_status()
                request_id = resp.json().get("ReportRequestId")
                
                # Poll for completion
                start_time = time.time()
                poll_wait = 5
                location = None
                
                while (time.time() - start_time) < (30 * 60):  # 30 min timeout
                    access_token = tm.get_token()
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "DeveloperToken": MICROSOFT_DEVELOPER_TOKEN,
                        "CustomerId": login_customer_id,
                        "CustomerAccountId": account_id,
                        "Content-Type": "application/json",
                    }
                    
                    r = requests.post(MICROSOFT_REPORT_POLL_URL, headers=headers, json={"ReportRequestId": request_id}, timeout=20)
                    r.raise_for_status()
                    data = r.json()
                    status = data.get("ReportRequestStatus", {}).get("Status")
                    
                    if status == "Success":
                        location = data.get("ReportRequestStatus", {}).get("ReportDownloadUrl")
                        break
                    if status == "Error":
                        raise Exception("API returned Error status")
                    
                    time.sleep(poll_wait)
                    poll_wait = min(poll_wait * 1.5, 60)
                
                acct_rows = 0
                if location:
                    import tempfile
                    clean_url = html.unescape(location)
                    r = requests.get(clean_url, stream=True, timeout=300)
                    r.raise_for_status()

                    tmp_zip_path = None
                    try:
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
                            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                                if chunk:
                                    tmp_zip.write(chunk)
                            tmp_zip_path = tmp_zip.name

                        with zipfile.ZipFile(tmp_zip_path) as z:
                            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                            if csv_files:
                                with z.open(csv_files[0], "r") as f:
                                    # Read everything once to avoid seek(0) which fails on some Zip streams
                                    raw_content = f.read().decode('utf-8', errors='replace')
                                    
                                    # Skip Microsoft's header junk
                                    lines = raw_content.splitlines()
                                    header_row_idx = 0
                                    for idx, line in enumerate(lines):
                                        if "CampaignId" in line:
                                            header_row_idx = idx
                                            break
                                    
                                    import csv as csv_module
                                    reader = csv_module.DictReader(lines[header_row_idx:])

                                    for row in reader:
                                        date_str = row.get("TimePeriod", "")
                                        if not date_str:
                                            continue
                                        try:
                                            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                            clean_date = date_obj.strftime("%Y-%m-%d")
                                        except:
                                            continue
                                        
                                        imp = int(row.get("Impressions", "0").replace(",", "") or 0)
                                        clk = int(row.get("Clicks", "0").replace(",", "") or 0)
                                        spd = float(row.get("Spend", "0").replace(",", "") or 0.0)
                                        
                                        key = (clean_date, "microsoft", str(account_id))
                                        dm = combined.setdefault(key, DailyMetrics())
                                        dm.add(imp, clk, spd)
                                        acct_rows += 1

                    finally:
                        if tmp_zip_path and os.path.exists(tmp_zip_path):
                            os.unlink(tmp_zip_path)
                
                if progress:
                    progress.account_done(account_id, acct_rows)

            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, str(e))
                else:
                    print(f"Warning: Failed to fetch Microsoft data for account {account_id}: {e}")

    return combined


# ---------------------------------------------------------------------------
# Pinterest API scraper
# ---------------------------------------------------------------------------

# Pinterest API Constants (from pinterest.py)
PINTEREST_API_BASE_URL = "https://api.pinterest.com/v5"
PINTEREST_MAX_POLLS = 30
PINTEREST_POLL_INTERVAL = 5


def fetch_pinterest_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Pinterest Ads API data and normalize to daily metrics.
    Adapted from pinterest.py
    """
    import time

    print("\n=== Pinterest Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_pinterest_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_pinterest_api_daily, "_progress", None
    )

    if not channel_credentials:
        if progress:
            progress.skip_channel("Pinterest Ads")
        else:
            print("No Pinterest Ads credentials found for this workspace; skipping Pinterest.")
        return {}
    
    # Group by bearer_token because multiple integrations may exist
    grouped_accounts: Dict[str, List[str]] = {}
    for row in channel_credentials:
        token_raw = row.get("token") or row.get("refresh_token") or ""
        acc_raw = row.get("account_id") or ""

        bt = os.getenv("PINTEREST_BEARER_TOKEN") or str(token_raw).strip()
        acc = str(acc_raw).strip()

        if bt and acc:
            grouped_accounts.setdefault(bt, []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required Pinterest Ads configuration (bearer token or account IDs). Skipping Pinterest.")
        return {}

    # Collect all account IDs for BigQuery filter
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for acc in accs:
                cleaned = acc.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0

    if progress:
        progress.start_channel("Pinterest Ads", total_accounts)

    for bearer_token, account_ids in grouped_accounts.items():
        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json'
        }

        for account_id in account_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(account_id, current_acct_idx, total_accounts)
            else:
                print(f"\nFetching Pinterest data for account: {account_id}")
            
            # Submit report
            report_url = f"{PINTEREST_API_BASE_URL}/ad_accounts/{account_id}/reports"
            payload = {
                "start_date": start_date,
                "end_date": end_date,
                "granularity": "DAY",
                "level": "CAMPAIGN",
                "columns": [
                    "SPEND_IN_DOLLAR",
                    "TOTAL_IMPRESSION",
                    "TOTAL_CLICKTHROUGH",
                    "DATE"
                ],
                "click_window_days": 30,
                "engagement_window_days": 30,
                "view_window_days": 1,
                "conversion_report_time": "TIME_OF_AD_ACTION",
                "report_format": "JSON"
            }
            
            try:
                resp = requests.post(report_url, headers=headers, json=payload, timeout=120)
                resp.raise_for_status()
                report_response = resp.json()
                
                if 'token' in report_response:
                    token = report_response['token']
                    
                    # Poll for completion
                    poll_url = f"{PINTEREST_API_BASE_URL}/ad_accounts/{account_id}/reports?token={token}"
                    poll_headers = {'Authorization': f'Bearer {bearer_token}'}
                    
                    acct_rows = 0
                    success = False
                    for attempt in range(PINTEREST_MAX_POLLS):
                        poll_resp = requests.get(poll_url, headers=poll_headers, timeout=120)
                        poll_resp.raise_for_status()
                        data = poll_resp.json()
                        report_status = data.get('report_status', 'UNKNOWN')
                        
                        if report_status == 'FINISHED':
                            download_url = data.get('url')
                            if download_url:
                                download_resp = requests.get(download_url, timeout=120)
                                download_resp.raise_for_status()
                                report_data = download_resp.json()
                                
                                # Handle both list and dict responses (Pinterest API varies)
                                records = []
                                if isinstance(report_data, list):
                                    records = report_data
                                elif isinstance(report_data, dict):
                                    # Try common keys
                                    for k in ['data', 'rows', 'results', 'items']:
                                        if k in report_data and isinstance(report_data[k], list):
                                            records = report_data[k]
                                            break
                                    # If still empty, maybe it's keyed by ID?
                                    if not records:
                                        # Check if values are dicts/lists
                                        sample = next(iter(report_data.values()), None) if report_data else None
                                        if sample and (isinstance(sample, dict) or isinstance(sample, list)):
                                            for val in report_data.values():
                                                if isinstance(val, list): records.extend(val)
                                                elif isinstance(val, dict): records.append(val)

                                for record in records:
                                    date_str = record.get("DATE")
                                    if not date_str:
                                        continue
                                    try:
                                        from datetime import datetime
                                        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                        clean_date = date_obj.strftime("%Y-%m-%d")
                                    except:
                                        continue
                                    
                                    imp = int(record.get("TOTAL_IMPRESSION", 0) or 0)
                                    clk = int(record.get("TOTAL_CLICKTHROUGH", 0) or 0)
                                    spd = float(record.get("SPEND_IN_DOLLAR", 0.0) or 0.0)
                                    
                                    key = (clean_date, "pinterest", str(account_id))
                                    dm = combined.setdefault(key, DailyMetrics())
                                    dm.add(imp, clk, spd)
                                    acct_rows += 1
                                success = True
                                break
                        
                        elif report_status in ['FAILED', 'CANCELLED']:
                            if progress:
                                progress.account_error(account_id, 0, f"Report {report_status}")
                            else:
                                print(f"  ⚠️  Pinterest report failed or cancelled for {account_id}")
                            break
                        
                        time.sleep(PINTEREST_POLL_INTERVAL)
                    
                    if success:
                        if progress:
                            progress.account_done(account_id, acct_rows)
                    else:
                        if progress:
                            progress.account_error(account_id, 0, "Polling timed out")

            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, str(e))
                else:
                    print(f"  ⚠️  Pinterest error for account {account_id}: {e}")

    return combined


# ---------------------------------------------------------------------------
# Snapchat API scraper
# ---------------------------------------------------------------------------

# Snapchat API Constants (from snapchat.py)
SNAPCHAT_CLIENT_ID = os.getenv("SNAPCHAT_CLIENT_ID", "")
SNAPCHAT_CLIENT_SECRET = os.getenv("SNAPCHAT_CLIENT_SECRET", "")
SNAPCHAT_TOKEN_URL = "https://accounts.snapchat.com/accounts/oauth2/token"
SNAPCHAT_BASE_URL = "https://adsapi.snapchat.com/v1"


def fetch_snapchat_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Snapchat Ads API data and normalize to daily metrics.
    Adapted from snapchat.py
    """
    import time
    import threading
    from datetime import datetime, timedelta

    print("\n=== Snapchat Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_snapchat_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_snapchat_api_daily, "_progress", None
    )

    if not channel_credentials:
        if progress:
            progress.skip_channel("Snapchat Ads")
        else:
            print("No Snapchat Ads credentials found for this workspace; skipping Snapchat.")
        return {}

    if progress:
        progress.start_channel("Snapchat Ads", len(channel_credentials))

    # Group by refresh_token because multiple integrations may exist
    grouped_accounts: Dict[str, List[str]] = {}
    for row in channel_credentials:
        rt_raw = row.get("refresh_token") or row.get("token") or ""
        acc_raw = row.get("account_id") or ""

        rt = os.getenv("SNAPCHAT_REFRESH_TOKEN") or str(rt_raw).strip()
        acc = str(acc_raw).strip()

        if rt and acc:
            grouped_accounts.setdefault(rt, []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required Snapchat Ads configuration (refresh token or ad account IDs). Skipping Snapchat.")
        return {}

    # Collect all account IDs for BigQuery filter
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for ad_account_id in accs:
                cleaned = ad_account_id.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    # Auth class
    class SnapchatAuth:
        def __init__(self, refresh_token):
            self.refresh_token = refresh_token
            self.access_token = None
            self.token_lock = threading.Lock()

        def get_access_token(self):
            with self.token_lock:
                payload = {
                    "grant_type": "refresh_token",
                    "client_id": SNAPCHAT_CLIENT_ID,
                    "client_secret": SNAPCHAT_CLIENT_SECRET,
                    "refresh_token": self.refresh_token
                }
                r = requests.post(SNAPCHAT_TOKEN_URL, data=payload, timeout=60)
                r.raise_for_status()
                self.access_token = r.json()["access_token"]
                return self.access_token

        def headers(self):
            if not self.access_token:
                self.get_access_token()
            return {"Authorization": f"Bearer {self.access_token}"}

    combined: DailyMetricsMap = {}
    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0

    for refresh_token, ad_account_ids in grouped_accounts.items():
        auth = SnapchatAuth(refresh_token)
        
        # Split date range into 32-day chunks
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        date_ranges = []
        current_start = start_dt
        while current_start <= end_dt:
            current_end = min(current_start + timedelta(days=31), end_dt)
            date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
            current_start = current_end + timedelta(days=1)
        
        for ad_account_id in ad_account_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(ad_account_id, current_acct_idx, total_accounts)
            else:
                print(f"\nFetching Snapchat Ads data for account {ad_account_id}...")
            
            # Fetch campaign IDs
            try:
                url = f"{SNAPCHAT_BASE_URL}/adaccounts/{ad_account_id}/campaigns"
                r = requests.get(url, headers=auth.headers(), timeout=30)
                r.raise_for_status()
                campaigns = r.json().get("campaigns", [])
                campaign_ids = [c["campaign"]["id"] for c in campaigns]
            except Exception as e:
                if progress:
                    progress.account_error(ad_account_id, 0, f"Campaign fetch error: {e}")
                else:
                    print(f"Warning: Failed to fetch campaign IDs for Snapchat account {ad_account_id}: {e}")
                continue
            
            acct_rows = 0
            
            # Fetch stats for each campaign and date range
            for campaign_id in campaign_ids:
                for chunk_start, chunk_end in date_ranges:
                    chunk_end_api = (datetime.strptime(chunk_end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                    url = f"{SNAPCHAT_BASE_URL}/campaigns/{campaign_id}/stats"
                    params = {
                        "granularity": "DAY",
                        "fields": "impressions,spend,swipes",
                        "start_time": chunk_start,
                        "end_time": chunk_end_api
                    }
                    
                    try:
                        r = requests.get(url, headers=auth.headers(), params=params, timeout=30)
                        if r.status_code == 401:
                            auth.get_access_token()
                            r = requests.get(url, headers=auth.headers(), params=params, timeout=30)
                        r.raise_for_status()
                        data = r.json()
                        
                        if "timeseries_stats" in data:
                            for day in data["timeseries_stats"][0]["timeseries_stat"]["timeseries"]:
                                stats = day.get("stats", {})
                                day_date = day["start_time"][:10]
                                if chunk_start <= day_date <= chunk_end:
                                    imp = int(stats.get("impressions", 0) or 0)
                                    clk = int(stats.get("swipes", 0) or 0)
                                    spd = float(stats.get("spend", 0) or 0) / 1000000.0
                                    
                                    key = (day_date, "snapchat", str(ad_account_id))
                                    dm = combined.setdefault(key, DailyMetrics())
                                    dm.add(imp, clk, spd)
                                    acct_rows += 1
                    except Exception as e:
                        print(f"Warning: Failed to fetch Snapchat stats for campaign {campaign_id}: {e}")
                    
                    time.sleep(0.2)
            
            if progress:
                progress.account_done(ad_account_id, acct_rows)

    return combined


# ---------------------------------------------------------------------------
# TikTok API scraper
# ---------------------------------------------------------------------------

# PROXY CONFIGURATION REMOVED
# The user will rely on a PC-level VPN going forward.

def fetch_tiktok_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch TikTok Ads API data and normalize to daily metrics.
    Adapted from tiktok.py
    """
    print("\n=== TikTok Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_tiktok_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_tiktok_api_daily, "_progress", None
    )

    if not channel_credentials:
        if progress:
            progress.skip_channel("TikTok Ads")
        else:
            print("No TikTok Ads credentials found for this workspace; skipping TikTok.")
        return {}

    if progress:
        progress.start_channel("TikTok Ads", len(channel_credentials))

    # Group by access_token because multiple integrations may exist
    grouped_accounts: Dict[str, List[str]] = {}
    for row in channel_credentials:
        token_raw = row.get("token") or row.get("refresh_token") or ""
        acc_raw = row.get("account_id") or ""

        at = os.getenv("TIKTOK_ACCESS_TOKEN") or str(token_raw).strip()
        acc = str(acc_raw).strip()

        if at and acc:
            grouped_accounts.setdefault(at, []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required TikTok Ads configuration (access token or advertiser IDs). Skipping TikTok.")
        return {}

    # Collect all account IDs for BigQuery filter
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for advertiser_id in accs:
                cleaned = advertiser_id.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0

    for access_token, advertiser_ids in grouped_accounts.items():
        headers = {"Access-Token": access_token}

        for advertiser_id in advertiser_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(advertiser_id, current_acct_idx, total_accounts)
            else:
                print(f"\nFetching TikTok data for advertiser {advertiser_id}...")
            
            dimensions_list = ["stat_time_day"]
            metrics_list = ["impressions", "clicks", "spend"]
            # TikTok API allows max 30 days for stat_time_day
            start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
            
            acct_rows = 0
            curr_start = start_dt
            while curr_start <= end_dt:
                curr_end = min(curr_start + dt.timedelta(days=29), end_dt)
                
                params = {
                    "advertiser_id": advertiser_id,
                    "service_type": "AUCTION",
                    "report_type": "BASIC",
                    "data_level": "AUCTION_CAMPAIGN",
                    "dimensions": json.dumps(dimensions_list),
                    "metrics": json.dumps(metrics_list),
                    "time_granularity": "DAILY",
                    "start_date": curr_start.strftime("%Y-%m-%d"),
                    "end_date": curr_end.strftime("%Y-%m-%d"),
                    "page": 1,
                    "page_size": 1000,
                }
                
                max_retries = 5
                import time
                import random
                
                success = False
                for attempt in range(max_retries):
                    try:
                        # Bypass system OS proxy to rely strictly on VPN
                        resp = requests.get(url, headers=headers, params=params, timeout=60, proxies={"http": None, "https": None})
                        resp.raise_for_status()
                        resp_json = resp.json()
                        
                        if resp_json.get("code") != 0:
                            err_msg = resp_json.get('message', 'Unknown error')
                            print(f"  TikTok API error (attempt {attempt+1}): {err_msg}")
                            if attempt == max_retries - 1:
                                if progress:
                                    progress.account_error(advertiser_id, 0, f"API error: {err_msg}")
                                else:
                                    print(f"Warning: TikTok API error: {err_msg}")
                                break # Stop retrying if API returns error code
                            time.sleep(random.uniform(1, 3))
                            continue
                        
                        data_list = resp_json.get("data", {}).get("list", [])
                        for record in data_list:
                            dimensions = record.get("dimensions", {})
                            metrics = record.get("metrics", {})
                            date = dimensions.get("stat_time_day")
                            
                            if not date:
                                continue
                                
                            # Force YYYY-MM-DD string to match BigQuery precisely
                            date = str(date)[:10]
                            
                            imp = int(metrics.get("impressions", 0) or 0)
                            clk = int(metrics.get("clicks", 0) or 0)
                            spd = float(metrics.get("spend", 0.0) or 0.0)
                            
                            key = (date, "tiktok", str(advertiser_id))
                            dm = combined.setdefault(key, DailyMetrics())
                            dm.add(imp, clk, spd)
                            acct_rows += 1
                        success = True
                        break # Success, exit retry loop

                    except Exception as e:
                        print(f"  TikTok request error (attempt {attempt+1}): {e}")
                        if attempt == max_retries - 1:
                            if progress:
                                progress.account_error(advertiser_id, 0, str(e))
                            else:
                                print(f"Warning: Failed to fetch TikTok data for advertiser {advertiser_id}: {e}")
                        time.sleep(random.uniform(1, 3))
                
                if not success:
                    print(f"  Warning: Skipping dates {curr_start.strftime('%Y-%m-%d')} to {curr_end.strftime('%Y-%m-%d')} for {advertiser_id} after {max_retries} proxy failures.")
                    # Do NOT break; continue to the next date chunk so we don't drop the rest of the calendar.
                
                curr_start = curr_end + dt.timedelta(days=1)
            
            if acct_rows > 0:
                if progress:
                    progress.account_done(advertiser_id, acct_rows)

    return combined


# ---------------------------------------------------------------------------
# Twitter / X API scraper
# ---------------------------------------------------------------------------

# Twitter/X API Constants (from twitter(x).py)
TWITTER_CONSUMER_KEY = "OKjnm0fAt6tYgM1DySQe2qEpT"
TWITTER_CONSUMER_SECRET = "F4O5DyUXMwjP68kBF1Vpe5ybsiycCRZCztz1vpnZVYq5ca9B2W"
TWITTER_API_BASE = "https://ads-api.twitter.com/12"


def fetch_twitter_x_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Twitter/X Ads API data and normalize to daily metrics.
    Adapted from twitter(x).py
    """
    import gzip
    import time
    from datetime import datetime, timedelta
    from io import BytesIO
    from requests_oauthlib import OAuth1

    print("\n=== Twitter/X Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_twitter_x_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_twitter_x_api_daily, "_progress", None
    )

    if not channel_credentials:
        if progress:
            progress.skip_channel("Twitter/X Ads")
        else:
            print("No Twitter/X Ads credentials found for this workspace; skipping.")
        return {}

    # Group by (access_token, access_token_secret) because multiple integrations may exist
    grouped_accounts: Dict[Tuple[str, str], List[str]] = {}
    for row in channel_credentials:
        # Assume token = access token, refresh_token = access token secret
        at_raw = row.get("token") or ""
        ats_raw = row.get("refresh_token") or ""
        acc_raw = row.get("account_id") or ""

        at = str(at_raw).strip()
        ats = str(ats_raw).strip()
        acc = str(acc_raw).strip()

        if at and ats and acc:
            grouped_accounts.setdefault((at, ats), []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required Twitter/X Ads configuration (account ID or tokens). Skipping Twitter/X.")
        return {}

    # Collect all account IDs for BigQuery filter
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for account_id in accs:
                cleaned = account_id.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0

    if progress:
        progress.start_channel("Twitter/X Ads", total_accounts)

    for (access_token, access_token_secret), account_ids in grouped_accounts.items():
        auth = OAuth1(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, access_token, access_token_secret)
        
        for account_id in account_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(account_id, current_acct_idx, total_accounts)
            else:
                print(f"\nFetching Twitter/X data for account: {account_id}")
            
            # Get active campaigns
            try:
                url = f"{TWITTER_API_BASE}/accounts/{account_id}/campaigns"
                params = {"with_deleted": False}
                resp = requests.get(url, auth=auth, params=params, timeout=60)
                resp.raise_for_status()
                campaigns = resp.json().get('data', [])
                campaign_ids = [c['id'] for c in campaigns if c.get('entity_status') == 'ACTIVE']
            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, f"Campaign fetch error: {e}")
                else:
                    print(f"Warning: Failed to fetch campaigns for account {account_id}: {e}")
                continue
            
            if not campaign_ids:
                if progress:
                    progress.account_done(account_id, 0)
                continue
            
            # Break into 90-day chunks
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            date_ranges = []
            current = start_dt
            while current < end_dt:
                chunk_end = min(current + timedelta(days=90), end_dt)
                date_ranges.append((current.strftime("%Y-%m-%dT00:00:00Z"), chunk_end.strftime("%Y-%m-%dT00:00:00Z")))
                current = chunk_end
            
            # Create and poll jobs
            job_ids = []
            for start_ts, end_ts in date_ranges:
                try:
                    url = f"{TWITTER_API_BASE}/stats/jobs/accounts/{account_id}"
                    params = {
                        "entity": "CAMPAIGN",
                        "entity_ids": ",".join(campaign_ids[:20]),  # Limit to 20 campaigns
                        "start_time": start_ts,
                        "end_time": end_ts,
                        "granularity": "TOTAL",
                        "placement": "ALL_ON_TWITTER",
                        "metric_groups": "BILLING,ENGAGEMENT",
                    }
                    resp = requests.post(url, auth=auth, params=params, timeout=60)
                    resp.raise_for_status()
                    job_ids.append(resp.json()['data']['id'])
                    time.sleep(1)
                except Exception as e:
                    print(f"Warning: Job creation failed for {start_ts} to {end_ts}: {e}")
            
            # Poll for completion
            job_status = {}
            while job_ids:
                try:
                    url = f"{TWITTER_API_BASE}/stats/jobs/accounts/{account_id}"
                    resp = requests.get(url, auth=auth, timeout=60)
                    resp.raise_for_status()
                    all_jobs = resp.json().get('data', [])
                    pending = 0
                    for job in all_jobs:
                        if job['id'] in job_ids:
                            job_status[job['id']] = job
                            if job['status'] not in ["SUCCESS", "FAILED"]:
                                pending += 1
                    if pending == 0:
                        break
                    time.sleep(10)
                except Exception as e:
                    print(f"Warning: Error polling jobs: {e}")
                    break
            
            acct_rows = 0
            # Download and parse results
            for job_id in job_ids:
                job_data = job_status.get(job_id, {})
                url = job_data.get("url")
                
                if job_data.get("status") != "SUCCESS" or not url:
                    continue
                
                try:
                    import tempfile
                    # Stream download to temp file
                    r = requests.get(url, stream=True, timeout=300)
                    r.raise_for_status()
                    tmp_tw_path = None
                    try:
                        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp_tw:
                            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                                if chunk:
                                    tmp_tw.write(chunk)
                            tmp_tw_path = tmp_tw.name

                        with gzip.open(tmp_tw_path, "rb") as f:
                            result = json.loads(f.read().decode("utf-8"))
                    finally:
                        if tmp_tw_path and os.path.exists(tmp_tw_path):
                            os.unlink(tmp_tw_path)

                    # Aggregate by date (Twitter returns totals, so distribute evenly)
                    for item in result.get("data", []):
                        id_data = item.get("id_data", [])
                        if not id_data:
                            continue
                        for entry in id_data:
                            metrics = entry.get("metrics", {})

                            clicks = sum(int(x or 0) for x in metrics.get("clicks", [])) if isinstance(metrics.get("clicks"), list) else 0
                            impressions = sum(int(x or 0) for x in metrics.get("impressions", [])) if isinstance(metrics.get("impressions"), list) else 0
                            spend_micro = sum(int(x or 0) for x in metrics.get("billed_charge_local_micro", [])) if isinstance(metrics.get("billed_charge_local_micro"), list) else 0
                            spend = spend_micro / 1_000_000

                            days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1
                            daily_clicks = clicks // days if days > 0 else 0
                            daily_impressions = impressions // days if days > 0 else 0
                            daily_spend = spend / days if days > 0 else 0.0

                            current_date = datetime.strptime(start_date, "%Y-%m-%d")
                            while current_date <= datetime.strptime(end_date, "%Y-%m-%d"):
                                date_str = current_date.strftime("%Y-%m-%d")
                                key = (date_str, "twitter", str(account_id))
                                dm = combined.setdefault(key, DailyMetrics())
                                dm.add(daily_impressions, daily_clicks, daily_spend)
                                acct_rows += 1
                                current_date += timedelta(days=1)
                except Exception as e:
                    print(f"Warning: Failed to process Twitter job {job_id}: {e}")

            if progress:
                progress.account_done(account_id, acct_rows)

    if progress:
        progress.done_channel("Twitter/X Ads", len(combined))
    return combined


def fetch_amazon_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Amazon Ads API data and normalize to daily metrics.
    Adapted from amazon_paginated_report.py
    """
    import gzip
    import io
    import time
    from datetime import datetime, timedelta

    print("\n=== Amazon Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_amazon_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_amazon_api_daily, "_progress", None
    )

    if not channel_credentials:
        if progress:
            progress.skip_channel("Amazon Advertising")
        else:
            print("No Amazon advertising credentials found for this workspace; skipping.")
        return {}
    
    client_id = os.getenv("AMAZON_CLIENT_ID") or ""
    client_secret = os.getenv("AMAZON_CLIENT_SECRET") or ""

    # Group by refresh_token because multiple integrations may exist
    grouped_profiles: Dict[str, List[str]] = {}
    for row in channel_credentials:
        rt_raw = row.get("refresh_token") or row.get("token") or ""
        pid_raw = row.get("account_id") or ""

        rt = os.getenv("AMAZON_REFRESH_TOKEN") or str(rt_raw).strip()
        pid = str(pid_raw).strip()

        if rt and pid:
            grouped_profiles.setdefault(rt, []).append(pid)

    if not (client_id and client_secret and grouped_profiles):
        print("Warning: Missing required Amazon Ads configuration (client credentials, refresh tokens, or profiles). Skipping Amazon.")
        return {}

    # Collect profile IDs for BigQuery filter
    if account_ids_collector is not None:
        for pids in grouped_profiles.values():
            for pid in pids:
                cleaned = pid.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    total_accounts = sum(len(pids) for pids in grouped_profiles.values())
    current_acct_idx = 0

    if progress:
        progress.start_channel("Amazon Advertising", total_accounts)

    # Amazon API has 90-day limit per report
    MAX_DAYS = 90
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    for refresh_token, profile_ids in grouped_profiles.items():
        # Get access token for this integration
        try:
            token_url = "https://api.amazon.com/auth/o2/token"
            token_data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            }
            token_resp = requests.post(token_url, data=token_data, timeout=120)
            token_resp.raise_for_status()
            access_token = token_resp.json().get("access_token")
            if not access_token:
                raise RuntimeError("Access token missing in response")
        except Exception as e:
            msg = f"Failed to obtain Amazon access token: {e}"
            for pid in profile_ids:
                current_acct_idx += 1
                if progress:
                    progress.start_account(pid, current_acct_idx, total_accounts)
                    progress.account_error(pid, 0, msg)
                else:
                    print(f"Warning: {msg} for profiles {profile_ids}")
            continue

        for profile_id in profile_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(profile_id, current_acct_idx, total_accounts)
            else:
                print(f"\nFetching Amazon Ads data for profile: {profile_id}")

            acct_rows = 0
            current = start_dt
            while current <= end_dt:
                chunk_end = min(current + timedelta(days=MAX_DAYS - 1), end_dt)
                chunk_start_str = current.strftime("%Y-%m-%d")
                chunk_end_str = chunk_end.strftime("%Y-%m-%d")

                # Create report
                try:
                    report_url = "https://advertising-api.amazon.com/reporting/reports"
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Amazon-Advertising-API-ClientId": client_id,
                        "Amazon-Advertising-API-Scope": profile_id,
                        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
                        "Accept": "application/json",
                    }
                    body = {
                        "name": f"SP Campaign Metrics {chunk_start_str} to {chunk_end_str}",
                        "startDate": chunk_start_str,
                        "endDate": chunk_end_str,
                        "configuration": {
                            "adProduct": "SPONSORED_PRODUCTS",
                            "reportTypeId": "spCampaigns",
                            "groupBy": ["campaign"],
                            "columns": ["campaignId", "campaignName", "date", "impressions", "clicks", "cost"],
                            "timeUnit": "DAILY",
                            "format": "GZIP_JSON"
                        }
                    }

                    create_resp = requests.post(report_url, headers=headers, json=body, timeout=120)
                    create_resp.raise_for_status()
                    report_id = create_resp.json().get("reportId")
                    if not report_id:
                        raise RuntimeError("reportId missing in response")

                    # Poll for completion
                    poll_url = f"https://advertising-api.amazon.com/reporting/reports/{report_id}"
                    poll_headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Amazon-Advertising-API-ClientId": client_id,
                        "Amazon-Advertising-API-Scope": profile_id,
                        "Accept": "application/json",
                    }
                    
                    success = False
                    for poll_attempt in range(40): # ~10 mins max
                        poll_resp = requests.get(poll_url, headers=poll_headers, timeout=120)
                        poll_resp.raise_for_status()
                        status_data = poll_resp.json()
                        status = status_data.get("status")
                        
                        if not progress:
                            print(f"  Poll {poll_attempt+1}/40: {status}")

                        if status == "COMPLETED":
                            location = status_data.get("url") or status_data.get("location")
                            if location:
                                import tempfile
                                download_resp = requests.get(location, stream=True, timeout=300)
                                download_resp.raise_for_status()

                                tmp_gz_path = None
                                try:
                                    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp_gz:
                                        for chunk in download_resp.iter_content(chunk_size=8 * 1024 * 1024):
                                            if chunk:
                                                tmp_gz.write(chunk)
                                        tmp_gz_path = tmp_gz.name

                                    # Decompress and parse
                                    with gzip.open(tmp_gz_path, "rt", encoding="utf-8") as gz:
                                        text = gz.read()
                                    
                                    try:
                                        records = json.loads(text)
                                        if isinstance(records, dict): records = [records]
                                    except:
                                        # Handle JSON Lines or other formats
                                        records = []
                                        for line in text.splitlines():
                                            if line.strip():
                                                try: records.append(json.loads(line))
                                                except: pass

                                    for record in records:
                                        rd = record.get("date")
                                        if not rd: continue
                                        key = (rd, "amazon", str(profile_id))
                                        dm = combined.setdefault(key, DailyMetrics())
                                        dm.add(
                                            int(record.get("impressions", 0) or 0),
                                            int(record.get("clicks", 0) or 0),
                                            float(record.get("cost", 0.0) or 0.0),
                                        )
                                        acct_rows += 1
                                    success = True
                                finally:
                                    if tmp_gz_path and os.path.exists(tmp_gz_path):
                                        os.unlink(tmp_gz_path)
                            break
                        elif status in ("FAILED", "CANCELLED"):
                            print(f"Warning: Amazon report {report_id} {status}")
                            break
                        time.sleep(15)

                except Exception as e:
                    if progress:
                        progress.account_error(profile_id, 0, f"Chunk {chunk_start_str} error: {e}")
                    else:
                        print(f"Warning: Amazon error for {profile_id} ({chunk_start_str}): {e}")
                
                current = chunk_end + timedelta(days=1)

            if progress:
                progress.account_done(profile_id, acct_rows)

    if progress:
        progress.done_channel("Amazon Advertising", len(combined))

    return combined


def fetch_reddit_api_daily(start_date: DateStr, end_date: DateStr, account_ids_collector: Optional[Set[str]] = None) -> DailyMetricsMap:
    """
    Fetch Reddit Ads API data and normalize to daily metrics.

    Multi-account: loops over every credential row in _channel_credentials.
    Each row must have: account_id, refresh_token (or token)

    Basic-auth for the token endpoint is built from:
      REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET env vars (preferred), OR
      REDDIT_BASIC_AUTH env var (pre-built "Basic <base64>" string)
    """
    import base64
    from datetime import datetime, timedelta
    import time

    print("\n=== Reddit Ads configuration ===")

    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_reddit_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_reddit_api_daily, "_progress", None
    )

    if not channel_credentials:
        print("No Reddit Ads credentials found for this workspace; skipping Reddit.")
        return {}

    # Build Basic-auth header for Reddit token endpoint
    basic_auth_env = os.getenv("REDDIT_BASIC_AUTH", "").strip()
    client_id_env = os.getenv("REDDIT_CLIENT_ID", "").strip()
    client_secret_env = os.getenv("REDDIT_CLIENT_SECRET", "").strip()

    def _make_basic_auth(client_id: str, client_secret: str) -> str:
        raw = f"{client_id}:{client_secret}"
        return "Basic " + base64.b64encode(raw.encode()).decode()

    # Cache access tokens per refresh_token to avoid redundant token requests
    _access_token_cache: Dict[str, str] = {}

    def _get_access_token(refresh_token: str) -> Optional[str]:
        if refresh_token in _access_token_cache:
            return _access_token_cache[refresh_token]

        if basic_auth_env:
            basic_auth = basic_auth_env
        elif client_id_env and client_secret_env:
            basic_auth = _make_basic_auth(client_id_env, client_secret_env)
        else:
            print("  ⚠️  Reddit: no Basic-auth credentials found "
                  "(set REDDIT_CLIENT_ID+REDDIT_CLIENT_SECRET or REDDIT_BASIC_AUTH). Skipping account.")
            return None

        try:
            auth_resp = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                headers={
                    "Authorization": basic_auth,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "RedditAdsExtractor/1.0",
                },
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=60,
            )
            auth_resp.raise_for_status()
            token = auth_resp.json().get("access_token")
            if token:
                _access_token_cache[refresh_token] = token
            return token
        except Exception as e:
            print(f"  ⚠️  Reddit: failed to obtain access token: {e}")
            return None

    combined: DailyMetricsMap = {}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - start_dt).days + 1

    # De-duplicate credential rows by account_id (keep first refresh_token seen)
    seen_accounts: Set[str] = set()
    unique_creds: List[Mapping[str, Optional[str]]] = []
    for cred in channel_credentials:
        acc = str(cred.get("account_id") or "").strip()
        if acc and acc not in seen_accounts:
            seen_accounts.add(acc)
            unique_creds.append(cred)

    total_accounts = len(unique_creds)
    if total_accounts == 0:
        print("  ⚠️  Reddit: no account IDs found in credentials. Skipping.")
        return {}

    for acct_idx, cred in enumerate(unique_creds, start=1):
        ad_account_id = str(cred.get("account_id") or "").strip()
        refresh_token = str(cred.get("refresh_token") or cred.get("token") or "").strip()

        if not ad_account_id:
            continue
        if not refresh_token:
            print(f"  ⚠️  Reddit: skipping {ad_account_id} — missing refresh_token.")
            continue

        if progress:
            progress.start_account(ad_account_id, acct_idx, total_accounts)

        # Collect for BigQuery
        if account_ids_collector is not None:
            cleaned = ad_account_id.replace("-", "").strip()
            if cleaned:
                account_ids_collector.add(cleaned)

        access_token = _get_access_token(refresh_token)
        if not access_token:
            if progress:
                progress.account_error(ad_account_id, 401, "No access token")
            continue

        reports_url = f"https://ads-api.reddit.com/api/v3/ad_accounts/{ad_account_id}/reports"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        current = start_dt
        done_days = 0
        acct_rows = 0

        while current <= end_dt:
            day_start = current.strftime("%Y-%m-%dT00:00:00Z")
            day_end = (current + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
            date_str = current.strftime("%Y-%m-%d")
            done_days += 1

            if progress:
                progress.day_progress(ad_account_id, date_str, total_days, done_days)

            payload = {
                "data": {
                    "breakdowns": ["AD_ID", "DATE", "CAMPAIGN_ID"],
                    "fields": ["CLICKS", "IMPRESSIONS", "SPEND"],
                    "filter": (
                        "campaign:effective_status==PENDING_BILLING_INFO,"
                        "campaign:effective_status==ACTIVE,"
                        "campaign:effective_status==PAUSED,"
                        "campaign:effective_status==COMPLETED"
                    ),
                    "starts_at": day_start,
                    "ends_at": day_end,
                    "time_zone_id": "GMT",
                }
            }

            try:
                resp = requests.post(reports_url, headers=headers, json=payload, timeout=120)
                if resp.status_code == 401:
                    _access_token_cache.pop(refresh_token, None)
                    access_token = _get_access_token(refresh_token)
                    if not access_token:
                        break
                    headers["Authorization"] = f"Bearer {access_token}"
                    resp = requests.post(reports_url, headers=headers, json=payload, timeout=120)
                resp.raise_for_status()
                metrics_list = resp.json().get("data", {}).get("metrics", [])

                key = (date_str, "reddit", ad_account_id)
                dm = combined.setdefault(key, DailyMetrics())

                for metric_row in metrics_list:
                    spend_micros = float(metric_row.get("spend", 0) or 0) / 1_000_000
                    dm.add(
                        int(metric_row.get("impressions", 0) or 0),
                        int(metric_row.get("clicks", 0) or 0),
                        spend_micros,
                    )
                    acct_rows += 1

            except Exception as e:
                print(f"  ⚠️  Reddit: failed to fetch data for {ad_account_id} on {date_str}: {e}")

            time.sleep(2)  # polite rate limiting
            current += timedelta(days=1)

        if progress:
            progress.account_done(ad_account_id, acct_rows)

    return combined


# ---------------------------------------------------------------------------
# Vibe API scraper
# ---------------------------------------------------------------------------

def fetch_vibe_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """
    Fetch Vibe API data and normalize to daily metrics.
    """
    channel_credentials: Optional[List[Mapping[str, Optional[str]]]] = getattr(
        fetch_vibe_api_daily, "_channel_credentials", None
    )
    progress: Optional[ProgressReporter] = getattr(
        fetch_vibe_api_daily, "_progress", None
    )

    if not channel_credentials:
        print("No Vibe credentials found for this workspace; skipping Vibe.")
        return {}

    # Group by token
    grouped_accounts: Dict[str, List[str]] = {}
    for row in channel_credentials:
        token_raw = row.get("token") or row.get("refresh_token") or ""
        acc_raw = row.get("account_id") or ""
        
        at = str(token_raw).strip()
        acc = str(acc_raw).strip()

        if at and acc:
            grouped_accounts.setdefault(at, []).append(acc)

    if not grouped_accounts:
        print("Warning: Missing required Vibe configuration (token or account IDs). Skipping Vibe.")
        return {}
        
    if account_ids_collector is not None:
        for accs in grouped_accounts.values():
            for base_id in accs:
                account_ids_collector.add(base_id)

    combined: DailyMetricsMap = {}
    
    total_accounts = sum(len(accs) for accs in grouped_accounts.values())
    current_acct_idx = 0
    if progress:
        progress.start_channel("Vibe", total_accounts)

    from datetime import datetime, timedelta
    import time
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    for access_token, base_ids in grouped_accounts.items():
        headers = {
            "X-API-KEY": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        for adv_id in base_ids:
            current_acct_idx += 1
            if progress:
                progress.start_account(adv_id, current_acct_idx, total_accounts)
                
            acct_rows = 0

            # 1. Create async report
            create_url = "https://clear-platform.vibe.co/rest/reporting/v1/create_async_report"
            payload = {
                "advertiser_id": adv_id,
                "start_date": start_date,  # Vibe accepts YYYY-MM-DD
                "end_date": end_date,
                "metrics": ["impressions", "spend", "clicks"],
                "dimensions": ["date", "campaign_id"],
                "format": "json"
            }

            try:
                # Some API docs use Authorization Bearer while some use X-API-KEY. Trying X-API-KEY first.
                resp = requests.post(create_url, headers=headers, json=payload, timeout=60)
                if resp.status_code == 401 or resp.status_code == 403:
                    # Fallback to Bearer token
                    headers_bearer = headers.copy()
                    del headers_bearer["X-API-KEY"]
                    headers_bearer["Authorization"] = f"Bearer {access_token}"
                    resp = requests.post(create_url, headers=headers_bearer, json=payload, timeout=60)
                    headers = headers_bearer # Keep using working auth
                    
                resp.raise_for_status()
                data = resp.json()
                report_id = data.get("report_id")
                
                if not report_id:
                    print(f"  ⚠️  Vibe: No report_id returned for {adv_id}.")
                    if progress:
                        progress.account_error(adv_id, resp.status_code, "No report_id returned")
                    continue
                    
                # 2. Poll for report status
                status_url = f"https://clear-platform.vibe.co/rest/reporting/v1/get_report_status?report_id={report_id}"
                download_url = None
                
                max_polls = 30
                poll_delay = 5
                
                for attempt in range(max_polls):
                    s_resp = requests.get(status_url, headers=headers, timeout=30)
                    s_resp.raise_for_status()
                    s_data = s_resp.json()
                    status = s_data.get("status")
                    
                    if status == "SUCCESS":
                        download_url = s_data.get("download_url")
                        break
                    elif status in ["FAILED", "ERROR"]:
                        print(f"  ⚠️  Vibe: Report failed for {adv_id}. Status: {status}")
                        break
                        
                    time.sleep(poll_delay)
                
                if not download_url:
                    if progress:
                        progress.account_error(adv_id, 500, "Report did not complete in time or failed")
                    continue
                    
                # 3. Download and parse the JSON report
                dl_resp = requests.get(download_url, headers=headers, timeout=60)
                dl_resp.raise_for_status()
                report_data = dl_resp.json()
                
                # Format depends on API, usually a list of dicts.
                rows = report_data if isinstance(report_data, list) else report_data.get("data", [])
                
                for row in rows:
                    # Parse dates formatted differently if needed
                    raw_date = row.get("date")
                    if not raw_date:
                        continue
                    
                    # Normalize date to YYYY-MM-DD
                    date_val = str(raw_date)[:10]
                    
                    imps = int(row.get("impressions", 0) or 0)
                    clks = int(row.get("clicks", 0) or 0)
                    spend = float(row.get("spend", 0) or 0.0)
                    
                    key = (date_val, "vibe", adv_id)
                    dm = combined.setdefault(key, DailyMetrics())
                    dm.add(imps, clks, spend)
                    acct_rows += 1
                    
                if progress:
                    progress.account_done(adv_id, acct_rows)

            except Exception as e:
                if progress:
                    progress.account_error(adv_id, 500, str(e))
                else:
                    print(f"  ⚠️  Vibe error for account {adv_id}: {e}")

    if progress:
        progress.done_channel("Vibe", len(combined))

    return combined

def _aggregate_metrics(
    daily_metrics: DailyMetricsMap,
    granularity: str,
    start_date: str,
    end_date: str
) -> DailyMetricsMap:
    """
    Summarize DailyMetricsMap into new buckets based on granularity.
    - monthly: Groups by "YYYY-MM"
    - yearly:  Groups by "YYYY"
    - overall: Groups by a range string "YYYY-MM-DD to YYYY-MM-DD"
    """
    summarized: DailyMetricsMap = {}
    
    for (date_str, source, account_id), dm in daily_metrics.items():
        # Normalize date_str to YYYY-MM-DD
        d_clean = str(date_str)[:10].strip()
        
        if granularity == "monthly":
            bucket = d_clean[:7]  # "YYYY-MM"
        elif granularity == "yearly":
            bucket = d_clean[:4]  # "YYYY"
        elif granularity == "overall":
            bucket = f"{start_date} to {end_date}"
        else:
            # Fallback to daily bucket if somehow reached
            bucket = d_clean
            
        key = (bucket, source, account_id)
        target = summarized.setdefault(key, DailyMetrics())
        target.add(dm.impressions, dm.clicks, dm.spend)
        
    return summarized

CHANNEL_API_DISPATCH = {
    "google_youtube": fetch_google_youtube_api_daily,
    "meta": fetch_meta_api_daily,
    "microsoft": fetch_microsoft_api_daily,
    "pinterest": fetch_pinterest_api_daily,
    "snapchat": fetch_snapchat_api_daily,
    "tiktok": fetch_tiktok_api_daily,
    "twitter_x": fetch_twitter_x_api_daily,
    "amazon": fetch_amazon_api_daily,
    "reddit": fetch_reddit_api_daily,
    "vibe": fetch_vibe_api_daily,
}


# ---------------------------------------------------------------------------
# Combined orchestration
# ---------------------------------------------------------------------------

def _parse_date(prompt: str) -> DateStr:
    val = input(prompt).strip()
    try:
        dt.datetime.strptime(val, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"Invalid date (expected YYYY-MM-DD): {val}")
    return val


def _scrape_one_workspace(
    workspace_id: str,
    workspace_name: str,
    start_date: DateStr,
    end_date: DateStr,
    selected_channels: List[str],
    writer: csv._writer,
    progress: ProgressReporter,
    granularity: str = "daily",
) -> int:
    """
    Perform API + BigQuery scraping for a single workspace and write results to a shared CSV writer.
    Returns number of CSV rows written for this workspace.
    """
    # 1) Load all channel-level credentials (tokenactiveaccount)
    workspace_creds = _get_workspace_channel_credentials(workspace_id)
    
    # 2) Optionally add Reddit from common_auth_active_account if reddit is selected
    if "reddit" in selected_channels:
        reddit_creds = _get_reddit_credentials(workspace_id)
        if reddit_creds:
            workspace_creds.extend(reddit_creds)

    creds_by_channel: Dict[str, List[Dict[str, Optional[str]]]] = defaultdict(list)
    for row in workspace_creds:
        connector_raw = (row.get("connector") or "").strip()
        if not connector_raw:
            continue
        connector_key = connector_raw.lower()
        channel_key = CONNECTOR_TO_CHANNEL.get(connector_key)
        if not channel_key:
            continue
        creds_by_channel[channel_key].append(row)

    # 3) Fetch API metrics
    collected_account_ids: Set[str] = set()
    api_metrics: DailyMetricsMap = {}

    for ch in selected_channels:
        label = CHANNEL_LABELS[ch]
        fetcher = CHANNEL_API_DISPATCH[ch]

        channel_creds = creds_by_channel.get(ch, [])
        if not channel_creds:
            # We don't print here anymore, ProgressReporter or fetcher handle it
            continue

        # Pass context via attributes
        setattr(fetcher, "_channel_credentials", channel_creds)
        setattr(fetcher, "_progress", progress)

        per_channel = fetcher(
            start_date,
            end_date,
            account_ids_collector=collected_account_ids,
        )
        for key, dm in per_channel.items():
            bucket = api_metrics.setdefault(key, DailyMetrics())
            bucket.add(dm.impressions, dm.clicks, dm.spend)

    # 4) Fetch BigQuery metrics
    if not collected_account_ids:
        bq_account_ids = []
    else:
        bq_account_ids = list(collected_account_ids)
    
    setattr(fetch_bigquery_metrics, "_progress", progress)
    bq_metrics = fetch_bigquery_metrics(
        workspace_id=workspace_id,
        start_date=start_date,
        end_date=end_date,
        account_ids=bq_account_ids if bq_account_ids else [""],
        selected_channels=selected_channels,
    )

    # 5) Aggregate based on granularity
    if granularity != "daily":
        api_metrics = _aggregate_metrics(api_metrics, granularity, start_date, end_date)
        bq_metrics = _aggregate_metrics(bq_metrics, granularity, start_date, end_date)

    # 6) Combine and write rows
    all_keys: Set[Tuple[DateStr, SourceStr, AccountStr]] = set(api_metrics.keys()) | set(bq_metrics.keys())
    
    def _sort_key(item: Tuple[DateStr, SourceStr, AccountStr]) -> Tuple[dt.date | str, str, str]:
        d, s, a = item
        if granularity == "daily":
            # Normalize date string to first 10 chars (YYYY-MM-DD)
            normalized_d = str(d)[:10].strip()
            return (dt.datetime.strptime(normalized_d, "%Y-%m-%d").date(), s, a)
        return (d, s, a)

    sorted_keys = sorted(all_keys, key=_sort_key)
    ws_rows = 0
    for date_bucket, source, account_id in sorted_keys:
        api_dm = api_metrics.get((date_bucket, source, account_id), DailyMetrics())
        bq_dm = bq_metrics.get((date_bucket, source, account_id), DailyMetrics())
        writer.writerow(
            [
                date_bucket,
                workspace_name,
                source,
                account_id,
                str(api_dm.impressions),
                str(api_dm.clicks),
                f"{api_dm.spend:.2f}",
                str(bq_dm.impressions),
                str(bq_dm.clicks),
                f"{bq_dm.spend:.2f}",
            ]
        )
        ws_rows += 1
    
    progress.csv_rows_written(ws_rows)
    return ws_rows


def run_combined_scraper() -> None:
    print("=== Unified Ads Scraper (API + BigQuery) ===")

    # 1) Workspace selection (supports multi)
    workspaces = _ask_workspaces()
    
    start_date = _parse_date("Start date (YYYY-MM-DD): ")
    end_date = _parse_date("End date   (YYYY-MM-DD): ")

    if start_date > end_date:
        raise SystemExit("start_date must be <= end_date.")

    selected_channels = _ask_channels()

    if questionary is not None:
        granularity = questionary.select(
            "Select granularity:",
            choices=[
                questionary.Choice("Daily", "daily"),
                questionary.Choice("Monthly", "monthly"),
                questionary.Choice("Yearly", "yearly"),
                questionary.Choice("Overall Total", "overall"),
            ]
        ).ask()
    else:
        print("Granularity options: daily, monthly, yearly, overall")
        granularity = input("Enter granularity [daily]: ").strip().lower() or "daily"

    # Determine output path
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if len(workspaces) == 1:
        safe_ws_name = workspaces[0][1].replace(" ", "_")
        filename = f"combined_metrics_{safe_ws_name}_{start_date}_to_{end_date}.csv"
    else:
        filename = f"combined_metrics_MULTI_{start_date}_to_{end_date}.csv"
    out_path = os.path.join(base_dir, filename)

    progress = ProgressReporter(total_workspaces=len(workspaces))

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "date",
                "workspace_name",
                "source",
                "ad_account_id",
                "api_impressions",
                "api_clicks",
                "api_spends",
                "bigquery_impressions",
                "bigquery_clicks",
                "bigquery_spends",
            ]
        )

        for ws_id, ws_name in workspaces:
            progress.start_workspace(ws_id, ws_name)
            ws_rows = _scrape_one_workspace(
                workspace_id=ws_id,
                workspace_name=ws_name,
                start_date=start_date,
                end_date=end_date,
                selected_channels=selected_channels,
                writer=writer,
                progress=progress,
                granularity=granularity
            )
            progress.done_workspace(ws_name, ws_rows)

    print(f"\n✅ Combined CSV written to: {out_path}")


def run_scraper_api(
    workspaces: List[Tuple[str, str]],
    start_date: DateStr,
    end_date: DateStr,
    selected_channels: List[str],
    granularity: str = "daily"
) -> str:
    """
    API entry point for the scraper. Returns CSV content as a string.
    """
    import io
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        "date",
        "workspace_name",
        "source",
        "ad_account_id",
        "api_impressions",
        "api_clicks",
        "api_spends",
        "bigquery_impressions",
        "bigquery_clicks",
        "bigquery_spends",
    ])
    
    progress = ProgressReporter(total_workspaces=len(workspaces))
    
    for ws_id, ws_name in workspaces:
        progress.start_workspace(ws_id, ws_name)
        ws_rows = _scrape_one_workspace(
            workspace_id=ws_id,
            workspace_name=ws_name,
            start_date=start_date,
            end_date=end_date,
            selected_channels=selected_channels,
            writer=writer,
            progress=progress,
            granularity=granularity
        )
        progress.done_workspace(ws_name, ws_rows)
        
    return output.getvalue()


if __name__ == "__main__":
    run_combined_scraper()

