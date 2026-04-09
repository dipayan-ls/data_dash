"""Reddit Ads API microservice."""

from __future__ import annotations

import base64
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Set

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr


def _make_basic_auth(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}"
    return "Basic " + base64.b64encode(raw.encode()).decode()


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_reddit_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Reddit Ads daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Reddit Ads")
        return {}

    basic_auth_env = os.getenv("REDDIT_BASIC_AUTH", "").strip()
    client_id_env = os.getenv("REDDIT_CLIENT_ID", "").strip()
    client_secret_env = os.getenv("REDDIT_CLIENT_SECRET", "").strip()

    _token_cache: Dict[str, str] = {}

    def _get_access_token(refresh_token: str) -> Optional[str]:
        if refresh_token in _token_cache:
            return _token_cache[refresh_token]
        if basic_auth_env:
            ba = basic_auth_env
        elif client_id_env and client_secret_env:
            ba = _make_basic_auth(client_id_env, client_secret_env)
        else:
            print("       Reddit: no client credentials. Skipping.")
            return None
        try:
            resp = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                headers={"Authorization": ba, "Content-Type": "application/x-www-form-urlencoded", "User-Agent": "RedditAdsExtractor/1.0"},
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=60,
            )
            resp.raise_for_status()
            token = resp.json().get("access_token")
            if token:
                _token_cache[refresh_token] = token
            return token
        except Exception as e:
            print(f"       Reddit token error: {e}")
            return None

    # Deduplicate by account_id
    seen: Set[str] = set()
    unique_creds: List[Mapping[str, Optional[str]]] = []
    for cred in channel_credentials:
        acc = str(cred.get("account_id") or "").strip()
        if acc and acc not in seen:
            seen.add(acc)
            unique_creds.append(cred)

    if not unique_creds:
        if progress:
            progress.skip_channel("Reddit Ads")
        return {}

    combined: DailyMetricsMap = {}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - start_dt).days + 1

    for acct_idx, cred in enumerate(unique_creds, start=1):
        ad_account_id = str(cred.get("account_id") or "").strip()
        refresh_token = str(cred.get("refresh_token") or cred.get("token") or "").strip()

        if not ad_account_id or not refresh_token:
            continue

        if progress:
            progress.start_account(ad_account_id, acct_idx, len(unique_creds))

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
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json", "Accept": "application/json"}

        current = start_dt
        done_days = 0
        acct_rows = 0

        while current <= end_dt:
            day_start = current.strftime("%Y-%m-%dT00:00:00Z")
            day_end = (current + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
            ds = current.strftime("%Y-%m-%d")
            done_days += 1

            if progress:
                progress.day_progress(ad_account_id, ds, total_days, done_days)

            payload = {
                "data": {
                    "breakdowns": ["AD_ID", "DATE", "CAMPAIGN_ID"],
                    "fields": ["CLICKS", "IMPRESSIONS", "SPEND"],
                    "filter": "campaign:effective_status==PENDING_BILLING_INFO,campaign:effective_status==ACTIVE,campaign:effective_status==PAUSED,campaign:effective_status==COMPLETED",
                    "starts_at": day_start,
                    "ends_at": day_end,
                    "time_zone_id": "GMT",
                }
            }

            try:
                resp = requests.post(reports_url, headers=headers, json=payload, timeout=120)
                if resp.status_code == 401:
                    _token_cache.pop(refresh_token, None)
                    access_token = _get_access_token(refresh_token)
                    if not access_token:
                        break
                    headers["Authorization"] = f"Bearer {access_token}"
                    resp = requests.post(reports_url, headers=headers, json=payload, timeout=120)
                resp.raise_for_status()
                metrics_list = resp.json().get("data", {}).get("metrics", [])

                key = (ds, "reddit", ad_account_id)
                dm = combined.setdefault(key, DailyMetrics())
                for mr in metrics_list:
                    spend_micros = float(mr.get("spend", 0) or 0) / 1_000_000
                    dm.add(int(mr.get("impressions", 0) or 0), int(mr.get("clicks", 0) or 0), spend_micros)
                    acct_rows += 1

            except Exception as e:
                print(f"       Reddit error for {ad_account_id} on {ds}: {e}")

            time.sleep(2)
            current += timedelta(days=1)

        if progress:
            progress.account_done(ad_account_id, acct_rows)

    if progress:
        progress.done_channel("Reddit Ads", len(combined))
    return combined
