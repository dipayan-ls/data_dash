"""Google Ads (Google + YouTube) API microservice."""

from __future__ import annotations

import decimal
import os
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

DEVELOPER_TOKEN = "yM_9tlY1lgbgVTdIDbtcpQ"
API_URL_TEMPLATE = "https://googleads.googleapis.com/v20/customers/{customer_id}/googleAds:searchStream"
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
TOKEN_URL = "https://oauth2.googleapis.com/token"


def _get_access_token(refresh_token: str) -> str:
    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
    }
    resp = requests.post(TOKEN_URL, data=payload, timeout=60)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to obtain Google Ads access token.")
    return token


def _fetch_campaign_data(
    login_customer_id: str,
    access_token: str,
    customer_id: str,
    start_date: DateStr,
    end_date: DateStr,
) -> List[Mapping]:
    clean_login = login_customer_id.replace("-", "")
    clean_customer = customer_id.replace("-", "")
    url = API_URL_TEMPLATE.format(customer_id=clean_customer)

    headers = {
        "developer-token": DEVELOPER_TOKEN,
        "login-customer-id": clean_login,
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    query = f"""
        SELECT
            campaign.id, campaign.name, campaign.status,
            campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
            segments.ad_network_type, segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY segments.date ASC
    """

    resp = requests.post(url, headers=headers, json={"query": query}, timeout=120)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        status = resp.status_code
        if status in (401, 403, 429):
            print(f"       HTTP {status} for {customer_id} — skipping.")
        else:
            print(f"       HTTP {status} for {customer_id} — skipping.")
        return []

    data = resp.json()
    return data if isinstance(data, list) else []


def _aggregate_by_network(raw_chunks: Iterable[Mapping]) -> Dict[str, Dict[str, DailyMetrics]]:
    """Split into 'google' vs 'youtube' by ad_network_type."""
    yt_daily: Dict[str, DailyMetrics] = {}
    gg_daily: Dict[str, DailyMetrics] = {}

    for chunk in raw_chunks:
        for res in chunk.get("results", []):
            metrics = res.get("metrics", {}) or {}
            segments = res.get("segments", {}) or {}
            date = segments.get("date")
            if not date:
                continue

            ad_network = (segments.get("adNetworkType") or "").upper()
            impressions = int(metrics.get("impressions", 0) or 0)
            clicks = int(metrics.get("clicks", 0) or 0)
            cost_micros = decimal.Decimal(str(metrics.get("costMicros", 0) or 0))
            spend = float(cost_micros / decimal.Decimal("1000000"))

            bucket = yt_daily.setdefault(date, DailyMetrics()) if "YOUTUBE" in ad_network else gg_daily.setdefault(date, DailyMetrics())
            bucket.add(impressions, clicks, spend)

    return {"google": gg_daily, "youtube": yt_daily}


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_google_youtube_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Google + YouTube daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Google Ads")
        return {}

    # Group by (refresh_token, login_customer_id)
    grouped: Dict[Tuple[str, str], List[str]] = {}
    for row in channel_credentials:
        rt = str(row.get("refresh_token") or row.get("token") or "").strip()
        lc = str(row.get("login_customer_id") or row.get("customer_id") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if rt and lc and acc:
            grouped.setdefault((rt, lc), []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Google Ads")
        return {}

    combined: DailyMetricsMap = {}
    total = sum(len(v) for v in grouped.values())
    if progress:
        progress.start_channel("Google Ads (Google + YouTube)", total)

    idx = 0
    for (refresh_token, login_id), accounts in grouped.items():
        try:
            access_token = _get_access_token(refresh_token)
        except Exception as e:
            print(f"       Failed to get Google access token: {e}")
            idx += len(accounts)
            continue

        for acc in accounts:
            idx += 1
            if progress:
                progress.start_account(acc, idx, total)

            clean_acc = acc.replace("-", "").replace("act_", "").strip()
            if account_ids_collector is not None and clean_acc:
                account_ids_collector.add(clean_acc)

            try:
                raw = _fetch_campaign_data(login_id, access_token, acc, start_date, end_date)
                per_source = _aggregate_by_network(raw)
                acct_rows = 0
                for source, daily in per_source.items():
                    for date, dm in daily.items():
                        key = (date, source, clean_acc)
                        bucket = combined.setdefault(key, DailyMetrics())
                        bucket.add(dm.impressions, dm.clicks, dm.spend)
                        acct_rows += 1
                if progress:
                    progress.account_done(acc, acct_rows)
            except Exception as e:
                if progress:
                    progress.account_error(acc, 0, str(e))

    if progress:
        progress.done_channel("Google Ads (Google + YouTube)", len(combined))
    return combined
