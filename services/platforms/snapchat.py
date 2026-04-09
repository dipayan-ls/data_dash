"""Snapchat Ads API microservice."""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Set

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

CLIENT_ID = os.getenv("SNAPCHAT_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SNAPCHAT_CLIENT_SECRET", "")
TOKEN_URL = "https://accounts.snapchat.com/accounts/oauth2/token"
BASE_URL = "https://adsapi.snapchat.com/v1"


class _SnapAuth:
    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self._lock = threading.Lock()

    def get_access_token(self) -> str:
        with self._lock:
            payload = {
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": self.refresh_token,
            }
            r = requests.post(TOKEN_URL, data=payload, timeout=60)
            r.raise_for_status()
            self.access_token = r.json()["access_token"]
            return self.access_token

    def headers(self) -> dict:
        if not self.access_token:
            self.get_access_token()
        return {"Authorization": f"Bearer {self.access_token}"}


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_snapchat_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Snapchat daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Snapchat Ads")
        return {}

    grouped: Dict[str, List[str]] = {}
    for row in channel_credentials:
        rt = os.getenv("SNAPCHAT_REFRESH_TOKEN") or str(row.get("refresh_token") or row.get("token") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if rt and acc:
            grouped.setdefault(rt, []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Snapchat Ads")
        return {}

    if account_ids_collector is not None:
        for accs in grouped.values():
            for a in accs:
                cleaned = a.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    total = sum(len(v) for v in grouped.values())
    idx = 0

    if progress:
        progress.start_channel("Snapchat Ads", total)

    for refresh_token, ad_account_ids in grouped.items():
        auth = _SnapAuth(refresh_token)

        # Split into 32-day chunks
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        date_ranges = []
        cs = start_dt
        while cs <= end_dt:
            ce = min(cs + timedelta(days=31), end_dt)
            date_ranges.append((cs.strftime("%Y-%m-%d"), ce.strftime("%Y-%m-%d")))
            cs = ce + timedelta(days=1)

        for ad_account_id in ad_account_ids:
            idx += 1
            if progress:
                progress.start_account(ad_account_id, idx, total)

            try:
                url = f"{BASE_URL}/adaccounts/{ad_account_id}/campaigns"
                r = requests.get(url, headers=auth.headers(), timeout=30)
                r.raise_for_status()
                campaigns = r.json().get("campaigns", [])
                campaign_ids = [c["campaign"]["id"] for c in campaigns]
            except Exception as e:
                if progress:
                    progress.account_error(ad_account_id, 0, f"Campaign fetch: {e}")
                continue

            acct_rows = 0
            for cid in campaign_ids:
                for chunk_start, chunk_end in date_ranges:
                    chunk_end_api = (datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    url = f"{BASE_URL}/campaigns/{cid}/stats"
                    params = {
                        "granularity": "DAY",
                        "fields": "impressions,spend,swipes",
                        "start_time": chunk_start,
                        "end_time": chunk_end_api,
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
                                dd = day["start_time"][:10]
                                if chunk_start <= dd <= chunk_end:
                                    key = (dd, "snapchat", str(ad_account_id))
                                    dm = combined.setdefault(key, DailyMetrics())
                                    dm.add(
                                        int(stats.get("impressions", 0) or 0),
                                        int(stats.get("swipes", 0) or 0),
                                        float(stats.get("spend", 0) or 0) / 1_000_000.0,
                                    )
                                    acct_rows += 1
                    except Exception as e:
                        print(f"       Snapchat stats error for campaign {cid}: {e}")
                    time.sleep(0.2)

            if progress:
                progress.account_done(ad_account_id, acct_rows)

    if progress:
        progress.done_channel("Snapchat Ads", len(combined))
    return combined
