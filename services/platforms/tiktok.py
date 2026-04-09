"""TikTok Ads API microservice."""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import time
from typing import Dict, List, Mapping, Optional, Set

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

API_URL = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
MAX_RETRIES = 5


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_tiktok_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch TikTok daily metrics. Fault-tolerant per-account. Requires VPN."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("TikTok Ads")
        return {}

    grouped: Dict[str, List[str]] = {}
    for row in channel_credentials:
        at = os.getenv("TIKTOK_ACCESS_TOKEN") or str(row.get("token") or row.get("refresh_token") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if at and acc:
            grouped.setdefault(at, []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("TikTok Ads")
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
        progress.start_channel("TikTok Ads", total)

    for access_token, advertiser_ids in grouped.items():
        headers = {"Access-Token": access_token}

        for advertiser_id in advertiser_ids:
            idx += 1
            if progress:
                progress.start_account(advertiser_id, idx, total)

            start_dt_obj = dt.datetime.strptime(start_date, "%Y-%m-%d")
            end_dt_obj = dt.datetime.strptime(end_date, "%Y-%m-%d")
            acct_rows = 0
            curr = start_dt_obj

            while curr <= end_dt_obj:
                curr_end = min(curr + dt.timedelta(days=29), end_dt_obj)
                params = {
                    "advertiser_id": advertiser_id,
                    "service_type": "AUCTION",
                    "report_type": "BASIC",
                    "data_level": "AUCTION_CAMPAIGN",
                    "dimensions": json.dumps(["stat_time_day"]),
                    "metrics": json.dumps(["impressions", "clicks", "spend"]),
                    "time_granularity": "DAILY",
                    "start_date": curr.strftime("%Y-%m-%d"),
                    "end_date": curr_end.strftime("%Y-%m-%d"),
                    "page": 1,
                    "page_size": 1000,
                }

                success = False
                for attempt in range(MAX_RETRIES):
                    try:
                        resp = requests.get(
                            API_URL, headers=headers, params=params,
                            timeout=60, proxies={"http": None, "https": None},
                        )
                        resp.raise_for_status()
                        resp_json = resp.json()

                        if resp_json.get("code") != 0:
                            err_msg = resp_json.get("message", "Unknown")
                            if attempt == MAX_RETRIES - 1:
                                if progress:
                                    progress.account_error(advertiser_id, 0, f"API: {err_msg}")
                                break
                            time.sleep(random.uniform(1, 3))
                            continue

                        for rec in resp_json.get("data", {}).get("list", []):
                            dims = rec.get("dimensions", {})
                            mets = rec.get("metrics", {})
                            date = dims.get("stat_time_day")
                            if not date:
                                continue
                            date = str(date)[:10]
                            key = (date, "tiktok", str(advertiser_id))
                            dm = combined.setdefault(key, DailyMetrics())
                            dm.add(
                                int(mets.get("impressions", 0) or 0),
                                int(mets.get("clicks", 0) or 0),
                                float(mets.get("spend", 0.0) or 0.0),
                            )
                            acct_rows += 1
                        success = True
                        break

                    except Exception as e:
                        if attempt == MAX_RETRIES - 1:
                            if progress:
                                progress.account_error(advertiser_id, 0, str(e))
                        time.sleep(random.uniform(1, 3))

                curr = curr_end + dt.timedelta(days=1)

            if acct_rows > 0 and progress:
                progress.account_done(advertiser_id, acct_rows)

    if progress:
        progress.done_channel("TikTok Ads", len(combined))
    return combined
