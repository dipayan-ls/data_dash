"""Meta (Facebook + Instagram) API microservice."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Set

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError, ReadTimeout
from urllib3.util.retry import Retry

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

CLIENT_ID = os.getenv("META_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("META_CLIENT_SECRET", "")
API_VERSION = "v22.0"
TIMEOUT = 90
MAX_RETRIES = 3
RETRY_BACKOFF = 10
RATE_LIMIT_SLEEP = 60
CHUNK_DAYS = 6  # 7-day windows


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_meta_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Meta (Facebook + Instagram) daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Meta")
        return {}

    # Group by access_token
    grouped: Dict[str, List[str]] = {}
    for row in channel_credentials:
        token = os.getenv("META_ACCESS_TOKEN") or str(row.get("token") or row.get("refresh_token") or "").strip()
        acc = str(row.get("account_id") or "").strip().replace("act_", "")
        if token and acc:
            grouped.setdefault(token, []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Meta")
        return {}

    if account_ids_collector is not None:
        for accs in grouped.values():
            for a in accs:
                account_ids_collector.add(a)

    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=10, pool_maxsize=10,
        max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504]),
    )
    session.mount("https://", adapter)

    combined: DailyMetricsMap = {}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    total = sum(len(v) for v in grouped.values())
    idx = 0
    if progress:
        progress.start_channel("Meta (Facebook + Instagram)", total)

    for access_token, base_ids in grouped.items():
        for base_id in base_ids:
            idx += 1
            if progress:
                progress.start_account(base_id, idx, total)

            api_id = f"act_{base_id}"
            current = start_dt
            acct_rows = 0

            while current <= end_dt:
                chunk_end = min(current + timedelta(days=CHUNK_DAYS), end_dt)
                cs, ce = current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")

                url = f"https://graph.facebook.com/{API_VERSION}/{api_id}/insights"
                params = {
                    "access_token": access_token,
                    "time_range": json.dumps({"since": cs, "until": ce}),
                    "fields": "campaign_name,campaign_id,adset_name,ad_name,ad_id,date_start,date_stop,impressions,spend,inline_link_clicks",
                    "breakdowns": "publisher_platform",
                    "level": "ad",
                    "limit": 500,
                    "time_increment": "1",
                }

                next_url: Optional[str] = url
                current_params = params.copy()

                while next_url:
                    attempt = 0
                    success = False
                    while attempt <= MAX_RETRIES:
                        try:
                            r = session.get(next_url, params=current_params, timeout=TIMEOUT)

                            if r.status_code == 429:
                                wait = RATE_LIMIT_SLEEP * (attempt + 1)
                                if progress:
                                    progress.account_retry(base_id, attempt + 1, MAX_RETRIES, wait, "Rate Limit (429)")
                                time.sleep(wait)
                                attempt += 1
                                continue

                            if r.status_code in (401, 403):
                                if progress:
                                    progress.account_error(base_id, r.status_code, "token expired or inaccessible")
                                next_url = None
                                success = True
                                break

                            r.raise_for_status()
                            data = r.json()

                            for row in data.get("data", []):
                                platform = str(row.get("publisher_platform", "")).lower().strip()
                                ds = row.get("date_start")
                                if not ds:
                                    continue
                                source = "instagram" if platform == "instagram" else "facebook"
                                key = (ds, source, base_id)
                                dm = combined.setdefault(key, DailyMetrics())
                                dm.add(
                                    int(row.get("impressions", 0) or 0),
                                    int(row.get("inline_link_clicks", 0) or 0),
                                    float(row.get("spend", 0) or 0),
                                )
                                acct_rows += 1

                            next_url = data.get("paging", {}).get("next")
                            current_params = {}
                            success = True
                            break

                        except (ReadTimeout, RequestsConnectionError) as e:
                            wait = RETRY_BACKOFF * (2 ** attempt)
                            if progress:
                                progress.account_retry(base_id, attempt + 1, MAX_RETRIES, wait, str(e))
                            time.sleep(wait)
                            attempt += 1

                        except Exception as e:
                            if progress:
                                progress.account_error(base_id, 0, str(e))
                            next_url = None
                            success = True
                            break

                    if not success:
                        if progress:
                            progress.account_error(base_id, 408, f"Max retries ({MAX_RETRIES})")
                        next_url = None

                current = chunk_end + timedelta(days=1)
                time.sleep(0.5)

            if progress:
                progress.account_done(base_id, acct_rows)

    if progress:
        progress.done_channel("Meta (Facebook + Instagram)", len(combined))
    return combined
