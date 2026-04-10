"""Pinterest Ads API microservice."""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Set, Tuple

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

API_BASE_URL = "https://api.pinterest.com/v5"
MAX_POLLS = 30
POLL_INTERVAL = 5


# ── Public entry point ───────────────────────────────────────────────────────

def _split_date_range(start_date: str, end_date: str, max_days: int = 186) -> List[Tuple[str, str]]:
    """Split a date range into chunks of max_days or fewer."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    chunks = []
    current_start = start
    while current_start <= end:
        current_end = min(current_start + timedelta(days=max_days - 1), end)
        chunks.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        current_start = current_end + timedelta(days=1)
    return chunks


def fetch_pinterest_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Pinterest daily metrics. Fault-tolerant per-account. Handles 186-day limit."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Pinterest Ads")
        return {}

    # 1. Chunking
    chunks = _split_date_range(start_date, end_date)
    
    grouped: Dict[str, List[str]] = {}
    for row in channel_credentials:
        bt = os.getenv("PINTEREST_BEARER_TOKEN") or str(row.get("token") or row.get("refresh_token") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if bt and acc:
            grouped.setdefault(bt, []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Pinterest Ads")
        return {}

    if account_ids_collector is not None:
        for accs in grouped.values():
            for a in accs:
                cleaned = a.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    total_accounts = sum(len(v) for v in grouped.values())
    total_work = total_accounts * len(chunks)
    idx = 0

    if progress:
        progress.start_channel("Pinterest Ads", total_work)

    for bearer_token, account_ids in grouped.items():
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}

        for account_id in account_ids:
            acct_rows = 0
            
            for chunk_start, chunk_end in chunks:
                idx += 1
                if progress:
                    progress.start_account(f"{account_id} ({chunk_start})", idx, total_work)

                report_url = f"{API_BASE_URL}/ad_accounts/{account_id}/reports"
                payload = {
                    "start_date": chunk_start,
                    "end_date": chunk_end,
                    "granularity": "DAY",
                    "level": "CAMPAIGN",
                    "columns": ["SPEND_IN_DOLLAR", "TOTAL_IMPRESSION", "TOTAL_CLICKTHROUGH"],
                    "click_window_days": 30,
                    "engagement_window_days": 30,
                    "view_window_days": 1,
                    "conversion_report_time": "TIME_OF_AD_ACTION",
                    "report_format": "JSON",
                }

                try:
                    resp = requests.post(report_url, headers=headers, json=payload, timeout=120)
                    resp.raise_for_status()
                    report_response = resp.json()

                    if "token" not in report_response:
                        if progress:
                            progress.account_error(account_id, 0, f"No token for chunk {chunk_start}")
                        continue

                    token = report_response["token"]
                    poll_url = f"{API_BASE_URL}/ad_accounts/{account_id}/reports?token={token}"
                    poll_headers = {"Authorization": f"Bearer {bearer_token}"}

                    chunk_success = False
                    for _ in range(MAX_POLLS):
                        poll_resp = requests.get(poll_url, headers=poll_headers, timeout=120)
                        poll_resp.raise_for_status()
                        data = poll_resp.json()
                        status = data.get("report_status", "UNKNOWN")

                        if status == "FINISHED":
                            download_url = data.get("url")
                            if download_url:
                                dl = requests.get(download_url, timeout=120)
                                dl.raise_for_status()
                                try:
                                    report_data = dl.json()
                                except Exception:
                                    report_data = []

                                records = []
                                if isinstance(report_data, list):
                                    records = report_data
                                elif isinstance(report_data, dict):
                                    for k in ("data", "rows", "results", "items"):
                                        if k in report_data and isinstance(report_data[k], list):
                                            records = report_data[k]
                                            break
                                    if not records:
                                        # Try scanning for any list
                                        for val in report_data.values():
                                            if isinstance(val, list):
                                                records.extend(val)
                                            elif isinstance(val, dict):
                                                records.append(val)

                                for rec in records:
                                    ds = rec.get("DATE")
                                    if not ds:
                                        continue
                                    try:
                                        clean_date = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%m-%d")
                                    except Exception:
                                        continue
                                    key = (clean_date, "pinterest", str(account_id))
                                    dm = combined.setdefault(key, DailyMetrics())
                                    dm.add(
                                        int(rec.get("TOTAL_IMPRESSION", 0) or 0),
                                        int(rec.get("TOTAL_CLICKTHROUGH", 0) or 0),
                                        float(rec.get("SPEND_IN_DOLLAR", 0.0) or 0.0),
                                    )
                                    acct_rows += 1
                            chunk_success = True
                            break

                        if status in ("FAILED", "CANCELLED"):
                            if progress:
                                progress.account_error(account_id, 0, f"Chunk {chunk_start} {status}")
                            break

                        time.sleep(POLL_INTERVAL)

                    if not chunk_success and progress:
                        progress.account_error(account_id, 0, f"Chunk {chunk_start} timeout")

                except Exception as e:
                    if progress:
                        progress.account_error(account_id, 0, f"Chunk {chunk_start}: {e}")

            if progress:
                progress.account_done(account_id, acct_rows)

    if progress:
        progress.done_channel("Pinterest Ads", len(combined))
    return combined

    if progress:
        progress.done_channel("Pinterest Ads", len(combined))
    return combined
