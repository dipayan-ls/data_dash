"""Vibe Ads API microservice."""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Set

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_vibe_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Vibe daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Vibe")
        return {}

    grouped: Dict[str, List[str]] = {}
    for row in channel_credentials:
        at = str(row.get("token") or row.get("refresh_token") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if at and acc:
            grouped.setdefault(at, []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Vibe")
        return {}

    if account_ids_collector is not None:
        for accs in grouped.values():
            for a in accs:
                account_ids_collector.add(a)

    combined: DailyMetricsMap = {}
    total = sum(len(v) for v in grouped.values())
    idx = 0

    if progress:
        progress.start_channel("Vibe", total)

    for access_token, base_ids in grouped.items():
        headers = {"X-API-KEY": access_token, "Content-Type": "application/json", "Accept": "application/json"}

        for adv_id in base_ids:
            idx += 1
            if progress:
                progress.start_account(adv_id, idx, total)

            acct_rows = 0

            create_url = "https://clear-platform.vibe.co/rest/reporting/v1/create_async_report"
            payload = {
                "advertiser_id": adv_id,
                "start_date": start_date,
                "end_date": end_date,
                "metrics": ["impressions", "spend", "clicks"],
                "dimensions": ["date", "campaign_id"],
                "format": "json",
            }

            try:
                resp = requests.post(create_url, headers=headers, json=payload, timeout=60)
                if resp.status_code in (401, 403):
                    headers_b = {k: v for k, v in headers.items() if k != "X-API-KEY"}
                    headers_b["Authorization"] = f"Bearer {access_token}"
                    resp = requests.post(create_url, headers=headers_b, json=payload, timeout=60)
                    headers = headers_b

                resp.raise_for_status()
                report_id = resp.json().get("report_id")
                if not report_id:
                    if progress:
                        progress.account_error(adv_id, 0, "No report_id")
                    continue

                status_url = f"https://clear-platform.vibe.co/rest/reporting/v1/get_report_status?report_id={report_id}"
                download_url = None

                for _ in range(30):
                    sr = requests.get(status_url, headers=headers, timeout=30)
                    sr.raise_for_status()
                    sd = sr.json()
                    status = sd.get("status")
                    if status == "SUCCESS":
                        download_url = sd.get("download_url")
                        break
                    if status in ("FAILED", "ERROR"):
                        break
                    time.sleep(5)

                if not download_url:
                    if progress:
                        progress.account_error(adv_id, 0, "Report did not complete")
                    continue

                dl = requests.get(download_url, headers=headers, timeout=60)
                dl.raise_for_status()
                report_data = dl.json()
                rows = report_data if isinstance(report_data, list) else report_data.get("data", [])

                for row in rows:
                    raw_date = row.get("date")
                    if not raw_date:
                        continue
                    date_val = str(raw_date)[:10]
                    key = (date_val, "vibe", adv_id)
                    dm = combined.setdefault(key, DailyMetrics())
                    dm.add(int(row.get("impressions", 0) or 0), int(row.get("clicks", 0) or 0), float(row.get("spend", 0) or 0.0))
                    acct_rows += 1

                if progress:
                    progress.account_done(adv_id, acct_rows)

            except Exception as e:
                if progress:
                    progress.account_error(adv_id, 0, str(e))

    if progress:
        progress.done_channel("Vibe", len(combined))
    return combined
