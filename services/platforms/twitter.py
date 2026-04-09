"""Twitter / X Ads API microservice."""

from __future__ import annotations

import gzip
import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, List, Mapping, Optional, Set, Tuple

import requests
from requests_oauthlib import OAuth1

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

CONSUMER_KEY = "OKjnm0fAt6tYgM1DySQe2qEpT"
CONSUMER_SECRET = "F4O5DyUXMwjP68kBF1Vpe5ybsiycCRZCztz1vpnZVYq5ca9B2W"
API_BASE = "https://ads-api.twitter.com/12"


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_twitter_x_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Twitter/X daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Twitter/X Ads")
        return {}

    grouped: Dict[Tuple[str, str], List[str]] = {}
    for row in channel_credentials:
        at = str(row.get("token") or "").strip()
        ats = str(row.get("refresh_token") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if at and ats and acc:
            grouped.setdefault((at, ats), []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Twitter/X Ads")
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
        progress.start_channel("Twitter/X Ads", total)

    for (access_token, access_token_secret), account_ids in grouped.items():
        auth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET, access_token, access_token_secret)

        for account_id in account_ids:
            idx += 1
            if progress:
                progress.start_account(account_id, idx, total)

            # Get campaigns
            try:
                url = f"{API_BASE}/accounts/{account_id}/campaigns"
                resp = requests.get(url, auth=auth, params={"with_deleted": False}, timeout=60)
                resp.raise_for_status()
                campaigns = resp.json().get("data", [])
                campaign_ids = [c["id"] for c in campaigns if c.get("entity_status") == "ACTIVE"]
            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, f"Campaign fetch: {e}")
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

            # Create jobs
            job_ids = []
            for start_ts, end_ts in date_ranges:
                try:
                    url = f"{API_BASE}/stats/jobs/accounts/{account_id}"
                    params = {
                        "entity": "CAMPAIGN",
                        "entity_ids": ",".join(campaign_ids[:20]),
                        "start_time": start_ts,
                        "end_time": end_ts,
                        "granularity": "TOTAL",
                        "placement": "ALL_ON_TWITTER",
                        "metric_groups": "BILLING,ENGAGEMENT",
                    }
                    resp = requests.post(url, auth=auth, params=params, timeout=60)
                    resp.raise_for_status()
                    job_ids.append(resp.json()["data"]["id"])
                    time.sleep(1)
                except Exception as e:
                    print(f"       Twitter job creation failed: {e}")

            # Poll
            job_status = {}
            for _ in range(30):
                try:
                    url = f"{API_BASE}/stats/jobs/accounts/{account_id}"
                    resp = requests.get(url, auth=auth, timeout=60)
                    resp.raise_for_status()
                    all_jobs = resp.json().get("data", [])
                    pending = 0
                    for job in all_jobs:
                        if job["id"] in job_ids:
                            job_status[job["id"]] = job
                            if job["status"] not in ("SUCCESS", "FAILED"):
                                pending += 1
                    if pending == 0:
                        break
                    time.sleep(10)
                except Exception:
                    break

            acct_rows = 0
            for job_id in job_ids:
                jd = job_status.get(job_id, {})
                url = jd.get("url")
                if jd.get("status") != "SUCCESS" or not url:
                    continue

                try:
                    r = requests.get(url, stream=True, timeout=300)
                    r.raise_for_status()
                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
                            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                                if chunk:
                                    tmp.write(chunk)
                            tmp_path = tmp.name
                        with gzip.open(tmp_path, "rb") as f:
                            result = json.loads(f.read().decode("utf-8"))
                    finally:
                        if tmp_path and os.path.exists(tmp_path):
                            os.unlink(tmp_path)

                    for item in result.get("data", []):
                        for entry in item.get("id_data", []):
                            metrics = entry.get("metrics", {})
                            clicks = sum(int(x or 0) for x in metrics.get("clicks", [])) if isinstance(metrics.get("clicks"), list) else 0
                            impressions = sum(int(x or 0) for x in metrics.get("impressions", [])) if isinstance(metrics.get("impressions"), list) else 0
                            spend_micro = sum(int(x or 0) for x in metrics.get("billed_charge_local_micro", [])) if isinstance(metrics.get("billed_charge_local_micro"), list) else 0
                            spend = spend_micro / 1_000_000

                            days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1
                            d_clicks = clicks // days if days > 0 else 0
                            d_imp = impressions // days if days > 0 else 0
                            d_spend = spend / days if days > 0 else 0.0

                            cd = datetime.strptime(start_date, "%Y-%m-%d")
                            while cd <= datetime.strptime(end_date, "%Y-%m-%d"):
                                key = (cd.strftime("%Y-%m-%d"), "twitter", str(account_id))
                                dm = combined.setdefault(key, DailyMetrics())
                                dm.add(d_imp, d_clicks, d_spend)
                                acct_rows += 1
                                cd += timedelta(days=1)
                except Exception as e:
                    print(f"       Twitter job {job_id} error: {e}")

            if progress:
                progress.account_done(account_id, acct_rows)

    if progress:
        progress.done_channel("Twitter/X Ads", len(combined))
    return combined
