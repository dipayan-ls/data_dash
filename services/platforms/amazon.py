"""Amazon Ads API microservice."""

from __future__ import annotations

import gzip
import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Set

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_amazon_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Amazon Ads daily metrics. Fault-tolerant per-profile."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Amazon Advertising")
        return {}

    client_id = os.getenv("AMAZON_CLIENT_ID") or ""
    client_secret = os.getenv("AMAZON_CLIENT_SECRET") or ""

    grouped: Dict[str, List[str]] = {}
    for row in channel_credentials:
        rt = os.getenv("AMAZON_REFRESH_TOKEN") or str(row.get("refresh_token") or row.get("token") or "").strip()
        pid = str(row.get("account_id") or "").strip()
        if rt and pid:
            grouped.setdefault(rt, []).append(pid)

    if not (client_id and client_secret and grouped):
        if progress:
            progress.skip_channel("Amazon Advertising")
        return {}

    if account_ids_collector is not None:
        for pids in grouped.values():
            for p in pids:
                cleaned = p.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    combined: DailyMetricsMap = {}
    total = sum(len(v) for v in grouped.values())
    idx = 0
    MAX_DAYS = 90

    if progress:
        progress.start_channel("Amazon Advertising", total)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    for refresh_token, profile_ids in grouped.items():
        try:
            token_resp = requests.post(
                "https://api.amazon.com/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=120,
            )
            token_resp.raise_for_status()
            access_token = token_resp.json().get("access_token")
            if not access_token:
                raise RuntimeError("No access_token in response")
        except Exception as e:
            for pid in profile_ids:
                idx += 1
                if progress:
                    progress.start_account(pid, idx, total)
                    progress.account_error(pid, 0, f"Token: {e}")
            continue

        for profile_id in profile_ids:
            idx += 1
            if progress:
                progress.start_account(profile_id, idx, total)

            acct_rows = 0
            current = start_dt
            while current <= end_dt:
                chunk_end = min(current + timedelta(days=MAX_DAYS - 1), end_dt)
                cs = current.strftime("%Y-%m-%d")
                ce = chunk_end.strftime("%Y-%m-%d")

                try:
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Amazon-Advertising-API-ClientId": client_id,
                        "Amazon-Advertising-API-Scope": profile_id,
                        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
                        "Accept": "application/json",
                    }
                    body = {
                        "name": f"SP Campaign Metrics {cs} to {ce}",
                        "startDate": cs,
                        "endDate": ce,
                        "configuration": {
                            "adProduct": "SPONSORED_PRODUCTS",
                            "reportTypeId": "spCampaigns",
                            "groupBy": ["campaign"],
                            "columns": ["campaignId", "campaignName", "date", "impressions", "clicks", "cost"],
                            "timeUnit": "DAILY",
                            "format": "GZIP_JSON",
                        },
                    }

                    create_resp = requests.post("https://advertising-api.amazon.com/reporting/reports", headers=headers, json=body, timeout=120)
                    create_resp.raise_for_status()
                    report_id = create_resp.json().get("reportId")
                    if not report_id:
                        raise RuntimeError("No reportId")

                    poll_url = f"https://advertising-api.amazon.com/reporting/reports/{report_id}"
                    poll_headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Amazon-Advertising-API-ClientId": client_id,
                        "Amazon-Advertising-API-Scope": profile_id,
                        "Accept": "application/json",
                    }

                    for _ in range(40):
                        pr = requests.get(poll_url, headers=poll_headers, timeout=120)
                        pr.raise_for_status()
                        sd = pr.json()
                        status = sd.get("status")

                        if status == "COMPLETED":
                            loc = sd.get("url") or sd.get("location")
                            if loc:
                                dl = requests.get(loc, stream=True, timeout=300)
                                dl.raise_for_status()
                                tmp_path = None
                                try:
                                    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
                                        for chunk in dl.iter_content(chunk_size=8 * 1024 * 1024):
                                            if chunk:
                                                tmp.write(chunk)
                                        tmp_path = tmp.name
                                    with gzip.open(tmp_path, "rt", encoding="utf-8") as gz:
                                        text = gz.read()
                                    try:
                                        records = json.loads(text)
                                        if isinstance(records, dict):
                                            records = [records]
                                    except Exception:
                                        records = []
                                        for line in text.splitlines():
                                            if line.strip():
                                                try:
                                                    records.append(json.loads(line))
                                                except Exception:
                                                    pass
                                    for rec in records:
                                        rd = rec.get("date")
                                        if not rd:
                                            continue
                                        key = (rd, "amazon", str(profile_id))
                                        dm = combined.setdefault(key, DailyMetrics())
                                        dm.add(
                                            int(rec.get("impressions", 0) or 0),
                                            int(rec.get("clicks", 0) or 0),
                                            float(rec.get("cost", 0.0) or 0.0),
                                        )
                                        acct_rows += 1
                                finally:
                                    if tmp_path and os.path.exists(tmp_path):
                                        os.unlink(tmp_path)
                            break
                        elif status in ("FAILED", "CANCELLED"):
                            break
                        time.sleep(15)

                except Exception as e:
                    if progress:
                        progress.account_error(profile_id, 0, f"Chunk {cs}: {e}")

                current = chunk_end + timedelta(days=1)

            if progress:
                progress.account_done(profile_id, acct_rows)

    if progress:
        progress.done_channel("Amazon Advertising", len(combined))
    return combined
