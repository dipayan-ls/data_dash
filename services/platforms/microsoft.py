"""Microsoft / Bing Ads API microservice."""

from __future__ import annotations

import csv as csv_module
import html
import io
import os
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Set, Tuple

import requests

from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# ── Constants ────────────────────────────────────────────────────────────────

CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET", "")
DEVELOPER_TOKEN = os.getenv("MICROSOFT_DEVELOPER_TOKEN", "")
SCOPE = "https://ads.microsoft.com/msads.manage"
TOKEN_ENDPOINT = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
REPORT_SUBMIT_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit"
REPORT_POLL_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll"


class _TokenManager:
    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.token_expiry = 0.0
        self.lock = threading.Lock()

    def get_token(self) -> str:
        with self.lock:
            if time.time() >= self.token_expiry - 300:
                self._refresh()
            return self.access_token  # type: ignore

    def _refresh(self):
        if not CLIENT_ID or not CLIENT_SECRET:
            raise RuntimeError("MICROSOFT_CLIENT_ID and MICROSOFT_CLIENT_SECRET are missing from the environment. Please configure them in Render.")

        payload = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
            "scope": SCOPE,
        }
        resp = requests.post(TOKEN_ENDPOINT, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        self.token_expiry = time.time() + data.get("expires_in", 3600)


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_microsoft_api_daily(
    start_date: DateStr,
    end_date: DateStr,
    channel_credentials: List[Mapping[str, Optional[str]]],
    progress: Optional[ProgressReporter] = None,
    account_ids_collector: Optional[Set[str]] = None,
) -> DailyMetricsMap:
    """Fetch Microsoft Ads daily metrics. Fault-tolerant per-account."""
    if not channel_credentials:
        if progress:
            progress.skip_channel("Microsoft Ads")
        return {}

    grouped: Dict[Tuple[str, str], List[str]] = {}
    for row in channel_credentials:
        rt = os.getenv("MICROSOFT_REFRESH_TOKEN") or str(row.get("refresh_token") or row.get("token") or "").strip()
        lc = str(row.get("login_customer_id") or row.get("customer_id") or "").strip()
        acc = str(row.get("account_id") or "").strip()
        if rt and lc and acc:
            grouped.setdefault((rt, lc), []).append(acc)

    if not grouped:
        if progress:
            progress.skip_channel("Microsoft Ads")
        return {}

    if account_ids_collector is not None:
        for accs in grouped.values():
            for a in accs:
                cleaned = a.replace("-", "").strip()
                if cleaned:
                    account_ids_collector.add(cleaned)

    token_managers: Dict[str, _TokenManager] = {}
    combined: DailyMetricsMap = {}
    total = sum(len(v) for v in grouped.values())
    idx = 0

    if progress:
        progress.start_channel("Microsoft Ads", total)

    y1, m1, d1 = map(int, start_date.split("-"))
    y2, m2, d2 = map(int, end_date.split("-"))

    for (refresh_token, login_customer_id), account_ids in grouped.items():
        if refresh_token not in token_managers:
            token_managers[refresh_token] = _TokenManager(refresh_token)
        tm = token_managers[refresh_token]

        for account_id in account_ids:
            idx += 1
            if progress:
                progress.start_account(account_id, idx, total)

            try:
                access_token = tm.get_token()
            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, f"Token: {e}")
                continue

            headers = {
                "Authorization": f"Bearer {access_token}",
                "DeveloperToken": DEVELOPER_TOKEN,
                "CustomerId": login_customer_id,
                "CustomerAccountId": account_id,
                "Content-Type": "application/json",
            }

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
                        "CustomDateRangeEnd": {"Year": y2, "Month": m2, "Day": d2},
                    },
                }
            }

            try:
                resp = requests.post(REPORT_SUBMIT_URL, headers=headers, json=payload, timeout=30)
                resp.raise_for_status()
                request_id = resp.json().get("ReportRequestId")

                # Poll
                start_time = time.time()
                poll_wait = 5.0
                location = None

                while (time.time() - start_time) < 1800:
                    access_token = tm.get_token()
                    headers["Authorization"] = f"Bearer {access_token}"
                    r = requests.post(REPORT_POLL_URL, headers=headers, json={"ReportRequestId": request_id}, timeout=20)
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
                    clean_url = html.unescape(location)
                    r = requests.get(clean_url, stream=True, timeout=300)
                    r.raise_for_status()

                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                                if chunk:
                                    tmp.write(chunk)
                            tmp_path = tmp.name

                        with zipfile.ZipFile(tmp_path) as z:
                            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                            if csv_files:
                                with z.open(csv_files[0], "r") as f:
                                    raw_content = f.read().decode("utf-8", errors="replace")
                                    lines = raw_content.splitlines()
                                    header_idx = 0
                                    for i, line in enumerate(lines):
                                        if "CampaignId" in line:
                                            header_idx = i
                                            break
                                    reader = csv_module.DictReader(lines[header_idx:])
                                    for row in reader:
                                        ds = row.get("TimePeriod", "")
                                        if not ds:
                                            continue
                                        try:
                                            clean_date = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%m-%d")
                                        except Exception:
                                            continue
                                        key = (clean_date, "microsoft", str(account_id))
                                        dm = combined.setdefault(key, DailyMetrics())
                                        dm.add(
                                            int(row.get("Impressions", "0").replace(",", "") or 0),
                                            int(row.get("Clicks", "0").replace(",", "") or 0),
                                            float(row.get("Spend", "0").replace(",", "") or 0.0),
                                        )
                                        acct_rows += 1
                    finally:
                        if tmp_path and os.path.exists(tmp_path):
                            os.unlink(tmp_path)

                if progress:
                    progress.account_done(account_id, acct_rows)

            except Exception as e:
                if progress:
                    progress.account_error(account_id, 0, str(e))

    if progress:
        progress.done_channel("Microsoft Ads", len(combined))
    return combined
