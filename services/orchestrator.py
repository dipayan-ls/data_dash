"""
Scraper orchestrator — coordinates platform services, BigQuery, and CSV output.

Each platform service runs independently with full fault isolation.
A failure in one platform never crashes others.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import os
import traceback
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from services.bigquery import fetch_bigquery_metrics
from services.credentials import get_reddit_credentials, get_workspace_channel_credentials
from services.progress import ProgressReporter
from services.registry import CHANNEL_LABELS, CONNECTOR_TO_CHANNEL, get_dispatch_table
from services.types import DailyMetrics, DailyMetricsMap, DateStr

# CSV output header — now includes account_name
CSV_HEADER = [
    "date",
    "workspace_name",
    "source",
    "ad_account_id",
    "account_name",
    "api_impressions",
    "api_clicks",
    "api_spends",
    "bigquery_impressions",
    "bigquery_clicks",
    "bigquery_spends",
]


def load_workspace_metadata(csv_path: Optional[str] = None) -> List[Tuple[str, str]]:
    """Load workspace_id and workspace_name from Workspace_information.csv."""
    if csv_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "Workspace_information.csv")

    if not os.path.exists(csv_path):
        raise RuntimeError(f"Workspace CSV not found: {csv_path}")

    workspaces: List[Tuple[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ws_id = (row.get("workspace_id") or "").strip()
            ws_name = (row.get("workspace_name") or "").strip()
            if ws_id:
                workspaces.append((ws_id, ws_name or ws_id))

    if not workspaces:
        raise RuntimeError("No workspace entries found in Workspace_information.csv.")
    return workspaces


def _aggregate_metrics(
    daily_metrics: DailyMetricsMap,
    granularity: str,
    start_date: str,
    end_date: str,
) -> DailyMetricsMap:
    """Roll up daily metrics into monthly / yearly / overall buckets."""
    summarized: DailyMetricsMap = {}
    for (date_str, source, account_id), dm in daily_metrics.items():
        d_clean = str(date_str)[:10].strip()
        if granularity == "monthly":
            bucket = d_clean[:7]
        elif granularity == "yearly":
            bucket = d_clean[:4]
        elif granularity == "overall":
            bucket = f"{start_date} to {end_date}"
        else:
            bucket = d_clean
        key = (bucket, source, account_id)
        target = summarized.setdefault(key, DailyMetrics())
        target.add(dm.impressions, dm.clicks, dm.spend)
    return summarized


def _scrape_one_workspace(
    workspace_id: str,
    workspace_name: str,
    start_date: DateStr,
    end_date: DateStr,
    selected_channels: List[str],
    writer: csv.writer,
    progress: ProgressReporter,
    granularity: str = "daily",
) -> int:
    """
    Scrape API + BigQuery for a single workspace. Write rows to shared CSV writer.
    Returns number of CSV rows written.

    Fault tolerance: Each platform is wrapped in its own try/except.
    A failure in one platform is logged and skipped; others continue.
    """
    dispatch = get_dispatch_table()

    # 1) Load credentials
    workspace_creds = get_workspace_channel_credentials(workspace_id)
    if "reddit" in selected_channels:
        reddit_creds = get_reddit_credentials(workspace_id)
        if reddit_creds:
            workspace_creds.extend(reddit_creds)

    creds_by_channel: Dict[str, List[Dict]] = defaultdict(list)
    for row in workspace_creds:
        connector_raw = (row.get("connector") or "").strip().lower()
        if not connector_raw:
            continue
        channel_key = CONNECTOR_TO_CHANNEL.get(connector_raw)
        if channel_key:
            creds_by_channel[channel_key].append(row)

    # 2) Fetch API metrics (each platform isolated)
    collected_account_ids: Set[str] = set()
    api_metrics: DailyMetricsMap = {}

    for ch in selected_channels:
        fetcher = dispatch.get(ch)
        if not fetcher:
            continue

        channel_creds = creds_by_channel.get(ch, [])
        if not channel_creds:
            if progress:
                progress.skip_channel(CHANNEL_LABELS.get(ch, ch))
            continue

        try:
            per_channel = fetcher(
                start_date=start_date,
                end_date=end_date,
                channel_credentials=channel_creds,
                progress=progress,
                account_ids_collector=collected_account_ids,
            )
            for key, dm in per_channel.items():
                bucket = api_metrics.setdefault(key, DailyMetrics())
                bucket.add(dm.impressions, dm.clicks, dm.spend)
        except Exception:
            label = CHANNEL_LABELS.get(ch, ch)
            print(f"\n  !! PLATFORM FAILURE: {label} !!")
            traceback.print_exc()
            print(f"  !! Continuing with remaining platforms...\n")

    # 3) Fetch BigQuery metrics
    bq_account_ids = list(collected_account_ids) if collected_account_ids else [""]
    try:
        bq_metrics, account_names = fetch_bigquery_metrics(
            workspace_id=workspace_id,
            start_date=start_date,
            end_date=end_date,
            account_ids=bq_account_ids,
            selected_channels=selected_channels,
            progress=progress,
        )
    except Exception:
        print("\n  !! BIGQUERY FAILURE !!")
        traceback.print_exc()
        bq_metrics = {}
        account_names = {}

    # 4) Aggregate if needed
    if granularity != "daily":
        api_metrics = _aggregate_metrics(api_metrics, granularity, start_date, end_date)
        bq_metrics = _aggregate_metrics(bq_metrics, granularity, start_date, end_date)

    # 5) Merge and write CSV rows
    all_keys = set(api_metrics.keys()) | set(bq_metrics.keys())

    def _sort_key(item):
        d, s, a = item
        if granularity == "daily":
            normalized = str(d)[:10].strip()
            try:
                return (dt.datetime.strptime(normalized, "%Y-%m-%d").date(), s, a)
            except ValueError:
                return (d, s, a)
        return (d, s, a)

    ws_rows = 0
    for date_bucket, source, account_id in sorted(all_keys, key=_sort_key):
        api_dm = api_metrics.get((date_bucket, source, account_id), DailyMetrics())
        bq_dm = bq_metrics.get((date_bucket, source, account_id), DailyMetrics())
        acct_name = account_names.get(account_id, "")
        writer.writerow([
            date_bucket,
            workspace_name,
            source,
            account_id,
            acct_name,
            str(api_dm.impressions),
            str(api_dm.clicks),
            f"{api_dm.spend:.2f}",
            str(bq_dm.impressions),
            str(bq_dm.clicks),
            f"{bq_dm.spend:.2f}",
        ])
        ws_rows += 1

    progress.csv_rows_written(ws_rows)
    return ws_rows


def run_scraper_api(
    workspaces: List[Tuple[str, str]],
    start_date: DateStr,
    end_date: DateStr,
    selected_channels: List[str],
    granularity: str = "daily",
) -> str:
    """API entry point. Returns CSV content as a string."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(CSV_HEADER)

    progress = ProgressReporter(total_workspaces=len(workspaces))

    for ws_id, ws_name in workspaces:
        progress.start_workspace(ws_id, ws_name)
        try:
            ws_rows = _scrape_one_workspace(
                workspace_id=ws_id,
                workspace_name=ws_name,
                start_date=start_date,
                end_date=end_date,
                selected_channels=selected_channels,
                writer=writer,
                progress=progress,
                granularity=granularity,
            )
            progress.done_workspace(ws_name, ws_rows)
        except Exception:
            print(f"\n  !! WORKSPACE FAILURE: {ws_name} !!")
            traceback.print_exc()
            progress.done_workspace(ws_name, 0)

    return output.getvalue()
