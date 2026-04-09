"""
BigQuery metric fetching service.

Target table: moda-platform-prd.{workspace_id}.ad_insights
Key column:   account_id  (was ad_account_id on adspends_mat_view)
Also returns: account_name for enriched output
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

from google.cloud import bigquery

from services.credentials import load_bq_credentials
from services.progress import ProgressReporter
from services.types import DailyMetrics, DailyMetricsMap


# Map internal channel keys to expected BQ "source" column values (lowercased).
CHANNEL_TO_BQ_SOURCES: Mapping[str, List[str]] = {
    "google_youtube": ["google", "youtube"],
    "meta": ["facebook", "instagram", "fb", "insta"],
    "microsoft": ["microsoft", "bing"],
    "pinterest": ["pinterest"],
    "snapchat": ["snapchat"],
    "tiktok": ["tiktok"],
    "twitter_x": ["twitter", "x"],
    "amazon": ["amazon"],
    "reddit": ["reddit"],
    "vibe": ["vibe"],
}


def fetch_bigquery_metrics(
    workspace_id: str,
    start_date: str,
    end_date: str,
    account_ids: Iterable[str],
    selected_channels: Iterable[str],
    progress: Optional[ProgressReporter] = None,
) -> Tuple[DailyMetricsMap, Dict[str, str]]:
    """
    Fetch daily metrics from BigQuery for all selected channels in ONE query.

    Table:  moda-platform-prd.{workspace_id}.ad_insights
    Column: account_id   (mapped from old ad_account_id)
    Extra:  account_name (returned for enriched CSV output)

    Returns:
        (metrics_map, account_names_map)
        - metrics_map:       {(date, source, account_id): DailyMetrics}
        - account_names_map: {account_id: account_name}
    """
    credentials = load_bq_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    wanted_sources: Set[str] = set()
    for ch in selected_channels:
        wanted_sources.update(CHANNEL_TO_BQ_SOURCES.get(ch, []))
    if not wanted_sources:
        return {}, {}

    cleaned_ids = [acc.replace("-", "").strip() for acc in account_ids if acc.strip()]
    table_ref = f"`moda-platform-prd.{workspace_id}.ad_insights`"
    source_list_sql = ", ".join([f"'{src}'" for src in wanted_sources])

    if cleaned_ids:
        account_id_list_sql = ", ".join([f"'{aid}'" for aid in cleaned_ids])
        query = f"""
            SELECT
                date,
                LOWER(source) AS source,
                CAST(REPLACE(CAST(account_id AS STRING), '-', '') AS STRING) AS account_id,
                MAX(account_name) AS account_name,
                CAST(SUM(impressions) AS FLOAT64) AS impressions,
                CAST(SUM(clicks) AS FLOAT64) AS clicks,
                CAST(SUM(spend) AS FLOAT64) AS spend
            FROM {table_ref}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
              AND REPLACE(CAST(account_id AS STRING), '-', '') IN ({account_id_list_sql})
              AND LOWER(source) IN ({source_list_sql})
            GROUP BY date, source, account_id
            ORDER BY date, source, account_id
        """
    else:
        query = f"""
            SELECT
                date,
                LOWER(source) AS source,
                CAST(REPLACE(CAST(account_id AS STRING), '-', '') AS STRING) AS account_id,
                MAX(account_name) AS account_name,
                CAST(SUM(impressions) AS FLOAT64) AS impressions,
                CAST(SUM(clicks) AS FLOAT64) AS clicks,
                CAST(SUM(spend) AS FLOAT64) AS spend
            FROM {table_ref}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
              AND LOWER(source) IN ({source_list_sql})
            GROUP BY date, source, account_id
            ORDER BY date, source, account_id
        """

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        use_query_cache=True,
    )

    if progress:
        progress.bq_query_start(workspace_id, len(list(account_ids)))

    metrics: DailyMetricsMap = {}
    account_names: Dict[str, str] = {}

    query_job = client.query(query, job_config=job_config)
    row_count = 0
    for row in query_job.result(page_size=1000):
        row_count += 1
        raw_date = row["date"]
        if hasattr(raw_date, "strftime"):
            date_str = raw_date.strftime("%Y-%m-%d")
        else:
            date_str = str(raw_date)[:10]

        src = str(row["source"]).lower()
        acct = str(row["account_id"] or "")
        acct_name = str(row["account_name"] or "")

        if acct and acct_name:
            account_names[acct] = acct_name

        key = (date_str, src, acct)
        dm = metrics.setdefault(key, DailyMetrics())
        dm.add(
            int(row["impressions"] or 0),
            int(row["clicks"] or 0),
            float(row["spend"] or 0.0),
        )

    if progress:
        progress.bq_query_done(row_count)

    return metrics, account_names
