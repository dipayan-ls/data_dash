"""
Data Integrity Dashboard — Services Package

Modular, fault-tolerant microservice architecture for ad platform data collection.
Each platform runs independently; a failure in one never crashes others.
"""

from services.types import DailyMetrics, DailyMetricsMap, DateStr, SourceStr, AccountStr
from services.registry import CHANNEL_LABELS, CONNECTOR_TO_CHANNEL, CHANNEL_API_DISPATCH
from services.orchestrator import run_scraper_api, load_workspace_metadata

__all__ = [
    "DailyMetrics",
    "DailyMetricsMap",
    "DateStr",
    "SourceStr",
    "AccountStr",
    "CHANNEL_LABELS",
    "CONNECTOR_TO_CHANNEL",
    "CHANNEL_API_DISPATCH",
    "run_scraper_api",
    "load_workspace_metadata",
]
