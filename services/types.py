"""Shared type definitions used across all services."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple

DateStr = str       # "YYYY-MM-DD"
SourceStr = str     # e.g. "google", "youtube", "facebook"
AccountStr = str    # ad account ID (cleaned, no dashes/act_ prefix)


@dataclass
class DailyMetrics:
    """Accumulator for daily impressions / clicks / spend."""
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0

    def add(self, imp: int, clk: int, spend: float) -> None:
        self.impressions += int(imp or 0)
        self.clicks += int(clk or 0)
        self.spend += float(spend or 0.0)


# Key is (date, source, account_id) — account_id may be "" if unknown.
DailyMetricsMap = Dict[Tuple[DateStr, SourceStr, AccountStr], DailyMetrics]
