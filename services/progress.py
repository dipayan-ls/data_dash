"""Lightweight progress reporter — pure print, no external deps."""

from __future__ import annotations

import datetime as dt


class ProgressReporter:
    """
    Print-based status tracker with workspace / channel / account / BQ / CSV levels.
    """

    def __init__(self, total_workspaces: int) -> None:
        self.total_workspaces = total_workspaces
        self._ws_idx = 0
        self._api_rows: int = 0
        self._ts_fmt = "%H:%M:%S"

    def _ts(self) -> str:
        return dt.datetime.now().strftime(self._ts_fmt)

    # ── workspace ──

    def start_workspace(self, ws_id: str, ws_name: str) -> None:
        self._ws_idx += 1
        self._api_rows = 0
        print(
            f"\n{'=' * 60}\n"
            f"  [{self._ws_idx}/{self.total_workspaces}] Workspace: {ws_name} ({ws_id})  [{self._ts()}]\n"
            f"{'=' * 60}"
        )

    def done_workspace(self, ws_name: str, csv_rows: int) -> None:
        print(
            f"  [{self._ws_idx}/{self.total_workspaces}] Done: {ws_name}  "
            f"{csv_rows:,} CSV rows  [{self._ts()}]"
        )

    # ── channel ──

    def start_channel(self, label: str, total_accounts: int) -> None:
        suffix = f"  ({total_accounts} account(s))" if total_accounts else ""
        print(f"\n  -- Channel: {label}{suffix}  [{self._ts()}]")

    def done_channel(self, label: str, rows: int) -> None:
        print(f"  -- Done: {label}  {rows:,} rows  [{self._ts()}]")

    def skip_channel(self, label: str) -> None:
        print(f"  -- Skipped: {label} (no credentials)  [{self._ts()}]")

    # ── account ──

    def start_account(self, account_id: str, idx: int, total: int) -> None:
        print(f"     Fetching account [{idx}/{total}]: {account_id}  [{self._ts()}]")

    def account_done(self, account_id: str, rows: int) -> None:
        print(f"       OK  {rows:,} rows for {account_id}")
        self._api_rows += rows

    def account_error(self, account_id: str, status: int, msg: str) -> None:
        print(f"       ERR  HTTP {status} for {account_id}: {msg}")

    def account_retry(self, account_id: str, attempt: int, max_retries: int, wait: float, reason: str) -> None:
        print(f"       Retry {attempt}/{max_retries} for {account_id} in {wait:.0f}s ({reason})")

    def chunk_progress(self, account_id: str, chunk_start: str, chunk_end: str, page: int) -> None:
        print(f"       {account_id}  {chunk_start} -> {chunk_end}  page {page}  [{self._ts()}]")

    # ── BigQuery ──

    def bq_query_start(self, workspace_id: str, account_count: int) -> None:
        print(f"\n  -- BigQuery for '{workspace_id}'  ({account_count} account(s))  [{self._ts()}]")

    def bq_query_done(self, rows: int) -> None:
        print(f"       OK  {rows:,} BigQuery rows  [{self._ts()}]")

    # ── CSV ──

    def csv_rows_written(self, n: int) -> None:
        print(f"  -- CSV rows written: {n:,}  [{self._ts()}]")

    # ── day-level polling (Reddit, etc.) ──

    def day_progress(self, account_id: str, date_str: str, total_days: int, done_days: int) -> None:
        if done_days % 10 == 0 or done_days == 1:
            print(f"       {account_id}  day {done_days}/{total_days}: {date_str}  [{self._ts()}]")
