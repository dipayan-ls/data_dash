export interface CsvRow {
  date: string;
  workspace_name: string;
  source: string;
  ad_account_id: string;
  account_name: string;
  api_impressions: number;
  api_clicks: number;
  api_spends: number;
  bigquery_impressions: number;
  bigquery_clicks: number;
  bigquery_spends: number;
}

export interface AggregatedData {
  group: string;
  workspace: string;
  account: string;
  accountName: string;
  date: string;
  api_impressions: number;
  api_clicks: number;
  api_spends: number;
  bigquery_impressions: number;
  bigquery_clicks: number;
  bigquery_spends: number;
}

export interface ChannelStats {
  totalSpend: number;
  totalDiff: number;
  issues: number;
  totalApiSpend: number;
  accounts: number;
}

export type SortOption = 'date-desc' | 'date-asc' | 'diff-desc' | 'diff-asc' | 'spend-desc' | 'spend-asc';

export interface FilterState {
  workspaces: string[];
  startDate: string;
  endDate: string;
  sort: SortOption;
  channels: string[];
}
