import React, { createContext, useContext, useState, useMemo, ReactNode } from 'react';
import Papa from 'papaparse';
import { AggregatedData, FilterState, ChannelStats } from '../types';
import { getChannelGroup } from '../lib/utils';

interface DataContextType {
  data: AggregatedData[];
  isLoaded: boolean;
  isLoading: boolean;
  workspaces: string[];
  channels: string[];
  filters: FilterState;
  setFilters: React.Dispatch<React.SetStateAction<FilterState>>;
  processFile: (file: File) => void;
  filteredData: AggregatedData[];
  getChannelStats: (channel: string) => ChannelStats;
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const useData = () => {
  const context = useContext(DataContext);
  if (!context) throw new Error('useData must be used within a DataProvider');
  return context;
};

export const DataProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [data, setData] = useState<AggregatedData[]>([]);
  const [isLoaded, setIsLoaded] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [filters, setFilters] = useState<FilterState>({
    workspaces: [],
    startDate: '',
    endDate: '',
    sort: 'date-desc',
    channels: []
  });

  const processFile = (file: File) => {
    setIsLoading(true);
    const streamStorage: Record<string, AggregatedData> = {};

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      step: (row: any) => {
        const d = row.data;
        if (!d.date || !d.source) return;

        const date = d.date.trim();
        const rawSource = d.source.toLowerCase().trim();
        const groupName = getChannelGroup(rawSource);
        const workspace = (d.workspace_name || d.workspace || 'Unknown').trim();
        const accountId = (d.ad_account_id || d.ad_account || d.account_id || 'N/A').trim();
        const accountName = (d.account_name || '').trim();

        const uniqueKey = `${groupName}|${workspace}|${accountId}|${date}`;

        if (!streamStorage[uniqueKey]) {
          streamStorage[uniqueKey] = {
            group: groupName,
            workspace,
            account: accountId,
            accountName,
            date,
            api_impressions: 0,
            api_clicks: 0,
            api_spends: 0,
            bigquery_impressions: 0,
            bigquery_clicks: 0,
            bigquery_spends: 0
          };
        }

        const entry = streamStorage[uniqueKey];
        if (accountName && !entry.accountName) entry.accountName = accountName;
        entry.api_impressions += parseFloat(d.api_impressions || '0');
        entry.api_clicks += parseFloat(d.api_clicks || '0');
        entry.api_spends += parseFloat(d.api_spends || '0');
        entry.bigquery_impressions += parseFloat(d.bigquery_impressions || '0');
        entry.bigquery_clicks += parseFloat(d.bigquery_clicks || '0');
        entry.bigquery_spends += parseFloat(d.bigquery_spends || '0');
      },
      complete: () => {
        setData(Object.values(streamStorage));
        setIsLoaded(true);
        setIsLoading(false);
      },
      error: (err) => {
        console.error("CSV Parse Error:", err);
        setIsLoading(false);
      }
    });
  };

  const workspaces = useMemo(() => {
    return Array.from(new Set(data.map(d => d.workspace))).sort();
  }, [data]);

  const channels = useMemo(() => {
    return Array.from(new Set(data.map(d => d.group))).sort();
  }, [data]);

  const filteredData = useMemo(() => {
    let filtered = data.filter(d => {
      if (filters.workspaces.length > 0 && !filters.workspaces.includes(d.workspace)) return false;
      if (filters.channels.length > 0 && !filters.channels.includes(d.group)) return false;
      if (filters.startDate && d.date < filters.startDate) return false;
      if (filters.endDate && d.date > filters.endDate) return false;
      return true;
    });

    return filtered.sort((a, b) => {
      const dateA = new Date(a.date).getTime();
      const dateB = new Date(b.date).getTime();
      const diffA = Math.abs(a.api_spends - a.bigquery_spends);
      const pctA = a.bigquery_spends > 0 ? (diffA / a.bigquery_spends) : 0;
      const diffB = Math.abs(b.api_spends - b.bigquery_spends);
      const pctB = b.bigquery_spends > 0 ? (diffB / b.bigquery_spends) : 0;

      switch(filters.sort) {
        case 'date-asc': return dateA - dateB;
        case 'date-desc': return dateB - dateA;
        case 'diff-desc': return pctB - pctA;
        case 'diff-asc': return pctA - pctB;
        case 'spend-desc': return b.bigquery_spends - a.bigquery_spends;
        case 'spend-asc': return a.bigquery_spends - b.bigquery_spends;
        default: return dateB - dateA;
      }
    });
  }, [data, filters]);

  const getChannelStats = (channel: string): ChannelStats => {
    const channelData = filteredData.filter(d => d.group === channel);
    let totalSpend = 0, totalDiff = 0, totalApiSpend = 0, issues = 0;
    const accountSet = new Set<string>();

    channelData.forEach(d => {
      totalSpend += d.bigquery_spends;
      totalApiSpend += d.api_spends;
      const diff = d.api_spends - d.bigquery_spends;
      totalDiff += diff;
      accountSet.add(d.account);
      const pct = d.bigquery_spends > 0 ? (diff / d.bigquery_spends) * 100 : 0;
      if (Math.abs(pct) > 1 && d.bigquery_spends > 5) issues++;
    });

    return { totalSpend, totalDiff, issues, totalApiSpend, accounts: accountSet.size };
  };

  return (
    <DataContext.Provider value={{ data, isLoaded, isLoading, workspaces, channels, filters, setFilters, processFile, filteredData, getChannelStats }}>
      {children}
    </DataContext.Provider>
  );
};
