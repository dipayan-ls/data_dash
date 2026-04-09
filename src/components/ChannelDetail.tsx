import React, { useMemo, useState } from 'react';
import { useData } from '../contexts/DataContext';
import { formatCurrency, formatNumber, cn } from '../lib/utils';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, AreaChart
} from 'recharts';
import { ChevronLeft, ChevronRight, Download, TrendingUp, TrendingDown, Activity } from 'lucide-react';

interface ChannelDetailProps {
  channel: string;
}

export const ChannelDetail: React.FC<ChannelDetailProps> = ({ channel }) => {
  const { filteredData, getChannelStats } = useData();
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 25;

  const channelData = useMemo(() => filteredData.filter(d => d.group === channel), [filteredData, channel]);

  const chartData = useMemo(() => {
    const map = new Map<string, { date: string; api: number; bq: number }>();
    channelData.forEach(d => {
      if (!map.has(d.date)) map.set(d.date, { date: d.date, api: 0, bq: 0 });
      const entry = map.get(d.date)!;
      entry.api += d.api_spends;
      entry.bq += d.bigquery_spends;
    });
    return Array.from(map.values()).sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
  }, [channelData]);

  const totalPages = Math.ceil(channelData.length / itemsPerPage);
  const paginatedData = channelData.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

  const handlePageChange = (p: number) => { if (p >= 1 && p <= totalPages) setCurrentPage(p); };

  const handleDownloadCSV = () => {
    if (channelData.length === 0) return;
    const headers = ['Date', 'Ad Account', 'Account Name', 'BQ Spend', 'API Spend', 'Diff ($)', 'Diff (%)', 'BQ Impr', 'API Impr'];
    const rows = channelData.map(row => {
      const diff = row.api_spends - row.bigquery_spends;
      const pct = row.bigquery_spends > 0 ? (diff / row.bigquery_spends) * 100 : 0;
      return [row.date, `"${row.account}"`, `"${row.accountName}"`, row.bigquery_spends.toFixed(2), row.api_spends.toFixed(2), diff.toFixed(2), `${pct.toFixed(2)}%`, row.bigquery_impressions, row.api_impressions].join(',');
    });
    const blob = new Blob([[headers.join(','), ...rows].join('\n')], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `${channel.replace(/\s+/g, '_')}_data.csv`;
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  if (channelData.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-24 animate-fade-in">
        <Activity className="w-12 h-12 text-slate-300 mb-4" />
        <p className="text-slate-400 text-sm">No data for <span className="font-semibold text-slate-600">{channel}</span> with current filters.</p>
      </div>
    );
  }

  const stats = getChannelStats(channel);

  return (
    <div className="space-y-5 animate-fade-in">
      {/* Stat pills */}
      <div className="flex flex-wrap gap-3">
        <StatPill label="BQ Spend" value={formatCurrency(stats.totalSpend)} icon={<TrendingUp className="w-3.5 h-3.5" />} />
        <StatPill label="API Spend" value={formatCurrency(stats.totalApiSpend)} icon={<TrendingDown className="w-3.5 h-3.5" />} />
        <StatPill
          label="Variance"
          value={formatCurrency(stats.totalDiff)}
          icon={<Activity className="w-3.5 h-3.5" />}
          alert={Math.abs(stats.totalDiff) > 1}
        />
        <StatPill label="Issues" value={String(stats.issues)} alert={stats.issues > 0} />
      </div>

      {/* Chart */}
      <div className="bg-white p-5 rounded-2xl shadow-sm border border-slate-200/60">
        <h3 className="text-sm font-semibold text-slate-700 mb-4">Spend Trend</h3>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="bqGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6366f1" stopOpacity={0.12} />
                  <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="apiGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.12} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#94a3b8' }} tickLine={false} axisLine={{ stroke: '#e2e8f0' }} />
              <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} tickLine={false} axisLine={false} tickFormatter={v => `$${v}`} />
              <Tooltip
                contentStyle={{ borderRadius: '12px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0/0.07)', fontSize: '12px' }}
                formatter={(val: number) => formatCurrency(val)}
              />
              <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
              <Area type="monotone" dataKey="bq" name="BigQuery" stroke="#6366f1" strokeWidth={2} fill="url(#bqGrad)" dot={false} activeDot={{ r: 5, fill: '#6366f1' }} />
              <Area type="monotone" dataKey="api" name="API" stroke="#10b981" strokeWidth={2} fill="url(#apiGrad)" dot={false} strokeDasharray="5 5" activeDot={{ r: 5, fill: '#10b981' }} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Table */}
      <div className="bg-white rounded-2xl shadow-sm border border-slate-200/60 overflow-hidden">
        <div className="px-5 py-3 bg-slate-50/80 border-b border-slate-200/60 flex justify-between items-center">
          <h3 className="text-sm font-semibold text-slate-700">Reconciliation Data</h3>
          <div className="flex items-center gap-3 text-xs">
            <button
              onClick={handleDownloadCSV}
              className="flex items-center gap-1.5 px-3 py-1.5 font-semibold text-slate-600 bg-white border border-slate-200 rounded-lg hover:bg-emerald-50 hover:text-emerald-700 hover:border-emerald-200 transition-colors shadow-sm"
            >
              <Download className="w-3.5 h-3.5" /> Export
            </button>
            <div className="flex items-center gap-2 text-slate-500">
              <button onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 1} className="p-1 rounded-md hover:bg-slate-200 disabled:opacity-30 transition">
                <ChevronLeft className="w-4 h-4" />
              </button>
              <span className="tabular-nums font-medium">{currentPage}/{totalPages}</span>
              <button onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage === totalPages} className="p-1 rounded-md hover:bg-slate-200 disabled:opacity-30 transition">
                <ChevronRight className="w-4 h-4" />
              </button>
            </div>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs text-slate-600">
            <thead className="bg-slate-50/50 text-[11px] uppercase font-semibold text-slate-400 tracking-wider border-b border-slate-100">
              <tr>
                <th className="px-5 py-3 whitespace-nowrap">Date</th>
                <th className="px-5 py-3 whitespace-nowrap">Account</th>
                <th className="px-5 py-3 text-right whitespace-nowrap">BQ Spend</th>
                <th className="px-5 py-3 text-right whitespace-nowrap">API Spend</th>
                <th className="px-5 py-3 text-right whitespace-nowrap">Diff</th>
                <th className="px-5 py-3 text-right whitespace-nowrap">Diff %</th>
                <th className="px-5 py-3 text-center whitespace-nowrap">Status</th>
                <th className="px-5 py-3 text-right whitespace-nowrap">BQ Impr</th>
                <th className="px-5 py-3 text-right whitespace-nowrap">API Impr</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100/80">
              {paginatedData.map((row, idx) => {
                const diff = row.api_spends - row.bigquery_spends;
                const pct = row.bigquery_spends > 0 ? (diff / row.bigquery_spends) * 100 : 0;
                const isErr = Math.abs(pct) > 1 && row.bigquery_spends > 1;

                return (
                  <tr key={`${row.date}-${row.account}-${idx}`} className={cn("hover:bg-slate-50/50 transition-colors", isErr && "bg-red-50/40")}>
                    <td className="px-5 py-3 font-mono text-slate-500 whitespace-nowrap">{row.date}</td>
                    <td className="px-5 py-3 whitespace-nowrap">
                      <div className="font-mono text-slate-600 text-[11px]">{row.account}</div>
                      {row.accountName && <div className="text-[10px] text-slate-400 truncate max-w-[160px]">{row.accountName}</div>}
                    </td>
                    <td className="px-5 py-3 text-right font-mono tabular-nums">{formatCurrency(row.bigquery_spends)}</td>
                    <td className="px-5 py-3 text-right font-mono tabular-nums">{formatCurrency(row.api_spends)}</td>
                    <td className={cn("px-5 py-3 text-right font-mono tabular-nums", diff !== 0 ? "font-medium text-slate-700" : "text-slate-300")}>
                      {formatCurrency(diff)}
                    </td>
                    <td className={cn("px-5 py-3 text-right font-mono tabular-nums", isErr ? "text-red-600 font-bold" : "text-slate-400")}>
                      {pct.toFixed(2)}%
                    </td>
                    <td className="px-5 py-3 text-center">
                      <span className={cn(
                        "inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-bold uppercase tracking-wide",
                        isErr ? "bg-red-100 text-red-600" : "bg-emerald-50 text-emerald-600"
                      )}>
                        {isErr ? 'Review' : 'OK'}
                      </span>
                    </td>
                    <td className="px-5 py-3 text-right text-slate-400 font-mono tabular-nums">{formatNumber(row.bigquery_impressions)}</td>
                    <td className="px-5 py-3 text-right text-slate-400 font-mono tabular-nums">{formatNumber(row.api_impressions)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

const StatPill: React.FC<{ label: string; value: string; icon?: React.ReactNode; alert?: boolean }> = ({ label, value, icon, alert }) => (
  <div className={cn(
    "flex items-center gap-2 px-3.5 py-2 rounded-xl border text-xs font-semibold",
    alert ? "bg-red-50 text-red-600 border-red-100" : "bg-white text-slate-600 border-slate-200/60"
  )}>
    {icon}
    <span className="text-slate-400 font-medium">{label}:</span>
    <span className="tabular-nums">{value}</span>
  </div>
);
