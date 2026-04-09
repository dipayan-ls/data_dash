import React from 'react';
import { useData } from '../contexts/DataContext';
import { formatCurrency, cn } from '../lib/utils';
import { CheckCircle2, AlertTriangle, BarChart3, TrendingUp, ArrowRight, Users } from 'lucide-react';

interface OverviewProps {
  setActiveTab: (tab: string) => void;
}

export const Overview: React.FC<OverviewProps> = ({ setActiveTab }) => {
  const { filteredData, getChannelStats } = useData();

  const visibleChannels = Array.from(new Set(filteredData.map(d => d.group))).sort();

  if (visibleChannels.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-24 animate-fade-in">
        <div className="w-20 h-20 bg-slate-100 rounded-2xl flex items-center justify-center mb-5">
          <BarChart3 className="w-10 h-10 text-slate-300" />
        </div>
        <h3 className="text-lg font-semibold text-slate-700">No data loaded</h3>
        <p className="text-sm text-slate-400 mt-1 max-w-sm text-center">
          Run the auto scraper or upload a CSV file to begin reconciliation analysis.
        </p>
      </div>
    );
  }

  // Compute totals
  const totalBqSpend = filteredData.reduce((s, d) => s + d.bigquery_spends, 0);
  const totalApiSpend = filteredData.reduce((s, d) => s + d.api_spends, 0);
  const totalVariance = totalApiSpend - totalBqSpend;
  const totalIssues = visibleChannels.reduce((s, ch) => s + getChannelStats(ch).issues, 0);

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <SummaryCard label="BQ Total Spend" value={formatCurrency(totalBqSpend)} icon={<BarChart3 className="w-4 h-4" />} accent="indigo" />
        <SummaryCard label="API Total Spend" value={formatCurrency(totalApiSpend)} icon={<TrendingUp className="w-4 h-4" />} accent="emerald" />
        <SummaryCard
          label="Net Variance"
          value={formatCurrency(totalVariance)}
          icon={<AlertTriangle className="w-4 h-4" />}
          accent={Math.abs(totalVariance) < 1 ? "slate" : "red"}
        />
        <SummaryCard label="Discrepancies" value={totalIssues.toString()} icon={<Users className="w-4 h-4" />} accent={totalIssues > 0 ? "amber" : "slate"} />
      </div>

      {/* Channel cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {visibleChannels.map(channel => {
          const stats = getChannelStats(channel);
          const isHealthy = stats.issues === 0;
          const variancePct = stats.totalSpend > 0 ? ((stats.totalDiff / stats.totalSpend) * 100) : 0;

          return (
            <button
              key={channel}
              onClick={() => setActiveTab(channel)}
              className="bg-white p-5 rounded-2xl border border-slate-200/60 hover:border-slate-300 shadow-sm hover:shadow-md transition-all duration-200 cursor-pointer group text-left animate-fade-in"
            >
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className={cn(
                    "w-10 h-10 rounded-xl flex items-center justify-center transition-colors",
                    isHealthy ? "bg-emerald-50 text-emerald-500" : "bg-red-50 text-red-500"
                  )}>
                    {isHealthy ? <CheckCircle2 className="w-5 h-5" /> : <AlertTriangle className="w-5 h-5" />}
                  </div>
                  <div>
                    <h3 className="font-semibold text-slate-800 text-sm">{channel}</h3>
                    <p className="text-[11px] text-slate-400">{stats.accounts} account{stats.accounts !== 1 ? 's' : ''}</p>
                  </div>
                </div>
                <ArrowRight className="w-4 h-4 text-slate-300 group-hover:text-indigo-500 group-hover:translate-x-0.5 transition-all" />
              </div>

              <div className="space-y-2.5">
                <div className="flex justify-between text-xs items-center">
                  <span className="text-slate-400">BQ Spend</span>
                  <span className="font-semibold text-slate-700 tabular-nums">{formatCurrency(stats.totalSpend)}</span>
                </div>
                <div className="flex justify-between text-xs items-center">
                  <span className="text-slate-400">Variance</span>
                  <span className={cn(
                    "font-semibold tabular-nums",
                    Math.abs(stats.totalDiff) < 1 ? "text-slate-500" : "text-red-500"
                  )}>
                    {formatCurrency(stats.totalDiff)} ({variancePct.toFixed(1)}%)
                  </span>
                </div>

                {/* Health bar */}
                <div className="pt-2 border-t border-slate-100">
                  <div className="flex justify-between items-center text-xs">
                    <span className={cn(
                      "font-bold uppercase tracking-wider text-[10px]",
                      isHealthy ? "text-emerald-600" : "text-red-500"
                    )}>
                      {isHealthy ? 'Healthy' : `${stats.issues} discrepanc${stats.issues === 1 ? 'y' : 'ies'}`}
                    </span>
                    <div className={cn(
                      "w-2 h-2 rounded-full",
                      isHealthy ? "bg-emerald-400" : "bg-red-400 animate-pulse"
                    )} />
                  </div>
                </div>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
};

const SummaryCard: React.FC<{
  label: string; value: string; icon: React.ReactNode;
  accent: 'indigo' | 'emerald' | 'red' | 'amber' | 'slate';
}> = ({ label, value, icon, accent }) => {
  const colors = {
    indigo: 'bg-indigo-50 text-indigo-600 border-indigo-100',
    emerald: 'bg-emerald-50 text-emerald-600 border-emerald-100',
    red: 'bg-red-50 text-red-600 border-red-100',
    amber: 'bg-amber-50 text-amber-600 border-amber-100',
    slate: 'bg-slate-50 text-slate-500 border-slate-100',
  };

  return (
    <div className={cn("rounded-2xl p-4 border", colors[accent])}>
      <div className="flex items-center gap-2 mb-1">
        {icon}
        <span className="text-[11px] font-semibold uppercase tracking-wider opacity-70">{label}</span>
      </div>
      <p className="text-xl font-bold tabular-nums">{value}</p>
    </div>
  );
};
