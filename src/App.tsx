import { useState } from 'react';
import { DataProvider, useData } from './contexts/DataContext';
import { Sidebar } from './components/Sidebar';
import { FilterBar } from './components/FilterBar';
import { Overview } from './components/Overview';
import { ChannelDetail } from './components/ChannelDetail';
import { Activity } from 'lucide-react';

const DashboardContent = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const { data, isLoaded } = useData();

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50 font-sans antialiased">
      <Sidebar activeTab={activeTab} setActiveTab={setActiveTab} />

      <main className="flex-1 overflow-y-auto relative w-full flex flex-col min-w-0">
        {/* Header */}
        <header className="bg-white/80 backdrop-blur-lg px-8 py-5 sticky top-0 z-20 flex justify-between items-center flex-shrink-0 border-b border-slate-200/60">
          <div>
            <h2 className="text-xl font-semibold text-slate-800 tracking-tight">
              {activeTab === 'overview' ? 'Executive Summary' : `${activeTab}`}
            </h2>
            <p className="text-xs text-slate-400 mt-0.5">
              BigQuery Warehouse vs. Ad Platform API reconciliation
            </p>
          </div>
          <div className="flex items-center gap-3">
            {isLoaded && (
              <div className="flex items-center gap-2 bg-indigo-50 text-indigo-700 px-3.5 py-1.5 rounded-full border border-indigo-100">
                <Activity className="w-3.5 h-3.5" />
                <span className="text-xs font-semibold tabular-nums">
                  {data.length.toLocaleString()} rows
                </span>
              </div>
            )}
          </div>
        </header>

        {/* Filters */}
        {data.length > 0 && <FilterBar />}

        {/* Content */}
        <div className="p-6 lg:p-8 max-w-[1400px] mx-auto w-full flex-1 animate-fade-in">
          {activeTab === 'overview' ? (
            <Overview setActiveTab={setActiveTab} />
          ) : (
            <ChannelDetail channel={activeTab} />
          )}
        </div>
      </main>
    </div>
  );
};

export default function App() {
  return (
    <DataProvider>
      <DashboardContent />
    </DataProvider>
  );
}
