import React, { useState } from 'react';
import { useData } from '../contexts/DataContext';
import { LayoutDashboard, Upload, Loader2, Play, ChevronRight, Zap, BarChart3 } from 'lucide-react';
import { cn } from '../lib/utils';
import { ScraperDialog } from './ScraperDialog';

interface SidebarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

const CHANNEL_ICONS: Record<string, string> = {
  'Google': '#4285F4',
  'YouTube': '#FF0000',
  'Facebook': '#1877F2',
  'Instagram': '#E4405F',
  'Microsoft': '#00A4EF',
  'TikTok': '#000000',
  'Snapchat': '#FFFC00',
  'X (Twitter)': '#1DA1F2',
  'Pinterest': '#E60023',
  'Amazon Ads': '#FF9900',
  'Reddit': '#FF4500',
  'Vibe': '#7C3AED',
};

export const Sidebar: React.FC<SidebarProps> = ({ activeTab, setActiveTab }) => {
  const { channels, processFile, isLoading, isLoaded, getChannelStats } = useData();
  const [isScraperOpen, setIsScraperOpen] = useState(false);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      processFile(e.target.files[0]);
    }
  };

  return (
    <>
      <aside className="w-72 bg-slate-900 z-10 hidden md:flex flex-col flex-shrink-0 h-screen">
        {/* Logo */}
        <div className="px-6 py-5 flex items-center gap-3">
          <div className="w-9 h-9 bg-gradient-to-br from-indigo-400 to-indigo-600 rounded-xl flex items-center justify-center shadow-lg shadow-indigo-500/20">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-base font-bold text-white tracking-tight">DataAudit</h1>
            <p className="text-[10px] text-slate-500 font-medium uppercase tracking-widest">Integrity Dashboard</p>
          </div>
        </div>

        {/* Navigation */}
        <div className="flex-1 overflow-y-auto px-3 py-2 space-y-1">
          <button
            onClick={() => setActiveTab('overview')}
            className={cn(
              "w-full flex items-center gap-3 px-4 py-2.5 rounded-xl text-sm font-medium transition-all duration-200",
              activeTab === 'overview'
                ? "bg-indigo-600 text-white shadow-lg shadow-indigo-600/25"
                : "text-slate-400 hover:bg-slate-800 hover:text-slate-200"
            )}
          >
            <LayoutDashboard className="w-4.5 h-4.5" />
            Overview
          </button>

          {isLoaded && channels.length > 0 && (
            <div className="pt-4 pb-1">
              <p className="text-[10px] uppercase text-slate-600 font-bold px-4 mb-2 tracking-widest">Channels</p>
              <div className="space-y-0.5">
                {channels.map(channel => {
                  const stats = getChannelStats(channel);
                  const color = CHANNEL_ICONS[channel] || '#6366f1';
                  return (
                    <button
                      key={channel}
                      onClick={() => setActiveTab(channel)}
                      className={cn(
                        "w-full flex items-center gap-3 px-4 py-2 rounded-xl text-sm transition-all duration-200 group",
                        activeTab === channel
                          ? "bg-slate-800 text-white"
                          : "text-slate-400 hover:bg-slate-800/60 hover:text-slate-300"
                      )}
                    >
                      <span
                        className="w-2.5 h-2.5 rounded-full flex-shrink-0 ring-2 ring-transparent"
                        style={{ backgroundColor: color }}
                      />
                      <span className="truncate flex-1 text-left">{channel}</span>
                      {stats.issues > 0 && (
                        <span className="text-[10px] bg-red-500/20 text-red-400 font-bold px-1.5 py-0.5 rounded-md">
                          {stats.issues}
                        </span>
                      )}
                      <ChevronRight className={cn(
                        "w-3.5 h-3.5 flex-shrink-0 transition-opacity",
                        activeTab === channel ? "opacity-60" : "opacity-0 group-hover:opacity-40"
                      )} />
                    </button>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Bottom actions */}
        <div className="p-3 space-y-2 border-t border-slate-800">
          <button
            onClick={() => setIsScraperOpen(true)}
            className="w-full bg-gradient-to-r from-emerald-500 to-emerald-600 hover:from-emerald-600 hover:to-emerald-700 text-white text-xs font-bold py-2.5 px-4 rounded-xl transition-all flex items-center justify-center gap-2 shadow-lg shadow-emerald-600/20"
          >
            <Play className="w-4 h-4" />
            Run Auto Scraper
          </button>

          <label className={cn(
            "cursor-pointer bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold py-2.5 px-4 rounded-xl w-full block text-center transition-all flex items-center justify-center gap-2 border border-slate-700",
            isLoading && "opacity-60 cursor-not-allowed"
          )}>
            {isLoading ? (
              <><Loader2 className="w-4 h-4 animate-spin" /> Processing...</>
            ) : (
              <><Upload className="w-4 h-4" /> Upload CSV</>
            )}
            <input
              type="file"
              className="hidden"
              accept=".csv"
              onChange={handleFileChange}
              disabled={isLoading}
            />
          </label>
        </div>
      </aside>

      <ScraperDialog isOpen={isScraperOpen} onClose={() => setIsScraperOpen(false)} />
    </>
  );
};
