import React, { useState, useEffect } from 'react';
import { useData } from '../contexts/DataContext';
import { X, Loader2, Calendar, Database, Layers } from 'lucide-react';
import { cn } from '../lib/utils';

interface ScraperDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

interface Workspace {
  id: string;
  name: string;
}

interface Channel {
  id: string;
  name: string;
  status?: string;
}

// Default channel list — overridden by /api/channels if backend is running
const DEFAULT_CHANNELS: Channel[] = [
  { id: 'google_youtube', name: 'Google Ads (Google + YouTube)' },
  { id: 'meta', name: 'Meta (Facebook + Instagram)' },
  { id: 'microsoft', name: 'Microsoft / Bing' },
  { id: 'pinterest', name: 'Pinterest' },
  { id: 'snapchat', name: 'Snapchat' },
  { id: 'tiktok', name: 'TikTok' },
  { id: 'twitter_x', name: 'Twitter / X' },
  { id: 'amazon', name: 'Amazon Ads' },
  { id: 'reddit', name: 'Reddit Ads' },
];

export const ScraperDialog: React.FC<ScraperDialogProps> = ({ isOpen, onClose }) => {
  const { processFile } = useData();
  const [loading, setLoading] = useState(false);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [availableChannels, setAvailableChannels] = useState<Channel[]>(DEFAULT_CHANNELS);
  const [selectedWorkspaces, setSelectedWorkspaces] = useState<string[]>([]);
  const [selectedChannels, setSelectedChannels] = useState<string[]>(
    DEFAULT_CHANNELS.map(c => c.id) // all channels selected by default
  );
  const [dateRange, setDateRange] = useState({
    start: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 7 days ago
    end: new Date().toISOString().split('T')[0],
  });
  const [granularity, setGranularity] = useState<string>('daily');
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const API_BASE = import.meta.env.VITE_API_BASE_URL || '';

  useEffect(() => {
    if (isOpen) {
      setErrorMsg(null);
      fetchWorkspaces();
      // Don't fetch channels on mount anymore, fetch per workspace
      setAvailableChannels([]);
      setSelectedChannels([]);
      setSelectedWorkspaces([]);
    }
  }, [isOpen]);

  // Fetch channels whenever workspaces are selected, otherwise clear
  useEffect(() => {
    if (selectedWorkspaces.length === 1) {
      fetchWorkspaceChannels(selectedWorkspaces[0]);
    } else if (selectedWorkspaces.length > 1) {
      setAvailableChannels(DEFAULT_CHANNELS);
      setSelectedChannels(DEFAULT_CHANNELS.map(c => c.id));
    } else {
      setAvailableChannels([]);
      setSelectedChannels([]);
    }
  }, [selectedWorkspaces]);

  const fetchWorkspaces = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/workspaces`);
      if (res.ok) {
        const jsonRes = await res.json();
        const data = JSON.parse(atob(jsonRes.data));
        setWorkspaces(data);
      }
    } catch (e) {
      console.warn('Could not reach backend to fetch workspaces — is Flask running?', e);
    }
  };

  const fetchWorkspaceChannels = async (workspaceId: string) => {
    try {
      const res = await fetch(`${API_BASE}/api/workspace-channels`, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/plain',
        },
        body: btoa(JSON.stringify({ workspace_id: workspaceId })),
      });
      if (res.ok) {
        const jsonRes = await res.json();
        const data: Channel[] = JSON.parse(atob(jsonRes.data));
        setAvailableChannels(data);
        // By default, select all ACTIVE channels, or all if none specified
        const activeIds = data.filter(c => c.status === 'active' || !c.status).map(c => c.id);
        setSelectedChannels(activeIds.length > 0 ? activeIds : data.map(c => c.id));
      }
    } catch (e) {
      console.warn(`Could not fetch channels for workspace ${workspaceId}`, e);
      setAvailableChannels([]);
    }
  };

  const handleRunScraper = async () => {
    if (selectedChannels.length === 0) {
      setErrorMsg('Please select at least one channel.');
      return;
    }
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/scrape`, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/plain',
        },
        body: btoa(JSON.stringify({
          workspace_ids: selectedWorkspaces.length > 0 ? selectedWorkspaces : 'all',
          start_date: dateRange.start,
          end_date: dateRange.end,
          channels: selectedChannels,
          granularity: granularity,
        })),
      });

      if (res.ok) {
        const jsonRes = await res.json();
        const data = JSON.parse(atob(jsonRes.data));
        if (data.csv) {
          const file = new File([data.csv], data.filename || 'scraped_data.csv', {
            type: 'text/csv',
          });
          processFile(file);
          onClose();
        } else {
          setErrorMsg('Scraper ran but returned no data.');
        }
      } else {
        const err = await res.json().catch(() => ({ error: 'Unknown error' }));
        setErrorMsg(`Scraper failed: ${err.error}${err.details ? `\n${err.details}` : ''}`);
      }
    } catch (e) {
      setErrorMsg('Could not reach the backend. Make sure Flask is running: python3 app.py');
    } finally {
      setLoading(false);
    }
  };

  const toggleWorkspace = (id: string) => {
    setSelectedWorkspaces(prev =>
      prev.includes(id) ? prev.filter(w => w !== id) : [...prev, id]
    );
  };

  const toggleChannel = (id: string) => {
    setSelectedChannels(prev =>
      prev.includes(id) ? prev.filter(c => c !== id) : [...prev, id]
    );
  };

  const selectAllChannels = () => setSelectedChannels(availableChannels.map(c => c.id));
  const clearAllChannels = () => setSelectedChannels([]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="p-6 border-b border-gray-100 flex justify-between items-center sticky top-0 bg-white z-10">
          <h2 className="text-xl font-bold text-gray-800">Run Automated Scraper</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X className="w-6 h-6" />
          </button>
        </div>

        <div className="p-6 space-y-8">
          {/* Date Range */}
          <section>
            <h3 className="text-sm font-bold text-gray-900 uppercase tracking-wider mb-4 flex items-center gap-2">
              <Calendar className="w-4 h-4" /> Date Range
            </h3>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Start Date</label>
                <input
                  type="date"
                  value={dateRange.start}
                  onChange={e => setDateRange(prev => ({ ...prev, start: e.target.value }))}
                  className="w-full p-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">End Date</label>
                <input
                  type="date"
                  value={dateRange.end}
                  onChange={e => setDateRange(prev => ({ ...prev, end: e.target.value }))}
                  className="w-full p-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Granularity</label>
                <select
                  value={granularity}
                  onChange={e => setGranularity(e.target.value)}
                  className="w-full p-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 outline-none bg-white"
                >
                  <option value="daily">Daily</option>
                  <option value="monthly">Monthly</option>
                  <option value="yearly">Yearly</option>
                  <option value="overall">Overall Total</option>
                </select>
              </div>
            </div>
          </section>

          {/* Workspaces */}
          <section>
            <h3 className="text-sm font-bold text-gray-900 uppercase tracking-wider mb-4 flex items-center gap-2">
              <Database className="w-4 h-4" /> Workspaces
            </h3>
            {workspaces.length === 0 ? (
              <p className="text-xs text-amber-600 bg-amber-50 p-3 rounded-lg border border-amber-200">
                No workspaces loaded. Make sure Flask backend is running: <code className="font-mono">python3 app.py</code>
              </p>
            ) : (
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2 max-h-48 overflow-y-auto p-1">
                {workspaces.map(ws => (
                  <button
                    key={ws.id}
                    onClick={() => toggleWorkspace(ws.id)}
                    className={cn(
                      'text-left px-3 py-2 rounded-lg text-xs font-medium border transition-all truncate',
                      selectedWorkspaces.includes(ws.id)
                        ? 'bg-blue-50 border-blue-200 text-blue-700'
                        : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
                    )}
                    title={ws.name}
                  >
                    {ws.name}
                  </button>
                ))}
              </div>
            )}
            <p className="text-xs text-gray-400 mt-2">
              {selectedWorkspaces.length === 0
                ? 'All workspaces selected (default)'
                : `${selectedWorkspaces.length} selected`}
            </p>
          </section>

          {/* Channels */}
          {selectedWorkspaces.length > 0 && (
            <section>
              <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2">
                <Layers className="w-4 h-4" />
                {selectedWorkspaces.length === 1 ? 'Integrated Channels' : 'Channels'}
              </h3>
                <div className="flex gap-2">
                  <button onClick={selectAllChannels} className="text-xs text-blue-600 hover:underline">All</button>
                  <span className="text-gray-300">|</span>
                  <button onClick={clearAllChannels} className="text-xs text-blue-600 hover:underline">None</button>
                </div>
              </div>

              {availableChannels.length === 0 ? (
                <p className="text-xs text-gray-500 italic">No channels found for this workspace.</p>
              ) : (
                <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                  {availableChannels.map(ch => {
                    const isSelected = selectedChannels.includes(ch.id);

                    // Determine status badge color
                    let dotColor = "bg-gray-300";
                    let statusLabel = "";
                    if (ch.status === "active") {
                      dotColor = "bg-emerald-500";
                      statusLabel = "Active";
                    } else if (ch.status === "reconnect") {
                      dotColor = "bg-amber-500";
                      statusLabel = "Reconnect";
                    }

                    return (
                      <button
                        key={ch.id}
                        onClick={() => toggleChannel(ch.id)}
                        className={cn(
                          'p-3 rounded-lg border transition-all text-left flex flex-col gap-1.5',
                          isSelected
                            ? 'bg-blue-50 border-blue-200 shadow-sm'
                            : 'bg-white border-gray-200 hover:bg-gray-50'
                        )}
                      >
                        <div className="flex items-center justify-between w-full">
                          <span className={cn("text-sm font-semibold truncate", isSelected ? "text-blue-800" : "text-gray-700")}>
                            {ch.name}
                          </span>
                          <div className={cn(
                            "w-4 h-4 rounded-full border-2 flex-shrink-0 flex items-center justify-center transition-colors",
                            isSelected ? "border-blue-500 bg-blue-500" : "border-gray-300"
                          )}>
                            {isSelected && <div className="w-1.5 h-1.5 bg-white rounded-full" />}
                          </div>
                        </div>

                        {statusLabel && (
                          <div className="flex items-center gap-1.5">
                            <span className={cn("w-2 h-2 rounded-full", dotColor)} />
                            <span className="text-[10px] uppercase font-bold text-gray-500 tracking-wider">
                              {statusLabel}
                            </span>
                          </div>
                        )}
                      </button>
                    );
                  })}
                </div>
              )}
                <p className="text-xs text-gray-400 mt-2">{selectedChannels.length} / {availableChannels.length} selected</p>
              </section>
            )}

          {/* Error */}
          {errorMsg && (
            <div className="bg-red-50 border border-red-200 text-red-700 text-xs p-3 rounded-lg whitespace-pre-wrap">
              {errorMsg}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="p-6 border-t border-gray-100 bg-gray-50 flex justify-end gap-3 sticky bottom-0">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-600 hover:text-gray-800"
            disabled={loading}
          >
            Cancel
          </button>
          <button
            onClick={handleRunScraper}
            disabled={loading || selectedChannels.length === 0}
            className="px-6 py-2 bg-blue-600 text-white text-sm font-bold rounded-lg hover:bg-blue-700 disabled:opacity-70 flex items-center gap-2"
          >
            {loading && <Loader2 className="w-4 h-4 animate-spin" />}
            {loading ? 'Running Scraper...' : 'Run Scraper'}
          </button>
        </div>
      </div>
    </div>
  );
};
