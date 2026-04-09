import React from 'react';
import { useData } from '../contexts/DataContext';
import { SortOption } from '../types';
import { MultiSelect } from './MultiSelect';

export const FilterBar: React.FC = () => {
  const { workspaces, channels, filters, setFilters } = useData();

  const handleWorkspaceChange = (selected: string[]) => {
    setFilters(prev => ({ ...prev, workspaces: selected }));
  };

  const handleChannelChange = (selected: string[]) => {
    setFilters(prev => ({ ...prev, channels: selected }));
  };

  const handleSortChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setFilters(prev => ({ ...prev, sort: e.target.value as SortOption }));
  };

  const handleDateChange = (field: 'startDate' | 'endDate') => (e: React.ChangeEvent<HTMLInputElement>) => {
    setFilters(prev => ({ ...prev, [field]: e.target.value }));
  };

  const clearFilters = () => {
    setFilters({
      workspaces: [],
      startDate: '',
      endDate: '',
      sort: 'date-desc',
      channels: []
    });
  };

  return (
    <div className="px-8 py-3 bg-white border-b border-gray-100 flex flex-wrap gap-4 items-center shadow-sm sticky top-[80px] z-10">
      {/* Workspace Filter (Multi) */}
      <MultiSelect 
        label="Workspace" 
        options={workspaces} 
        selected={filters.workspaces} 
        onChange={handleWorkspaceChange} 
        placeholder="All Workspaces"
      />

      {/* Channel Filter (Multi) */}
      <MultiSelect 
        label="Channel" 
        options={channels} 
        selected={filters.channels} 
        onChange={handleChannelChange} 
        placeholder="All Channels"
      />

      {/* Date Filter */}
      <div className="flex items-center gap-2 bg-gray-50 p-1 rounded-md border border-gray-200">
        <span className="text-xs font-bold text-gray-500 uppercase px-2">Date:</span>
        <input 
          type="date" 
          value={filters.startDate}
          onChange={handleDateChange('startDate')}
          className="text-sm bg-transparent border-0 focus:ring-0 text-gray-700 p-1 outline-none"
        />
        <span className="text-gray-400">-</span>
        <input 
          type="date" 
          value={filters.endDate}
          onChange={handleDateChange('endDate')}
          className="text-sm bg-transparent border-0 focus:ring-0 text-gray-700 p-1 outline-none"
        />
      </div>
      
      {/* Sort */}
      <div className="flex items-center gap-2 bg-gray-50 p-1 rounded-md border border-gray-200 ml-auto">
        <span className="text-xs font-bold text-gray-500 uppercase px-2">Sort:</span>
        <select 
          value={filters.sort}
          onChange={handleSortChange}
          className="text-sm bg-transparent border-0 focus:ring-0 text-gray-700 p-1 pr-6 cursor-pointer outline-none"
        >
          <option value="date-desc">Date: Newest</option>
          <option value="date-asc">Date: Oldest</option>
          <option value="diff-desc">Diff %: High to Low</option>
          <option value="diff-asc">Diff %: Low to High</option>
          <option value="spend-desc">Spend: High to Low</option>
          <option value="spend-asc">Spend: Low to High</option>
        </select>
      </div>
      
      <button 
        onClick={clearFilters} 
        className="text-gray-400 hover:text-gray-600 text-xs font-bold px-2 py-2 transition"
      >
        Reset
      </button>
    </div>
  );
};
