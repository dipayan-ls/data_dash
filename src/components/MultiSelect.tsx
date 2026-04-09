import React, { useState, useRef, useEffect } from 'react';
import { ChevronDown, X } from 'lucide-react';
import { cn } from '../lib/utils';

interface MultiSelectProps {
  label: string;
  options: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
  placeholder?: string;
}

export const MultiSelect: React.FC<MultiSelectProps> = ({ 
  label, 
  options, 
  selected, 
  onChange,
  placeholder = "Select..." 
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const toggleOption = (option: string) => {
    if (selected.includes(option)) {
      onChange(selected.filter(item => item !== option));
    } else {
      onChange([...selected, option]);
    }
  };

  const clearSelection = (e: React.MouseEvent) => {
    e.stopPropagation();
    onChange([]);
  };

  return (
    <div className="flex items-center gap-2 bg-gray-50 p-1 rounded-md border border-gray-200 relative" ref={containerRef}>
      <span className="text-xs font-bold text-gray-500 uppercase px-2 whitespace-nowrap">{label}:</span>
      
      <div 
        className="relative min-w-[180px]"
        onClick={() => setIsOpen(!isOpen)}
      >
        <div className="flex items-center justify-between cursor-pointer px-2 py-1">
          <span className="text-sm text-gray-700 truncate max-w-[140px]">
            {selected.length === 0 
              ? placeholder 
              : selected.length === 1 
                ? selected[0] 
                : `${selected.length} selected`}
          </span>
          <div className="flex items-center gap-1">
            {selected.length > 0 && (
              <button 
                onClick={clearSelection}
                className="p-0.5 hover:bg-gray-200 rounded-full text-gray-400 hover:text-gray-600"
              >
                <X className="w-3 h-3" />
              </button>
            )}
            <ChevronDown className={cn("w-4 h-4 text-gray-400 transition-transform", isOpen && "rotate-180")} />
          </div>
        </div>

        {isOpen && (
          <div className="absolute top-full left-0 mt-2 w-full min-w-[200px] bg-white border border-gray-100 rounded-lg shadow-lg z-50 max-h-60 overflow-y-auto py-1">
            {options.length === 0 ? (
              <div className="px-4 py-2 text-sm text-gray-400 italic">No options available</div>
            ) : (
              options.map(option => (
                <div 
                  key={option}
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleOption(option);
                  }}
                  className="flex items-center px-4 py-2 hover:bg-gray-50 cursor-pointer"
                >
                  <div className={cn(
                    "w-4 h-4 border rounded mr-3 flex items-center justify-center transition-colors",
                    selected.includes(option) ? "bg-blue-600 border-blue-600" : "border-gray-300"
                  )}>
                    {selected.includes(option) && <div className="w-2 h-2 bg-white rounded-sm" />}
                  </div>
                  <span className={cn(
                    "text-sm",
                    selected.includes(option) ? "text-gray-900 font-medium" : "text-gray-600"
                  )}>
                    {option}
                  </span>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
};
