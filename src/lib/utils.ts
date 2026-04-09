import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export const formatCurrency = (val: number) =>
  new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val);

export const formatNumber = (val: number) =>
  new Intl.NumberFormat('en-US').format(val);

export const getChannelGroup = (rawSource: string): string => {
  const s = rawSource.toLowerCase().replace(/[^a-z0-9]/g, '');

  if (s.includes('youtube') || s.includes('yt')) return 'YouTube';
  if (s.includes('google')) return 'Google';
  if (s.includes('instagram') || s.includes('ig')) return 'Instagram';
  if (s.includes('facebook') || s.includes('fb')) return 'Facebook';
  if (s.includes('microsoft') || s.includes('bing')) return 'Microsoft';
  if (s.includes('tiktok') || s.includes('tt')) return 'TikTok';
  if (s.includes('snapchat') || s.includes('snap')) return 'Snapchat';
  if (s.includes('twitter') || s.includes('x')) return 'X (Twitter)';
  if (s.includes('linkedin') || s.includes('li')) return 'LinkedIn';
  if (s.includes('pinterest') || s.includes('pin')) return 'Pinterest';
  if (s.includes('amazon')) return 'Amazon Ads';
  if (s.includes('criteo')) return 'Criteo';
  if (s.includes('stackadapt')) return 'StackAdapt';
  if (s.includes('cj') || s.includes('cj_affiliate')) return 'CJ Affiliate';
  if (s.includes('ga4') || s.includes('analytics')) return 'GA4';
  if (s.includes('reddit')) return 'Reddit';

  return rawSource.charAt(0).toUpperCase() + rawSource.slice(1);
};
