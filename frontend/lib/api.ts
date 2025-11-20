import { DailyTrend, MonthlyTrend, StockRanking } from './db';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000';

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  count: number;
  error?: string;
  message?: string;
}

export async function fetchDailyTrend(params?: {
  ticker?: string;
  start_date?: string;
  end_date?: string;
}): Promise<DailyTrend[]> {
  const searchParams = new URLSearchParams();
  if (params?.ticker) searchParams.set('ticker', params.ticker);
  if (params?.start_date) searchParams.set('start_date', params.start_date);
  if (params?.end_date) searchParams.set('end_date', params.end_date);

  const url = `${API_BASE_URL}/api/daily-trend${searchParams.toString() ? `?${searchParams}` : ''}`;
  
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Failed to fetch daily trend: ${response.statusText}`);
  }
  
  const result: ApiResponse<DailyTrend[]> = await response.json();
  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch daily trend');
  }
  
  return result.data;
}

export async function fetchMonthlyTrend(params?: {
  ticker?: string;
  year?: number;
}): Promise<MonthlyTrend[]> {
  const searchParams = new URLSearchParams();
  if (params?.ticker) searchParams.set('ticker', params.ticker);
  if (params?.year) searchParams.set('year', params.year.toString());

  const url = `${API_BASE_URL}/api/monthly-trend${searchParams.toString() ? `?${searchParams}` : ''}`;
  
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Failed to fetch monthly trend: ${response.statusText}`);
  }
  
  const result: ApiResponse<MonthlyTrend[]> = await response.json();
  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch monthly trend');
  }
  
  return result.data;
}

export async function fetchStockRanking(params?: {
  limit?: number;
  sort_by?: string;
  order?: 'ASC' | 'DESC';
}): Promise<StockRanking[]> {
  const searchParams = new URLSearchParams();
  if (params?.limit) searchParams.set('limit', params.limit.toString());
  if (params?.sort_by) searchParams.set('sort_by', params.sort_by);
  if (params?.order) searchParams.set('order', params.order);

  const url = `${API_BASE_URL}/api/stock-ranking${searchParams.toString() ? `?${searchParams}` : ''}`;
  
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Failed to fetch stock ranking: ${response.statusText}`);
  }
  
  const result: ApiResponse<StockRanking[]> = await response.json();
  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch stock ranking');
  }
  
  return result.data;
}

export async function fetchTickers(): Promise<string[]> {
  const url = `${API_BASE_URL}/api/tickers`;
  
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Failed to fetch tickers: ${response.statusText}`);
  }
  
  const result: ApiResponse<string[]> = await response.json();
  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch tickers');
  }
  
  return result.data;
}
