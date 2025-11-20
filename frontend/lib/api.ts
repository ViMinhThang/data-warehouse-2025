import { DailyTrend, MonthlyTrend, StockRanking, MarketLiquidity } from './db';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000';

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  count: number;
  error?: string;
  message?: string;
}

export async function fetchDailyTrend(ticker?: string): Promise<DailyTrend[]> {
  const url = new URL(`${API_BASE_URL}/api/daily-trend`);
  if (ticker) {
    url.searchParams.append('ticker', ticker);
  }

  const res = await fetch(url.toString(), {
    cache: 'no-store',
  });
  
  if (!res.ok) {
    throw new Error('Failed to fetch daily trend');
  }
  
  const json = await res.json();
  return json.data;
}
export async function fetchMonthlyTrend(): Promise<MonthlyTrend[]> {
  const res = await fetch(`${API_BASE_URL}/api/monthly-trend`, {
    cache: 'no-store',
  });
  
  if (!res.ok) {
    throw new Error('Failed to fetch monthly trend');
  }
  
  const json = await res.json();
  return json.data;
}

export async function fetchStockRanking(): Promise<StockRanking[]> {
  const res = await fetch(`${API_BASE_URL}/api/stock-ranking`, {
    cache: 'no-store',
  });
  
  if (!res.ok) {
    throw new Error('Failed to fetch stock ranking');
  }
  
  const json = await res.json();
  return json.data;
}

export async function fetchMarketLiquidity(): Promise<MarketLiquidity[]> {
  const res = await fetch(`${API_BASE_URL}/api/market-liquidity`, {
    cache: 'no-store',
  });
  
  if (!res.ok) {
    throw new Error('Failed to fetch market liquidity');
  }
  
  const json = await res.json();
  return json.data;
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
