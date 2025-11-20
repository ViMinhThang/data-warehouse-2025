import { Pool, QueryResult } from 'pg';

// Database connection configuration
const pool = new Pool({
  host: 'localhost',
  port: 5432,
  database: 'dm',
  user: 'fragile',
  password: '123456',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Type definitions for our data models
export interface DailyTrend {
  ticker: string;
  full_date: string;
  avg_close: number;
  max_close: number;
  min_close: number;
  total_volume: number;
  avg_rsi: number;
  avg_roc: number;
  created_at: string;
}

export interface MonthlyTrend {
  ticker: string;
  year: number;
  month: number;
  avg_close: number;
  total_volume: number;
  avg_rsi: number;
  avg_roc: number;
  created_at: string;
}

export interface StockRanking {
  ticker: string;
  snapshot_date: string;
  min_close: number;
  max_close: number;
  price_change: number;
  avg_rsi: number;
  avg_roc_performance: number;
  avg_volatility: number;
  risk_category: string;
  created_at: string;
}

// Query helper function
export async function query<T>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const start = Date.now();
  try {
    const res = await pool.query<T>(text, params);
    const duration = Date.now() - start;
    console.log('Executed query', { text, duration, rows: res.rowCount });
    return res;
  } catch (error) {
    console.error('Database query error:', error);
    throw error;
  }
}

// Get a client from the pool for transactions
export async function getClient() {
  return await pool.connect();
}

// Close the pool (useful for cleanup)
export async function closePool() {
  await pool.end();
}

export default pool;
