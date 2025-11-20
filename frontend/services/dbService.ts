import { query } from "@/lib/db";
import {
  StockDailyTrend,
  StockRankingSnapshot,
  MarketLiquidityHistory,
} from "../types";

// Fetch daily trends
export const fetchDailyTrends = async (): Promise<StockDailyTrend[]> => {
  const result = await query(
    `SELECT *
     FROM dm_dw.stock_daily_trend
     ORDER BY full_date DESC, ticker
     LIMIT 500`
  );
  return result.rows;
};

export const fetchMonthlyTrends = async (): Promise<StockDailyTrend[]> => {
  const result = await query(
    `SELECT *
     FROM dm_dw.stock_monthly_trend
     ORDER BY year DESC, month DESC, ticker
     LIMIT 500`
  );
  return result.rows;
};

// Fetch  ranking snapshot
export const fetchRankingSnapshot = async (): Promise<
  StockRankingSnapshot[]
> => {
  const result = await query(
    `SELECT *
     FROM dm_dw.stock_ranking_snapshot
     ORDER BY avg_volatility DESC
     LIMIT 500`
  );
  return result.rows;
};

// Fetch market liquidity
export const fetchMarketLiquidity = async (): Promise<
  MarketLiquidityHistory[]
> => {
  const result = await query(
    `SELECT *
     FROM dm_dw.market_liquidity_history
     ORDER BY full_date DESC
     LIMIT 500`
  );
  return result.rows;
};
