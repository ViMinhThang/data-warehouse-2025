// Matching table: stock_daily_trend
export interface StockDailyTrend {
    ticker: string;
    full_date: string; // ISO Date string
    avg_close: number;
    max_close: number;
    min_close: number;
    total_volume: number;
    avg_rsi: number;
    avg_roc: number;
}

// Matching table: stock_ranking_snapshot
export interface StockRankingSnapshot {
    ticker: string;
    min_close: number;
    max_close: number;
    price_change: number;
    avg_rsi: number;
    avg_roc_performance: number;
    avg_volatility: number;
    risk_category: 'High Risk' | 'Medium Risk' | 'Low Risk';
}

// Matching table: market_liquidity_history
export interface MarketLiquidityHistory {
    full_date: string;
    total_market_volume: number;
    stocks_traded_count: number;
    volume_moving_avg_7d: number;
}
