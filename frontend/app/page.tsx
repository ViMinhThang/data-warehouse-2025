import React, { useEffect, useState } from "react";
import { SimpleCard } from "../components/ui/Card";
import LiquidityChart from "../components/charts/LiquidityChart";
import { MarketLiquidityHistory } from "../types";

export default function OverviewPage() {
  const [data, setData] = useState<MarketLiquidityHistory[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetch("/api/market-liquidity");
        const json = await res.json();
        setData(json);
      } catch (err) {
        console.error(err);
      }
    };

    fetchData();
  }, []);

  const latest = data[data.length - 1] || {
    total_market_volume: 0,
    stocks_traded_count: 0,
  };

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      {/* Header */}
      <div>
        <h2 className="text-3xl font-bold tracking-tight text-slate-900">
          Market Overview
        </h2>
        <p className="text-slate-500 mt-1">
          Real-time liquidity and volume analysis from the{" "}
          <code className="bg-slate-100 px-1 py-0.5 rounded text-blue-600 text-xs">
            dm_dw
          </code>{" "}
          warehouse.
        </p>
      </div>

      {/* Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Total Market Volume */}
        <SimpleCard title="Total Market Volume">
          {(latest.total_market_volume / 1_000_000).toFixed(1)}M
        </SimpleCard>
        {/* Stocks Traded */}
        <SimpleCard title="Stocks Traded">
          {latest.stocks_traded_count.toLocaleString()}
        </SimpleCard>
        {/* Market Sentiment */}
        <SimpleCard title="Market Sentiment">Bullish</SimpleCard>
      </div>

      {/* Chart */}
      <SimpleCard
        title="Liquidity Trend Analysis"
        subtitle="7-Day Moving Average vs Daily Volume"
      >
        <LiquidityChart data={data} />
      </SimpleCard>
    </div>
  );
}
