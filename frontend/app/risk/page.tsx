"use client";

import React, { useEffect, useState } from "react";
import RiskDonutChart from "../../components/charts/RiskDonutChart";
import RankingTable from "../../components/Tables/RankingTable";
import { StockRankingSnapshot } from "../../types";
import { SimpleCard } from "../../components/ui/Card";

export default function RiskPageClient() {
  const [data, setData] = useState<StockRankingSnapshot[]>([]);

  useEffect(() => {
    fetch("/api/ranking")
      .then((res) => res.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data.length) return <p>Loading...</p>;

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div>
        <h2 className="text-3xl font-bold tracking-tight text-slate-900">
          Risk & Rankings
        </h2>
        <p className="text-slate-500 mt-1">
          Portfolio segmentation derived from{" "}
          <code className="text-xs bg-slate-100 px-1 rounded text-slate-700">
            stock_ranking_snapshot
          </code>
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-1">
          <SimpleCard
            title="Risk Distribution"
            subtitle="Assets by Volatility Class"
          >
            <RiskDonutChart data={data} />
          </SimpleCard>
        </div>
        <div className="lg:col-span-2">
          <SimpleCard
            title="Top Volatility Movers"
            subtitle="Highest Standard Deviation (σ)"
          >
            <div className="space-y-3">
              {data
                .sort((a, b) => b.avg_volatility - a.avg_volatility)
                .slice(0, 3)
                .map((stock, idx) => (
                  <div
                    key={stock.ticker}
                    className="flex items-center justify-between p-4 bg-slate-50 rounded-lg border border-slate-100"
                  >
                    <div className="flex items-center space-x-4">
                      <div
                        className={`w-8 h-8 flex items-center justify-center rounded-full font-bold text-sm ${
                          idx === 0
                            ? "bg-blue-600 text-white"
                            : "bg-blue-100 text-blue-700"
                        }`}
                      >
                        {idx + 1}
                      </div>
                      <div>
                        <span className="block font-bold text-slate-800">
                          {stock.ticker}
                        </span>
                        <span className="text-xs text-slate-500">
                          {stock.risk_category}
                        </span>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-mono font-bold text-slate-900">
                        {stock.avg_volatility.toFixed(2)} σ
                      </div>
                      <div className="text-xs text-slate-500">Volatility</div>
                    </div>
                  </div>
                ))}
            </div>
          </SimpleCard>
        </div>
      </div>

      <SimpleCard
        title="Comprehensive Stock Ranking"
        subtitle="Performance & Risk Metrics"
      >
        <RankingTable data={data} />
      </SimpleCard>
    </div>
  );
}
