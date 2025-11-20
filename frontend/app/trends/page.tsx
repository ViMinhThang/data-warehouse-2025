import React, { useEffect, useState } from "react";
import { SimpleCard } from "../../components/ui/Card";
import StockTrendChart from "../../components/charts/StockTrendChart";
import { StockDailyTrend } from "../../types";

export default function TrendsPage() {
  const [data, setData] = useState<StockDailyTrend[]>([]);
  const [selectedTicker, setSelectedTicker] = useState("AAPL");

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetch(`/api/daily-trends?ticker=${selectedTicker}`);
        const json = await res.json();
        setData(json);
      } catch (err) {
        console.error(err);
      }
    };

    fetchData();
  }, [selectedTicker]);

  const tickers = Array.from(new Set(data.map((d) => d.ticker)));
  const currentStockData = data.filter((d) => d.ticker === selectedTicker);
  const latestData = currentStockData[currentStockData.length - 1];

  return (
    <div className="space-y-8">
      <div className="flex items-center gap-3">
        <label>Ticker:</label>
        <select
          value={selectedTicker}
          onChange={(e) => setSelectedTicker(e.target.value)}
        >
          {tickers.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      {latestData && (
        <SimpleCard
          title={`${selectedTicker} Price Action`}
          subtitle="Daily Close with High/Low"
        >
          <StockTrendChart data={currentStockData} ticker={selectedTicker} />
        </SimpleCard>
      )}
    </div>
  );
}
