import { fetchDailyTrend, fetchTickers } from '@/lib/api'; // Ensure fetchTickers is exported from api.ts
import { DailyTrendChart } from '@/components/charts/daily-trend-chart';
import { TickerSelector } from '@/components/dashboard/ticker-selector';
import { Suspense } from 'react';

function ChartSkeleton() {
  return (
    <div className="w-full h-96 bg-muted/20 animate-pulse rounded-lg flex items-center justify-center">
      <p className="text-muted-foreground">Loading Chart...</p>
    </div>
  );
}

export default async function DailyTrendPage({
  searchParams,
}: {
  searchParams: Promise<{ ticker?: string }>;
}) {
  const params = await searchParams;
  const ticker = params?.ticker;

  // Parallel data fetching
  const [dailyData, tickers] = await Promise.all([
    fetchDailyTrend(ticker).catch(() => []),
    fetchTickers().catch(() => []),
  ]);

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
        <div>
          <h1 className="text-3xl font-bold">Daily Trends</h1>
          <p className="text-muted-foreground">
            {ticker 
              ? `Analysis for ${ticker}` 
              : "Intraday price movements and volume analysis"}
          </p>
        </div>
        <TickerSelector tickers={tickers} />
      </div>
      
      <Suspense fallback={<ChartSkeleton />}>
        {dailyData.length > 0 ? (
          <DailyTrendChart data={dailyData} ticker={ticker} />
        ) : (
          <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
            No daily trend data available for this selection.
          </div>
        )}
      </Suspense>
    </div>
  );
}