import { fetchMonthlyTrend, fetchTickers } from '@/lib/api';
import { MonthlyTrendChart } from '@/components/charts/monthly-trend-chart';
import { TickerSelector } from '@/components/dashboard/ticker-selector'; // Reuse the component we created
import { Suspense } from 'react';

function ChartSkeleton() {
  return (
    <div className="w-full h-96 bg-muted/20 animate-pulse rounded-lg flex items-center justify-center">
      <p className="text-muted-foreground">Loading Monthly Data...</p>
    </div>
  );
}

export default async function MonthlyTrendPage({
  searchParams,
}: {
  searchParams: Promise<{ ticker?: string }>;
}) {
  const params = await searchParams;
  const ticker = params?.ticker;

  const [monthlyData, tickers] = await Promise.all([
    fetchMonthlyTrend(ticker).catch(() => []),
    fetchTickers().catch(() => []),
  ]);

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
        <div>
          <h1 className="text-3xl font-bold">Monthly Performance</h1>
          <p className="text-muted-foreground">
            {ticker 
              ? `Month-over-month comparison for ${ticker}`
              : "Month-over-month comparison across stocks"}
          </p>
        </div>
        <TickerSelector tickers={tickers} />
      </div>
      
      <Suspense fallback={<ChartSkeleton />}>
        {monthlyData.length > 0 ? (
          <MonthlyTrendChart data={monthlyData} ticker={ticker} />
        ) : (
          <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
            No monthly trend data available for this selection.
          </div>
        )}
      </Suspense>
    </div>
  );
}