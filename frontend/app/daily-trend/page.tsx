import { fetchDailyTrend } from '@/lib/api';
import { DailyTrendChart } from '@/components/charts/daily-trend-chart';
import { Suspense } from 'react';

// Loading component
function ChartSkeleton() {
  return (
    <div className="w-full h-96 bg-muted/20 animate-pulse rounded-lg flex items-center justify-center">
      <p className="text-muted-foreground">Loading...</p>
    </div>
  );
}

export default async function DailyTrendPage() {
  const dailyData = await fetchDailyTrend().catch(() => []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Daily Trends</h1>
        <p className="text-muted-foreground">
          Intraday price movements and volume analysis
        </p>
      </div>
      
      <Suspense fallback={<ChartSkeleton />}>
        {dailyData.length > 0 ? (
          <DailyTrendChart data={dailyData.slice(0, 100)} />
        ) : (
          <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
            No daily trend data available
          </div>
        )}
      </Suspense>
    </div>
  );
}
