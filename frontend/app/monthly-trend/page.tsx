import { fetchMonthlyTrend } from '@/lib/api';
import { MonthlyTrendChart } from '@/components/charts/monthly-trend-chart';
import { Suspense } from 'react';

// Loading component
function ChartSkeleton() {
  return (
    <div className="w-full h-96 bg-muted/20 animate-pulse rounded-lg flex items-center justify-center">
      <p className="text-muted-foreground">Loading...</p>
    </div>
  );
}

export default async function MonthlyTrendPage() {
  const monthlyData = await fetchMonthlyTrend({ year: 2024 }).catch(() => []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Monthly Performance</h1>
        <p className="text-muted-foreground">
          Month-over-month comparison across stocks
        </p>
      </div>
      
      <Suspense fallback={<ChartSkeleton />}>
        {monthlyData.length > 0 ? (
          <MonthlyTrendChart data={monthlyData} />
        ) : (
          <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
            No monthly trend data available
          </div>
        )}
      </Suspense>
    </div>
  );
}
