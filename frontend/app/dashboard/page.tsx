import { fetchDailyTrend, fetchMonthlyTrend, fetchStockRanking, fetchTickers } from '@/lib/api';
import { DailyTrendChart } from '@/components/charts/daily-trend-chart';
import { MonthlyTrendChart } from '@/components/charts/monthly-trend-chart';
import { StockRankingTable } from '@/components/charts/stock-ranking-table';
import { Suspense } from 'react';

// Loading component
function ChartSkeleton() {
  return (
    <div className="w-full h-96 bg-muted/20 animate-pulse rounded-lg flex items-center justify-center">
      <p className="text-muted-foreground">Loading...</p>
    </div>
  );
}

export default async function DashboardPage() {
  // Fetch all data in parallel
  const [dailyData, monthlyData, rankingData, tickers] = await Promise.all([
    fetchDailyTrend({ ticker: undefined }).catch(() => []),
    fetchMonthlyTrend({ year: 2024 }).catch(() => []),
    fetchStockRanking({ limit: 50 }).catch(() => []),
    fetchTickers().catch(() => []),
  ]);

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b border-border bg-card/50 backdrop-blur-sm sticky top-0 z-10">
        <div className="container mx-auto px-4 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
                Stock Analytics Dashboard
              </h1>
              <p className="text-muted-foreground mt-1">
                Real-time data warehouse insights
              </p>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-sm text-muted-foreground">
                {tickers.length} stocks tracked
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-8">
        <div className="space-y-8">
          {/* Daily Trend Section */}
          <section>
            <div className="mb-4">
              <h2 className="text-2xl font-semibold">Daily Trends</h2>
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
          </section>

          {/* Monthly Trend Section */}
          <section>
            <div className="mb-4">
              <h2 className="text-2xl font-semibold">Monthly Performance</h2>
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
          </section>

          {/* Stock Rankings Section */}
          <section>
            <div className="mb-4">
              <h2 className="text-2xl font-semibold">Stock Rankings</h2>
              <p className="text-muted-foreground">
                Performance metrics and risk categorization
              </p>
            </div>
            <Suspense fallback={<ChartSkeleton />}>
              {rankingData.length > 0 ? (
                <StockRankingTable data={rankingData} />
              ) : (
                <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
                  No ranking data available
                </div>
              )}
            </Suspense>
          </section>
        </div>
      </main>

      {/* Footer */}
      <footer className="border-t border-border mt-16 py-8">
        <div className="container mx-auto px-4 text-center text-sm text-muted-foreground">
          <p>Data Warehouse Analytics Dashboard • Built with Next.js & PostgreSQL</p>
        </div>
      </footer>
    </div>
  );
}
