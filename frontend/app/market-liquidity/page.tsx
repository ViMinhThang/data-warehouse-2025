import { Suspense } from 'react';
import { fetchMarketLiquidity } from '@/lib/api';
import { MarketLiquidityChart } from '@/components/charts/market-liquidity-chart';
import { Skeleton } from '@/components/ui/skeleton';

export default async function MarketLiquidityPage() {
  const data = await fetchMarketLiquidity().catch(() => []);

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
          Market Liquidity
        </h1>
        <p className="text-muted-foreground mt-2">
          Analysis of total market volume and trading activity
        </p>
      </div>

      <Suspense fallback={<ChartSkeleton />}>
        {data.length > 0 ? (
          <MarketLiquidityChart data={data} />
        ) : (
          <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
            No market liquidity data available
          </div>
        )}
      </Suspense>
    </div>
  );
}

function ChartSkeleton() {
  return (
    <div className="space-y-8">
      <div className="w-full h-[400px] rounded-xl border border-border bg-card/50 p-6">
        <div className="space-y-2">
          <Skeleton className="h-6 w-[200px]" />
          <Skeleton className="h-4 w-[300px]" />
        </div>
        <div className="mt-8 h-[300px] flex items-end gap-2">
          {Array.from({ length: 12 }).map((_, i) => (
            <Skeleton key={i} className="h-full w-full" style={{ height: `${Math.random() * 100}%` }} />
          ))}
        </div>
      </div>
    </div>
  );
}
