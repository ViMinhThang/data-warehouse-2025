import { fetchStockRanking } from '@/lib/api';
import { StockRankingTable } from '@/components/charts/stock-ranking-table';
import { Suspense } from 'react';

// Loading component
function TableSkeleton() {
  return (
    <div className="w-full h-96 bg-muted/20 animate-pulse rounded-lg flex items-center justify-center">
      <p className="text-muted-foreground">Loading...</p>
    </div>
  );
}

export default async function StockRankingPage() {
  const rankingData = await fetchStockRanking().catch(() => []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Stock Rankings</h1>
        <p className="text-muted-foreground">
          Performance metrics and risk categorization
        </p>
      </div>
      
      <Suspense fallback={<TableSkeleton />}>
        {rankingData.length > 0 ? (
          <StockRankingTable data={rankingData} />
        ) : (
          <div className="p-8 text-center text-muted-foreground border border-border rounded-lg">
            No ranking data available
          </div>
        )}
      </Suspense>
    </div>
  );
}
