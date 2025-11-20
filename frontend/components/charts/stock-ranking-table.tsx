'use client';

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { StockRanking } from '@/lib/db';
import { ArrowUp, ArrowDown } from 'lucide-react';
import { useState } from 'react';

interface StockRankingTableProps {
  data: StockRanking[];
}

type SortField = 'ticker' | 'price_change' | 'avg_rsi' | 'avg_roc_performance' | 'avg_volatility';
type SortOrder = 'asc' | 'desc';

export function StockRankingTable({ data }: StockRankingTableProps) {
  const [sortField, setSortField] = useState<SortField>('price_change');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('desc');
    }
  };

  const sortedData = [...data].sort((a, b) => {
    const aValue = Number(a[sortField]);
    const bValue = Number(b[sortField]);
    
    if (sortField === 'ticker') {
      return sortOrder === 'asc' 
        ? a.ticker.localeCompare(b.ticker)
        : b.ticker.localeCompare(a.ticker);
    }
    
    return sortOrder === 'asc' ? aValue - bValue : bValue - aValue;
  });

  const getRiskBadgeVariant = (risk: string): "default" | "secondary" | "destructive" | "outline" => {
    switch (risk) {
      case 'High Risk':
        return 'destructive';
      case 'Medium Risk':
        return 'secondary';
      case 'Low Risk':
        return 'outline';
      default:
        return 'default';
    }
  };

  const formatNumber = (num: number, decimals: number = 2) => {
    return num.toLocaleString('en-US', { 
      minimumFractionDigits: decimals, 
      maximumFractionDigits: decimals 
    });
  };

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null;
    return sortOrder === 'asc' ? (
      <ArrowUp className="inline h-4 w-4 ml-1" />
    ) : (
      <ArrowDown className="inline h-4 w-4 ml-1" />
    );
  };

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle>Stock Performance Rankings</CardTitle>
        <CardDescription>
          Ranked by performance metrics with risk categorization
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead 
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => handleSort('ticker')}
                >
                  Ticker <SortIcon field="ticker" />
                </TableHead>
                <TableHead 
                  className="cursor-pointer hover:bg-muted/50 text-right"
                  onClick={() => handleSort('price_change')}
                >
                  Price Change <SortIcon field="price_change" />
                </TableHead>
                <TableHead className="text-right">Min Close</TableHead>
                <TableHead className="text-right">Max Close</TableHead>
                <TableHead 
                  className="cursor-pointer hover:bg-muted/50 text-right"
                  onClick={() => handleSort('avg_rsi')}
                >
                  Avg RSI <SortIcon field="avg_rsi" />
                </TableHead>
                <TableHead 
                  className="cursor-pointer hover:bg-muted/50 text-right"
                  onClick={() => handleSort('avg_roc_performance')}
                >
                  Avg ROC <SortIcon field="avg_roc_performance" />
                </TableHead>
                <TableHead 
                  className="cursor-pointer hover:bg-muted/50 text-right"
                  onClick={() => handleSort('avg_volatility')}
                >
                  Volatility <SortIcon field="avg_volatility" />
                </TableHead>
                <TableHead>Risk Category</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {sortedData.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-muted-foreground py-8">
                    No data available
                  </TableCell>
                </TableRow>
              ) : (
                sortedData.map((stock) => (
                  <TableRow key={`${stock.ticker}-${stock.snapshot_date}`}>
                    <TableCell className="font-medium">{stock.ticker}</TableCell>
                    <TableCell className="text-right">
                      <span className={Number(stock.price_change) >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}>
                        {formatNumber(Number(stock.price_change))}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">{formatNumber(Number(stock.min_close))}</TableCell>
                    <TableCell className="text-right">{formatNumber(Number(stock.max_close))}</TableCell>
                    <TableCell className="text-right">{formatNumber(Number(stock.avg_rsi))}</TableCell>
                    <TableCell className="text-right">
                      <span className={Number(stock.avg_roc_performance) >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}>
                        {formatNumber(Number(stock.avg_roc_performance))}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">{formatNumber(Number(stock.avg_volatility))}</TableCell>
                    <TableCell>
                      <Badge variant={getRiskBadgeVariant(stock.risk_category)}>
                        {stock.risk_category}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
        <div className="mt-4 text-sm text-muted-foreground">
          Showing {sortedData.length} stock{sortedData.length !== 1 ? 's' : ''}
        </div>
      </CardContent>
    </Card>
  );
}
