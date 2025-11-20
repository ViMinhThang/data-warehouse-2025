'use client';

import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { MonthlyTrend } from '@/lib/db';

interface MonthlyTrendChartProps {
  data: MonthlyTrend[];
  ticker?: string;
}

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export function MonthlyTrendChart({ data, ticker }: MonthlyTrendChartProps) {
  // Group data by ticker if multiple tickers
  const groupedData: { [key: string]: MonthlyTrend[] } = {};
  data.forEach((item) => {
    if (!groupedData[item.ticker]) {
      groupedData[item.ticker] = [];
    }
    groupedData[item.ticker].push(item);
  });

  // Format data for recharts - combine all tickers by month
  const monthMap: { [key: string]: any } = {};
  
  data.forEach((item) => {
    const monthKey = `${item.year}-${String(item.month).padStart(2, '0')}`;
    const monthLabel = `${MONTH_NAMES[item.month - 1]} ${item.year}`;
    
    if (!monthMap[monthKey]) {
      monthMap[monthKey] = {
        month: monthLabel,
        sortKey: monthKey,
      };
    }
    
    monthMap[monthKey][`${item.ticker}_close`] = Number(item.avg_close);
    monthMap[monthKey][`${item.ticker}_volume`] = Number(item.total_volume);
  });

  const chartData = Object.values(monthMap).sort((a, b) => 
    a.sortKey.localeCompare(b.sortKey)
  );

  // Get unique tickers for legend
  const tickers = Array.from(new Set(data.map(d => d.ticker)));

  // Colors for different tickers
  const colors = [
    'hsl(var(--chart-1))',
    'hsl(var(--chart-2))',
    'hsl(var(--chart-3))',
    'hsl(var(--chart-4))',
    'hsl(var(--chart-5))',
  ];

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle>Monthly Stock Performance{ticker ? ` - ${ticker}` : ''}</CardTitle>
        <CardDescription>
          Monthly average close price and volume comparison
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-8">
          {/* Average Close Price */}
          <div>
            <h3 className="text-sm font-medium mb-4">Average Close Price by Month</h3>
            <ResponsiveContainer width="100%" height={350}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="month" 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
                  angle={-45}
                  textAnchor="end"
                  height={80}
                />
                <YAxis 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
                />
                <Tooltip 
                  contentStyle={{
                    backgroundColor: 'hsl(var(--card))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                  }}
                />
                <Legend />
                {tickers.map((ticker, index) => (
                  <Bar
                    key={ticker}
                    dataKey={`${ticker}_close`}
                    fill={colors[index % colors.length]}
                    name={`${ticker} Avg Close`}
                    radius={[4, 4, 0, 0]}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Total Volume */}
          <div>
            <h3 className="text-sm font-medium mb-4">Total Volume by Month</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="month" 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
                  angle={-45}
                  textAnchor="end"
                  height={80}
                />
                <YAxis 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
                />
                <Tooltip 
                  contentStyle={{
                    backgroundColor: 'hsl(var(--card))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                  }}
                />
                <Legend />
                {tickers.map((ticker, index) => (
                  <Bar
                    key={ticker}
                    dataKey={`${ticker}_volume`}
                    fill={colors[index % colors.length]}
                    name={`${ticker} Volume`}
                    radius={[4, 4, 0, 0]}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
