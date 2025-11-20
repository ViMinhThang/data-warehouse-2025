'use client';

import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from '@/components/ui/chart';
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

  // Dynamic chart config based on tickers
  const chartConfig: ChartConfig = {};
  tickers.forEach((ticker, index) => {
    chartConfig[`${ticker}_close`] = {
      label: `${ticker} Avg Close`,
      color: `hsl(var(--chart-${(index % 5) + 1}))`,
    };
    chartConfig[`${ticker}_volume`] = {
      label: `${ticker} Volume`,
      color: `hsl(var(--chart-${(index % 5) + 1}))`,
    };
  });

  return (
    <div className="space-y-8">
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Monthly Average Close Price{ticker ? ` - ${ticker}` : ''}</CardTitle>
          <CardDescription>
            Comparison across months
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[350px] w-full">
            <BarChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="month" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <ChartTooltip 
                cursor={{ fill: 'hsl(var(--muted)/0.5)' }}
                content={<ChartTooltipContent />} 
              />
              <ChartLegend content={<ChartLegendContent />} />
              {tickers.map((ticker) => (
                <Bar
                  key={ticker}
                  dataKey={`${ticker}_close`}
                  fill={`var(--color-${ticker}_close)`}
                  radius={[4, 4, 0, 0]}
                />
              ))}
            </BarChart>
          </ChartContainer>
        </CardContent>
      </Card>

      <Card className="w-full">
        <CardHeader>
          <CardTitle>Monthly Total Volume</CardTitle>
          <CardDescription>
            Volume comparison across months
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[300px] w-full">
            <BarChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="month" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <ChartTooltip 
                cursor={{ fill: 'hsl(var(--muted)/0.5)' }}
                content={<ChartTooltipContent />} 
              />
              <ChartLegend content={<ChartLegendContent />} />
              {tickers.map((ticker) => (
                <Bar
                  key={ticker}
                  dataKey={`${ticker}_volume`}
                  fill={`var(--color-${ticker}_volume)`}
                  radius={[4, 4, 0, 0]}
                />
              ))}
            </BarChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}

