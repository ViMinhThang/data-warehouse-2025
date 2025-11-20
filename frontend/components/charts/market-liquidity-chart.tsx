'use client';

import { CartesianGrid, Line, LineChart, XAxis, YAxis, Area, AreaChart, ComposedChart } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from '@/components/ui/chart';
import { MarketLiquidity } from '@/lib/db';
import { format } from 'date-fns';

interface MarketLiquidityChartProps {
  data: MarketLiquidity[];
}

const chartConfig = {
  volume: {
    label: "Total Volume",
    color: "hsl(var(--chart-1))",
  },
  movingAvg: {
    label: "7D Moving Avg",
    color: "hsl(var(--chart-2))",
  },
  stocksCount: {
    label: "Stocks Traded",
    color: "hsl(var(--chart-3))",
  },
} satisfies ChartConfig;

export function MarketLiquidityChart({ data }: MarketLiquidityChartProps) {
  const chartData = data.map((item) => ({
    date: format(new Date(item.full_date), 'MMM dd'),
    fullDate: item.full_date,
    volume: Number(item.total_market_volume),
    movingAvg: Number(item.volume_moving_avg_7d),
    stocksCount: Number(item.stocks_traded_count),
  }));

  return (
    <div className="space-y-8">
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Market Liquidity & Volume</CardTitle>
          <CardDescription>
            Total market volume and moving averages
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[400px] w-full">
            <ComposedChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="fillVolume" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="var(--color-volume)" stopOpacity={0.8} />
                  <stop offset="95%" stopColor="var(--color-volume)" stopOpacity={0.1} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="date" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                minTickGap={32}
              />
              <YAxis 
                yAxisId="left"
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                tickFormatter={(value) => (value / 1000000).toFixed(0) + 'M'}
              />
              <YAxis 
                yAxisId="right"
                orientation="right"
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <ChartTooltip 
                cursor={false}
                content={<ChartTooltipContent indicator="dot" />} 
              />
              <ChartLegend content={<ChartLegendContent />} />
              <Area
                yAxisId="left"
                dataKey="volume"
                type="monotone"
                fill="url(#fillVolume)"
                fillOpacity={0.4}
                stroke="var(--color-volume)"
                strokeWidth={2}
              />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="movingAvg"
                stroke="var(--color-movingAvg)"
                strokeWidth={2}
                dot={false}
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="stocksCount"
                stroke="var(--color-stocksCount)"
                strokeWidth={2}
                dot={false}
                strokeDasharray="5 5"
              />
            </ComposedChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}
