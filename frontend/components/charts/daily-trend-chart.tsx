'use client';

import { CartesianGrid, Line, LineChart, XAxis, YAxis, Bar, BarChart } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from '@/components/ui/chart';
import { DailyTrend } from '@/lib/db';
import { format } from 'date-fns';

interface DailyTrendChartProps {
  data: DailyTrend[];
  ticker?: string;
}

const chartConfig = {
  avgClose: {
    label: "Avg Close",
    color: "hsl(var(--chart-1))",
  },
  maxClose: {
    label: "Max Close",
    color: "hsl(var(--chart-2))",
  },
  minClose: {
    label: "Min Close",
    color: "hsl(var(--chart-3))",
  },
  volume: {
    label: "Volume",
    color: "hsl(var(--chart-4))",
  },
  rsi: {
    label: "RSI",
    color: "hsl(var(--chart-2))",
  },
  roc: {
    label: "ROC",
    color: "hsl(var(--chart-5))",
  },
} satisfies ChartConfig;

export function DailyTrendChart({ data, ticker }: DailyTrendChartProps) {
  // Format data for recharts
  const chartData = data.map((item) => ({
    date: format(new Date(item.full_date), 'MMM dd'),
    fullDate: item.full_date,
    avgClose: Number(item.avg_close),
    maxClose: Number(item.max_close),
    minClose: Number(item.min_close),
    volume: Number(item.total_volume),
    rsi: Number(item.avg_rsi),
    roc: Number(item.avg_roc),
  }));

  return (
    <div className="space-y-8">
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Daily Stock Trend{ticker ? ` - ${ticker}` : ''}</CardTitle>
          <CardDescription>
            Price movements over time
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[300px] w-full">
            <LineChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="date" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <ChartTooltip content={<ChartTooltipContent />} />
              <ChartLegend content={<ChartLegendContent />} />
              <Line 
                type="monotone" 
                dataKey="avgClose" 
                stroke="var(--color-avgClose)" 
                strokeWidth={2}
                dot={false}
              />
              <Line 
                type="monotone" 
                dataKey="maxClose" 
                stroke="var(--color-maxClose)" 
                strokeWidth={1.5}
                strokeDasharray="5 5"
                dot={false}
              />
              <Line 
                type="monotone" 
                dataKey="minClose" 
                stroke="var(--color-minClose)" 
                strokeWidth={1.5}
                strokeDasharray="5 5"
                dot={false}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>

      <Card className="w-full">
        <CardHeader>
          <CardTitle>Trading Volume</CardTitle>
          <CardDescription>
            Daily trading volume
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[200px] w-full">
            <BarChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="date" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Bar 
                dataKey="volume" 
                fill="var(--color-volume)" 
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ChartContainer>
        </CardContent>
      </Card>

      <Card className="w-full">
        <CardHeader>
          <CardTitle>Technical Indicators</CardTitle>
          <CardDescription>
            RSI and ROC indicators
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[200px] w-full">
            <LineChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="date" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
              />
              <ChartTooltip content={<ChartTooltipContent />} />
              <ChartLegend content={<ChartLegendContent />} />
              <Line 
                type="monotone" 
                dataKey="rsi" 
                stroke="var(--color-rsi)" 
                strokeWidth={2}
                dot={false}
              />
              <Line 
                type="monotone" 
                dataKey="roc" 
                stroke="var(--color-roc)" 
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}

