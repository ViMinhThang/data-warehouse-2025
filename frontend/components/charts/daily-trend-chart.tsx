'use client';

import { CartesianGrid, Line, LineChart, XAxis, YAxis, Bar, BarChart, Area, AreaChart, Brush } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from '@/components/ui/chart';
import { DailyTrend } from '@/lib/db';
import { format, isValid, parseISO } from 'date-fns';

interface DailyTrendChartProps {
  data: DailyTrend[];
  ticker?: string;
}

const chartConfig = {
  avgClose: {
    label: "Avg Close",
    color: "hsl(221.2, 83.2%, 53.3%)", // Blue
  },
  maxClose: {
    label: "Max Close",
    color: "hsl(217.2, 91.2%, 59.8%)", // Lighter Blue
  },
  minClose: {
    label: "Min Close",
    color: "hsl(224.3, 76.3%, 48%)", // Darker Blue
  },
  volume: {
    label: "Volume",
    color: "hsl(226, 70%, 55.5%)", // Another Blue Variant
  },
  rsi: {
    label: "RSI",
    color: "hsl(210, 100%, 50%)", // Pure Blue for RSI
  },
  roc: {
    label: "ROC",
    color: "hsl(199, 89%, 48%)", // Cyan/Blue for ROC
  },
} satisfies ChartConfig;

export function DailyTrendChart({ data, ticker }: DailyTrendChartProps) {
  // Format data for recharts with safety checks
  const chartData = data.map((item) => {
    // Safely handle date parsing
    let dateStr = '';
    try {
        const date = new Date(item.full_date);
        if (isValid(date)) {
            dateStr = format(date, 'MMM dd');
        } else {
             // Fallback if date is invalid string
             dateStr = item.full_date; 
        }
    } catch (e) {
        dateStr = item.full_date;
    }

    return {
        date: dateStr,
        fullDate: item.full_date,
        avgClose: item.avg_close != null ? Number(item.avg_close) : 0,
        maxClose: item.max_close != null ? Number(item.max_close) : 0,
        minClose: item.min_close != null ? Number(item.min_close) : 0,
        volume: item.total_volume != null ? Number(item.total_volume) : 0,
        // Default RSI/ROC to null if missing so chart doesn't plot 0 lines for missing data
        rsi: item.avg_rsi != null ? Number(item.avg_rsi) : undefined,
        roc: item.avg_roc != null ? Number(item.avg_roc) : undefined,
    };
  });

  return (
    <div className="space-y-8">
      {/* Price Chart */}
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Daily Stock Trend{ticker ? ` - ${ticker}` : ''}</CardTitle>
          <CardDescription>
            Price movements over time
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[400px] min-h-[400px] w-full">
            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="fillAvgClose" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="var(--color-avgClose)" stopOpacity={0.8} />
                  <stop offset="95%" stopColor="var(--color-avgClose)" stopOpacity={0.1} />
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
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                domain={['auto', 'auto']}
              />
              <ChartTooltip 
                cursor={false}
                content={<ChartTooltipContent indicator="dot" />} 
              />
              <ChartLegend content={<ChartLegendContent />} />
              <Area
                dataKey="avgClose"
                type="monotone"
                fill="url(#fillAvgClose)"
                fillOpacity={0.4}
                stroke="var(--color-avgClose)"
                strokeWidth={2}
                activeDot={{ r: 6 }}
              />
              {/* Added Brush for Zooming */}
              <Brush 
                dataKey="date" 
                height={30} 
                stroke="var(--color-muted-foreground)"
                fill="var(--color-background)"
                tickFormatter={() => ""} 
              />
            </AreaChart>
          </ChartContainer>
        </CardContent>
      </Card>

      {/* Volume Chart */}
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Trading Volume</CardTitle>
          <CardDescription>
            Daily trading volume
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[200px] min-h-[200px] w-full">
            <BarChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="date" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                minTickGap={32}
              />
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                tickFormatter={(val) => (val > 1000000 ? `${(val/1000000).toFixed(1)}M` : val)}
              />
              <ChartTooltip 
                cursor={{ fill: 'hsl(var(--muted)/0.5)' }}
                content={<ChartTooltipContent />} 
              />
              <Bar 
                dataKey="volume" 
                fill="var(--color-volume)" 
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ChartContainer>
        </CardContent>
      </Card>

      {/* Technical Indicators Chart */}
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Technical Indicators</CardTitle>
          <CardDescription>
            RSI and ROC indicators
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer config={chartConfig} className="h-[200px] min-h-[200px] w-full">
            <LineChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis 
                dataKey="date" 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                minTickGap={32}
              />
              {/* Changed YAxis domain to 'auto' to fix the scaling issue seen in the screenshot.
                The screenshot showed values like 6,923,074 on the axis, implying data was out of the [0, 100] range
                or incorrectly mapped. Setting to auto allows debugging or proper scaling if ROC > 100.
              */}
              <YAxis 
                tickLine={false}
                axisLine={false}
                tickMargin={8}
                domain={['auto', 'auto']} 
              />
              <ChartTooltip 
                cursor={false}
                content={<ChartTooltipContent />} 
              />
              <ChartLegend content={<ChartLegendContent />} />
              <Line 
                type="monotone" 
                dataKey="rsi" 
                stroke="var(--color-rsi)" 
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
                connectNulls 
              />
              <Line 
                type="monotone" 
                dataKey="roc" 
                stroke="var(--color-roc)" 
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
                connectNulls
              />
            </LineChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}