'use client';

import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { DailyTrend } from '@/lib/db';
import { format } from 'date-fns';

interface DailyTrendChartProps {
  data: DailyTrend[];
  ticker?: string;
}

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
    <Card className="w-full">
      <CardHeader>
        <CardTitle>Daily Stock Trend{ticker ? ` - ${ticker}` : ''}</CardTitle>
        <CardDescription>
          Price movements and volume over time
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-8">
          {/* Price Chart */}
          <div>
            <h3 className="text-sm font-medium mb-4">Price Trend</h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="date" 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
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
                  labelStyle={{ color: 'hsl(var(--foreground))' }}
                />
                <Legend />
                <Line 
                  type="monotone" 
                  dataKey="avgClose" 
                  stroke="hsl(var(--chart-1))" 
                  strokeWidth={2}
                  name="Avg Close"
                  dot={false}
                />
                <Line 
                  type="monotone" 
                  dataKey="maxClose" 
                  stroke="hsl(var(--chart-2))" 
                  strokeWidth={1.5}
                  strokeDasharray="5 5"
                  name="Max Close"
                  dot={false}
                />
                <Line 
                  type="monotone" 
                  dataKey="minClose" 
                  stroke="hsl(var(--chart-3))" 
                  strokeWidth={1.5}
                  strokeDasharray="5 5"
                  name="Min Close"
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Volume Chart */}
          <div>
            <h3 className="text-sm font-medium mb-4">Trading Volume</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="date" 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
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
                <Bar 
                  dataKey="volume" 
                  fill="hsl(var(--chart-4))" 
                  name="Volume"
                  radius={[4, 4, 0, 0]}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Technical Indicators */}
          <div>
            <h3 className="text-sm font-medium mb-4">Technical Indicators</h3>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="date" 
                  className="text-xs"
                  tick={{ fill: 'hsl(var(--muted-foreground))' }}
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
                <Line 
                  type="monotone" 
                  dataKey="rsi" 
                  stroke="hsl(var(--chart-2))" 
                  strokeWidth={2}
                  name="RSI"
                  dot={false}
                />
                <Line 
                  type="monotone" 
                  dataKey="roc" 
                  stroke="hsl(var(--chart-5))" 
                  strokeWidth={2}
                  name="ROC"
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
