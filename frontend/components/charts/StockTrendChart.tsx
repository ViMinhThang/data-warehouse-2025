import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { StockDailyTrend } from '../../types';

interface Props {
    data: StockDailyTrend[];
    ticker: string;
}

const StockTrendChart: React.FC<Props> = ({ data, ticker }) => {
    const tickerData = data.filter(d => d.ticker === ticker);

    return (
        <div className="h-[350px] w-full">
            <ResponsiveContainer width="100%" height="100%">
                <LineChart
                    data={tickerData}
                    margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                    <XAxis 
                        dataKey="full_date" 
                        tickFormatter={(str) => new Date(str).toLocaleDateString(undefined, {month:'short', day:'numeric'})}
                        stroke="#64748b"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                    />
                    <YAxis 
                        stroke="#64748b"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                        domain={['auto', 'auto']}
                        tickFormatter={(val) => `$${val}`}
                    />
                    <Tooltip 
                        contentStyle={{ backgroundColor: '#fff', borderRadius: '8px', border: '1px solid #e2e8f0' }}
                        formatter={(value: number) => [`$${value.toFixed(2)}`, 'Price']}
                    />
                    <Legend />
                    {/* Blue Monochrome Theme */}
                    <Line 
                        type="monotone" 
                        dataKey="avg_close" 
                        name={`${ticker} Avg Close`}
                        stroke="#2563eb" 
                        strokeWidth={3}
                        dot={false}
                        activeDot={{ r: 6, fill: '#2563eb', strokeWidth: 0 }}
                    />
                     <Line 
                        type="monotone" 
                        dataKey="max_close" 
                        name="High"
                        stroke="#93c5fd" 
                        strokeWidth={1}
                        dot={false}
                        strokeDasharray="5 5"
                    />
                    <Line 
                        type="monotone" 
                        dataKey="min_close" 
                        name="Low"
                        stroke="#bfdbfe" 
                        strokeWidth={1}
                        dot={false}
                        strokeDasharray="3 3"
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
};

export default StockTrendChart;
