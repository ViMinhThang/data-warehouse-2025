import React from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { MarketLiquidityHistory } from '../../types';

interface Props {
    data: MarketLiquidityHistory[];
}

const LiquidityChart: React.FC<Props> = ({ data }) => {
    return (
        <div className="h-[350px] w-full">
            <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                    data={data}
                    margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                >
                    <defs>
                        {/* Strictly BLUE gradients */}
                        <linearGradient id="colorVol" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#2563eb" stopOpacity={0.3}/>
                            <stop offset="95%" stopColor="#2563eb" stopOpacity={0}/>
                        </linearGradient>
                        <linearGradient id="colorMa" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#1e40af" stopOpacity={0.3}/>
                            <stop offset="95%" stopColor="#1e40af" stopOpacity={0}/>
                        </linearGradient>
                    </defs>
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
                        tickFormatter={(val) => `${(val / 1000000).toFixed(0)}M`}
                    />
                    <Tooltip 
                        contentStyle={{ backgroundColor: '#fff', borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}
                        itemStyle={{ color: '#1e293b' }}
                        labelStyle={{ color: '#64748b', marginBottom: '0.5rem' }}
                    />
                    <Legend iconType="circle" />
                    <Area 
                        type="monotone" 
                        dataKey="total_market_volume" 
                        name="Daily Volume"
                        stroke="#2563eb" 
                        fillOpacity={1} 
                        fill="url(#colorVol)" 
                        strokeWidth={2}
                    />
                    <Area 
                        type="monotone" 
                        dataKey="volume_moving_avg_7d" 
                        name="7D MA"
                        stroke="#1e40af" 
                        fillOpacity={1} 
                        fill="url(#colorMa)" 
                        strokeDasharray="4 4"
                        strokeWidth={2}
                    />
                </AreaChart>
            </ResponsiveContainer>
        </div>
    );
};

export default LiquidityChart;
