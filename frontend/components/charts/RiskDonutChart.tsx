import React from 'react';
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { StockRankingSnapshot } from '../../types';

interface Props {
    data: StockRankingSnapshot[];
}

const RiskDonutChart: React.FC<Props> = ({ data }) => {
    const riskCounts = data.reduce((acc, curr) => {
        acc[curr.risk_category] = (acc[curr.risk_category] || 0) + 1;
        return acc;
    }, {} as Record<string, number>);

    // Blue Monochrome Palette
    const chartData = [
        { name: 'Low Risk', value: riskCounts['Low Risk'] || 0, color: '#bfdbfe' },    // Blue 200
        { name: 'Medium Risk', value: riskCounts['Medium Risk'] || 0, color: '#3b82f6' }, // Blue 500
        { name: 'High Risk', value: riskCounts['High Risk'] || 0, color: '#1e3a8a' },   // Blue 900
    ];

    return (
        <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                    <Pie
                        data={chartData}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={100}
                        paddingAngle={2}
                        dataKey="value"
                        stroke="none"
                    >
                        {chartData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                        ))}
                    </Pie>
                    <Tooltip 
                        contentStyle={{ backgroundColor: '#fff', borderRadius: '8px', border: '1px solid #e2e8f0' }}
                        itemStyle={{ color: '#1e293b' }}
                    />
                    <Legend verticalAlign="bottom" height={36} iconType="circle"/>
                </PieChart>
            </ResponsiveContainer>
        </div>
    );
};

export default RiskDonutChart;
