import React from 'react';
import { StockRankingSnapshot } from '../../types';

interface Props {
    data: StockRankingSnapshot[];
}

const RankingTable: React.FC<Props> = ({ data }) => {
    return (
        <div className="overflow-x-auto">
            <table className="w-full text-sm text-left">
                <thead className="bg-slate-50 text-slate-500 font-medium border-b border-slate-200">
                    <tr>
                        <th className="px-6 py-3">Ticker</th>
                        <th className="px-6 py-3">Price Change</th>
                        <th className="px-6 py-3">Volatility</th>
                        <th className="px-6 py-3">RSI</th>
                        <th className="px-6 py-3">Risk Category</th>
                    </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                    {data.map((stock) => (
                        <tr key={stock.ticker} className="hover:bg-slate-50/50 transition-colors">
                            <td className="px-6 py-4 font-semibold text-slate-900">{stock.ticker}</td>
                            <td className={`px-6 py-4 font-medium ${stock.price_change >= 0 ? 'text-blue-600' : 'text-red-500'}`}>
                                {stock.price_change > 0 ? '+' : ''}{stock.price_change}%
                            </td>
                            <td className="px-6 py-4 text-slate-600">{stock.avg_volatility.toFixed(2)}</td>
                            <td className="px-6 py-4">
                                <div className="flex items-center space-x-2">
                                    <span className="text-slate-600 w-8">{stock.avg_rsi.toFixed(0)}</span>
                                    <div className="h-1.5 w-16 bg-slate-200 rounded-full overflow-hidden">
                                        <div 
                                            className={`h-full ${stock.avg_rsi > 70 ? 'bg-blue-600' : stock.avg_rsi < 30 ? 'bg-blue-300' : 'bg-blue-400'}`} 
                                            style={{ width: `${stock.avg_rsi}%` }}
                                        />
                                    </div>
                                </div>
                            </td>
                            <td className="px-6 py-4">
                                <span className={`px-2.5 py-1 rounded-full text-xs font-medium border
                                    ${stock.risk_category === 'High Risk' 
                                        ? 'bg-red-50 text-red-700 border-red-100' 
                                        : stock.risk_category === 'Medium Risk'
                                            ? 'bg-yellow-50 text-yellow-700 border-yellow-100'
                                            : 'bg-blue-50 text-blue-700 border-blue-100'
                                    }`}>
                                    {stock.risk_category}
                                </span>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

export default RankingTable;