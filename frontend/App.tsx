import React, { useState, useEffect } from 'react';
import Layout from './app/layout';
import OverviewPage from './app/page';
import TrendsPage from './app/trends/page';
import RiskPage from './app/risk/page';
import { fetchDailyTrends, fetchMarketLiquidity, fetchRankingSnapshot } from './services/dbService';
import { StockDailyTrend, MarketLiquidityHistory, StockRankingSnapshot } from './types';
import { Loader2, DatabaseZap } from 'lucide-react';

export default function App() {
    // Simple client-side router state
    const [currentPath, setCurrentPath] = useState('/');
    
    const [loading, setLoading] = useState(true);
    const [liquidityData, setLiquidityData] = useState<MarketLiquidityHistory[]>([]);
    const [trendData, setTrendData] = useState<StockDailyTrend[]>([]);
    const [rankingData, setRankingData] = useState<StockRankingSnapshot[]>([]);

    useEffect(() => {
        const loadData = async () => {
            setLoading(true);
            try {
                // Simulate fetching data from the stored procedures defined in the prompt
                const [liq, trends, ranks] = await Promise.all([
                    fetchMarketLiquidity(),
                    fetchDailyTrends(),
                    fetchRankingSnapshot()
                ]);
                setLiquidityData(liq);
                setTrendData(trends);
                setRankingData(ranks);
            } catch (error) {
                console.error("Failed to fetch data from DM", error);
            } finally {
                setLoading(false);
            }
        };
        loadData();
    }, []);

    if (loading) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-slate-50">
                <div className="flex flex-col items-center space-y-4">
                    <div className="relative">
                        <div className="w-16 h-16 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin"></div>
                        <DatabaseZap className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 text-blue-600 w-6 h-6" />
                    </div>
                    <div className="text-center">
                        <h3 className="text-lg font-semibold text-slate-900">Connecting to PostgreSQL...</h3>
                        <p className="text-sm text-slate-500">Executing stored procedures</p>
                    </div>
                </div>
            </div>
        );
    }

    const renderPage = () => {
        switch(currentPath) {
            case '/':
                return <OverviewPage data={liquidityData} />;
            case '/trends':
                return <TrendsPage data={trendData} />;
            case '/risk':
                return <RiskPage data={rankingData} />;
            default:
                return <OverviewPage data={liquidityData} />;
        }
    };

    return (
        <Layout currentPath={currentPath} navigate={setCurrentPath}>
            {renderPage()}
        </Layout>
    );
}
