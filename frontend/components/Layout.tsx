import React from 'react';
import { Activity, BarChart2, PieChart, Database, LayoutDashboard } from 'lucide-react';

interface LayoutProps {
    children: React.ReactNode;
    currentTab: string;
    setTab: (tab: string) => void;
}

const Layout: React.FC<LayoutProps> = ({ children, currentTab, setTab }) => {
    const navItems = [
        { id: 'overview', label: 'Market Overview', icon: LayoutDashboard },
        { id: 'trends', label: 'Stock Trends', icon: Activity },
        { id: 'risk', label: 'Risk & Rankings', icon: PieChart },
    ];

    return (
        <div className="min-h-screen bg-slate-50 flex flex-col md:flex-row">
            {/* Sidebar */}
            <aside className="w-full md:w-64 bg-white border-r border-slate-200 flex-shrink-0">
                <div className="p-6 border-b border-slate-100">
                    <div className="flex items-center space-x-2">
                        <div className="bg-blue-600 p-2 rounded-lg">
                            <Database className="w-6 h-6 text-white" />
                        </div>
                        <div>
                            <h1 className="text-xl font-bold text-slate-900">DM Analytics</h1>
                            <p className="text-xs text-slate-500">PostgreSQL Dashboard</p>
                        </div>
                    </div>
                </div>
                
                <nav className="p-4 space-y-2">
                    {navItems.map((item) => {
                        const Icon = item.icon;
                        const isActive = currentTab === item.id;
                        return (
                            <button
                                key={item.id}
                                onClick={() => setTab(item.id)}
                                className={`w-full flex items-center space-x-3 px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                                    isActive 
                                    ? 'bg-blue-50 text-blue-700' 
                                    : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'
                                }`}
                            >
                                <Icon className={`w-5 h-5 ${isActive ? 'text-blue-600' : 'text-slate-400'}`} />
                                <span>{item.label}</span>
                            </button>
                        );
                    })}
                </nav>

                <div className="p-4 mt-auto">
                    <div className="bg-blue-50 p-4 rounded-xl border border-blue-100">
                        <h4 className="text-xs font-semibold text-blue-800 uppercase mb-2">Connection Status</h4>
                        <div className="flex items-center space-x-2 text-sm text-blue-700">
                            <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
                            <span>Database: <strong>dm</strong></span>
                        </div>
                        <div className="text-xs text-blue-600 mt-1">User: fragile</div>
                    </div>
                </div>
            </aside>

            {/* Main Content */}
            <main className="flex-1 overflow-auto p-6 md:p-8">
                {children}
            </main>
        </div>
    );
};

export default Layout;