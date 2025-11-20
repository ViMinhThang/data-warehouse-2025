"use client";

import React from "react";
import { usePathname, useRouter } from "next/navigation";
import { Activity, Database, LayoutDashboard, ShieldAlert } from "lucide-react";

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const pathname = usePathname();
  const router = useRouter();

  const navItems = [
    { path: "/", label: "Market Overview", icon: LayoutDashboard },
    { path: "/trends", label: "Stock Trends", icon: Activity },
    { path: "/risk", label: "Risk & Rankings", icon: ShieldAlert },
  ];

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col md:flex-row font-sans text-slate-900">
      {/* Sidebar */}
      <aside className="w-full md:w-64 bg-white border-r border-slate-200 flex-shrink-0 flex flex-col fixed md:relative z-20 h-full">
        <div className="p-6 border-b border-slate-100">
          <div className="flex items-center space-x-3">
            <div className="bg-blue-600 p-2 rounded-lg shadow-lg shadow-blue-600/20">
              <Database className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-lg font-bold tracking-tight text-slate-900">
                BlueFin
              </h1>
              <p className="text-xs text-slate-500 font-medium">
                Analytics Dashboard
              </p>
            </div>
          </div>
        </div>

        <nav className="p-4 space-y-1 flex-1">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = pathname === item.path;
            return (
              <button
                key={item.path}
                onClick={() => router.push(item.path)}
                className={`w-full flex items-center space-x-3 px-4 py-3 rounded-lg text-sm font-medium transition-all duration-200 ${
                  isActive
                    ? "bg-blue-50 text-blue-700 shadow-sm ring-1 ring-blue-100"
                    : "text-slate-600 hover:bg-slate-50 hover:text-slate-900"
                }`}
              >
                <Icon
                  className={`w-5 h-5 ${
                    isActive ? "text-blue-600" : "text-slate-400"
                  }`}
                />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>

        <div className="p-4">
          <div className="bg-slate-900 text-slate-200 p-4 rounded-xl border border-slate-800 shadow-xl">
            <div className="flex items-center space-x-2 mb-3">
              <span className="relative flex h-3 w-3">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
              </span>
              <span className="text-xs font-semibold uppercase tracking-wider">
                PostgreSQL Connected
              </span>
            </div>
            <div className="space-y-1 text-xs font-mono bg-slate-950/50 p-2 rounded border border-slate-800/50">
              <div className="flex justify-between">
                <span className="text-slate-500">DB:</span>
                <span className="text-blue-400">dm</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-500">User:</span>
                <span className="text-blue-400">fragile</span>
              </div>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto p-6 md:p-8 relative">
        <div className="max-w-7xl mx-auto">{children}</div>
      </main>
    </div>
  );
}
