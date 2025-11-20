import Link from "next/link";
import { ArrowRight, BarChart3, TrendingUp, Database } from "lucide-react";

export default function Home() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-background via-background to-primary/5">
      <main className="flex flex-col items-center justify-center gap-8 px-8 py-16 text-center max-w-4xl">
        <div className="flex items-center gap-3 text-primary">
          <Database className="h-12 w-12" />
          <BarChart3 className="h-16 w-16" />
          <TrendingUp className="h-12 w-12" />
        </div>
        
        <div className="space-y-4">
          <h1 className="text-5xl font-bold bg-gradient-to-r from-primary via-accent to-primary bg-clip-text text-transparent">
            Stock Analytics Dashboard
          </h1>
          <p className="text-xl text-muted-foreground max-w-2xl">
            Comprehensive data warehouse analytics for stock market trends, performance metrics, and risk analysis
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mt-8 w-full">
          <div className="p-6 rounded-lg border border-border bg-card hover:border-primary/50 transition-colors">
            <h3 className="font-semibold text-lg mb-2">Daily Trends</h3>
            <p className="text-sm text-muted-foreground">
              Real-time price movements and volume analysis
            </p>
          </div>
          <div className="p-6 rounded-lg border border-border bg-card hover:border-primary/50 transition-colors">
            <h3 className="font-semibold text-lg mb-2">Monthly Performance</h3>
            <p className="text-sm text-muted-foreground">
              Month-over-month comparison across stocks
            </p>
          </div>
          <div className="p-6 rounded-lg border border-border bg-card hover:border-primary/50 transition-colors">
            <h3 className="font-semibold text-lg mb-2">Stock Rankings</h3>
            <p className="text-sm text-muted-foreground">
              Performance metrics with risk categorization
            </p>
          </div>
        </div>

        <Link
          href="/dashboard"
          className="group mt-8 flex items-center gap-2 rounded-full bg-primary px-8 py-4 text-lg font-medium text-primary-foreground transition-all hover:bg-primary/90 hover:gap-4"
        >
          View Dashboard
          <ArrowRight className="h-5 w-5 transition-transform group-hover:translate-x-1" />
        </Link>

        <div className="mt-12 text-sm text-muted-foreground">
          <p>Powered by PostgreSQL Data Warehouse • Built with Next.js 16</p>
        </div>
      </main>
    </div>
  );
}
