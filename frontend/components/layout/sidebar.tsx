"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { BarChart3, TrendingUp, Trophy, LayoutDashboard } from "lucide-react"
import { cn } from "@/lib/utils"

const sidebarItems = [
  {
    title: "Daily Trends",
    href: "/daily-trend",
    icon: TrendingUp,
  },
  {
    title: "Monthly Trends",
    href: "/monthly-trend",
    icon: BarChart3,
  },
  {
    title: "Stock Ranking",
    href: "/stock-ranking",
    icon: Trophy,
  },
]

export function Sidebar() {
  const pathname = usePathname()

  return (
    <div className="flex h-screen w-64 flex-col border-r bg-card">
      <div className="flex h-14 items-center border-b px-4">
        <Link href="/" className="flex items-center gap-2 font-semibold">
          <LayoutDashboard className="h-6 w-6 text-primary" />
          <span>Analytics</span>
        </Link>
      </div>
      <div className="flex-1 overflow-auto py-2">
        <nav className="grid items-start px-2 text-sm font-medium">
          {sidebarItems.map((item) => {
            const Icon = item.icon
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "group flex items-center gap-3 rounded-lg px-3 py-2 transition-all hover:text-primary",
                  pathname === item.href
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:bg-muted"
                )}
              >
                <Icon className="h-4 w-4" />
                {item.title}
              </Link>
            )
          })}
        </nav>
      </div>
      <div className="border-t p-4">
        <p className="text-xs text-muted-foreground text-center">
          © 2024 Stock Analytics
        </p>
      </div>
    </div>
  )
}
