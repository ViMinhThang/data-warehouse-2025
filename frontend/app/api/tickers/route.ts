import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export async function GET() {
  try {
    // Get distinct tickers from all three tables
    const queryText = `
      SELECT DISTINCT ticker FROM (
        SELECT ticker FROM stock_daily_trend
        UNION
        SELECT ticker FROM stock_monthly_trend
        UNION
        SELECT ticker FROM stock_ranking_snapshot
      ) AS all_tickers
      ORDER BY ticker
    `;

    const result = await query<{ ticker: string }>(queryText);

    return NextResponse.json({
      success: true,
      data: result.rows.map((row) => row.ticker),
      count: result.rowCount,
    });
  } catch (error) {
    console.error('Error fetching tickers:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch tickers',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
