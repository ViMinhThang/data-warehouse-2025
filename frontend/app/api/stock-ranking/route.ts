import { NextRequest, NextResponse } from 'next/server';
import { query, StockRanking } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const queryText = `
      SELECT * FROM stock_ranking_snapshot
      ORDER BY price_change DESC
      LIMIT 100
    `;
    const result = await query<StockRanking>(queryText);

    return NextResponse.json({
      success: true,
      data: result.rows,
      count: result.rowCount,
    });
  } catch (error) {
    console.error('Error fetching stock ranking:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch stock ranking data',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
