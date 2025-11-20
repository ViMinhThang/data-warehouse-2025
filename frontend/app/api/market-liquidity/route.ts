import { NextRequest, NextResponse } from 'next/server';
import { query, MarketLiquidity } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const queryText = 'SELECT * FROM market_liquidity_history ORDER BY full_date DESC LIMIT 1000';
    const result = await query<MarketLiquidity>(queryText);

    return NextResponse.json({
      success: true,
      data: result.rows,
      count: result.rowCount,
    });
  } catch (error) {
    console.error('Error fetching market liquidity:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch market liquidity data',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
