import { NextRequest, NextResponse } from 'next/server';
import { query, StockRanking } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = searchParams.get('limit') || '50';
    const sortBy = searchParams.get('sort_by') || 'price_change';
    const order = searchParams.get('order') || 'DESC';

    // Validate sort column to prevent SQL injection
    const validSortColumns = [
      'ticker',
      'price_change',
      'avg_rsi',
      'avg_roc_performance',
      'avg_volatility',
      'snapshot_date',
    ];

    const sortColumn = validSortColumns.includes(sortBy) ? sortBy : 'price_change';
    const sortOrder = order.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    const queryText = `
      SELECT * FROM dm_dw.stock_ranking_snapshot
      ORDER BY ${sortColumn} ${sortOrder}
      LIMIT $1
    `;

    const result = await query<StockRanking>(queryText, [parseInt(limit)]);

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
