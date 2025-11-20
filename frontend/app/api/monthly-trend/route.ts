import { NextRequest, NextResponse } from 'next/server';
import { query, MonthlyTrend } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const ticker = searchParams.get('ticker');

    let queryText = 'SELECT * FROM stock_monthly_trend';
    const queryParams: any[] = [];

    if (ticker) {
      queryText += ' WHERE ticker = $1';
      queryParams.push(ticker);
    }

    // Order chronologically for the chart (ASC)
    queryText += ' ORDER BY year ASC, month ASC LIMIT 1000';

    const result = await query<MonthlyTrend>(queryText, queryParams);

    return NextResponse.json({
      success: true,
      data: result.rows,
      count: result.rowCount,
    });
  } catch (error) {
    console.error('Error fetching monthly trend:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch monthly trend data',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}