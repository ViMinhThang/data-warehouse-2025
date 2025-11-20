import { NextRequest, NextResponse } from 'next/server';
import { query, DailyTrend } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const ticker = searchParams.get('ticker');
    const startDate = searchParams.get('start_date');
    const endDate = searchParams.get('end_date');

    // Build query based on parameters
    let queryText = 'SELECT * FROM stock_daily_trend';
    const queryParams: any[] = [];
    const conditions: string[] = [];

    if (ticker) {
      queryParams.push(ticker);
      conditions.push(`ticker = $${queryParams.length}`);
    }

    if (startDate) {
      queryParams.push(startDate);
      conditions.push(`full_date >= $${queryParams.length}`);
    }

    if (endDate) {
      queryParams.push(endDate);
      conditions.push(`full_date <= $${queryParams.length}`);
    }

    if (conditions.length > 0) {
      queryText += ' WHERE ' + conditions.join(' AND ');
    }

    queryText += ' ORDER BY full_date DESC LIMIT 1000';

    const result = await query<DailyTrend>(queryText, queryParams);

    return NextResponse.json({
      success: true,
      data: result.rows,
      count: result.rowCount,
    });
  } catch (error) {
    console.error('Error fetching daily trend:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch daily trend data',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
