import { NextRequest, NextResponse } from 'next/server';
import { query, MonthlyTrend } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const ticker = searchParams.get('ticker');
    const year = searchParams.get('year');

    // Build query based on parameters
    let queryText = 'SELECT * FROM stock_monthly_trend';
    const queryParams: any[] = [];
    const conditions: string[] = [];

    if (ticker) {
      queryParams.push(ticker);
      conditions.push(`ticker = $${queryParams.length}`);
    }

    if (year) {
      queryParams.push(parseInt(year));
      conditions.push(`year = $${queryParams.length}`);
    }

    if (conditions.length > 0) {
      queryText += ' WHERE ' + conditions.join(' AND ');
    }

    queryText += ' ORDER BY year DESC, month DESC LIMIT 500';

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
