import { NextRequest, NextResponse } from 'next/server';
import { query, MonthlyTrend } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const queryText = 'SELECT * FROM stock_monthly_trend ORDER BY year DESC, month DESC LIMIT 1000';
    const result = await query<MonthlyTrend>(queryText);

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
