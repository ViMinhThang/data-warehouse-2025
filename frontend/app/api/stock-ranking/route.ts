import { NextRequest, NextResponse } from 'next/server';
import { query, StockRanking } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = searchParams.get('limit') || '50';
    const sortBy = searchParams.get('sort_by') || 'price_change';
    const order = searchParams.get('order') || 'DESC';

    // Validate sort column to prevent SQL injection
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
