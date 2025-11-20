import { NextRequest, NextResponse } from "next/server";
import { fetchDailyTrends } from "@/services/dbService";

export async function GET(req: NextRequest) {
  try {
    const ticker = req.nextUrl.searchParams.get("ticker");
    let data = await fetchDailyTrends();

    if (ticker) {
      data = data.filter((d) => d.ticker === ticker);
    }

    return NextResponse.json(data);
  } catch (err: any) {
    console.error(err);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
