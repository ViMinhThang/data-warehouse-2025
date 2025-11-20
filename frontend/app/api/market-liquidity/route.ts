import { NextRequest, NextResponse } from "next/server";
import { fetchMarketLiquidity } from "@/services/dbService";

export async function GET(req: NextRequest) {
  try {
    const data = await fetchMarketLiquidity();
    return NextResponse.json(data);
  } catch (err: any) {
    console.error(err);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
