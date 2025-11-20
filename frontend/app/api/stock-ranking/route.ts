import { NextRequest, NextResponse } from "next/server";
import { fetchRankingSnapshot } from "@/services/dbService";

export async function GET(req: NextRequest) {
  try {
    const data = await fetchRankingSnapshot();
    return NextResponse.json(data);
  } catch (err: any) {
    console.error(err);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
