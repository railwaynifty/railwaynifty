import { NextRequest } from "next/server";
import { proxyJson } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const query = request.nextUrl.searchParams.toString();
  return proxyJson(request, `/admin/audit${query ? `?${query}` : ""}`, { method: "GET" });
}
