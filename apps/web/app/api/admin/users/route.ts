import { NextRequest } from "next/server";
import { proxyJson } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  return proxyJson(request, "/admin/users", { method: "GET" });
}
export async function POST(request: NextRequest) {
  return proxyJson(request, "/admin/users", { method: "POST", body: await request.text() });
}
