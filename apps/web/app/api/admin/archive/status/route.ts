import { NextRequest } from "next/server";
import { proxyJson } from "@/lib/backend";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function GET(request: NextRequest) {
  return proxyJson(request, "/admin/archive/status", { method: "GET" });
}
