import { NextRequest } from "next/server";
import { proxyJson } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  return proxyJson(request, "/admin/archive/jobs", {
    method: "POST",
    body: await request.text(),
  });
}
