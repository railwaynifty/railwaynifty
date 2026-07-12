import { NextRequest, NextResponse } from "next/server";
import { backendBase, internalHeaders, sessionToken } from "@/lib/backend";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function GET(request: NextRequest) {
  const token = sessionToken(request);
  if (!token) return NextResponse.json({ detail: "Not authenticated" }, { status: 401 });
  const query = request.nextUrl.searchParams.toString();
  const headers = internalHeaders();
  headers.set("authorization", `Bearer ${token}`);
  const forwardedFor = request.headers.get("x-forwarded-for") ?? request.headers.get("x-real-ip") ?? "";
  const userAgent = request.headers.get("user-agent") ?? "";
  if (forwardedFor) headers.set("x-client-ip", forwardedFor.split(",")[0].trim());
  if (userAgent) headers.set("x-client-user-agent", userAgent);

  const response = await fetch(`${backendBase()}/admin/audit/export${query ? `?${query}` : ""}`, {
    method: "GET", headers, cache: "no-store",
  });
  const outputHeaders = new Headers();
  outputHeaders.set("content-type", response.headers.get("content-type") ?? "text/csv; charset=utf-8");
  outputHeaders.set("cache-control", "no-store");
  const disposition = response.headers.get("content-disposition");
  if (disposition) outputHeaders.set("content-disposition", disposition);
  return new NextResponse(response.body, { status: response.status, headers: outputHeaders });
}
