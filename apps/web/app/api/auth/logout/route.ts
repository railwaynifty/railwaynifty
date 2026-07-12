import { NextRequest, NextResponse } from "next/server";
import { backendBase, internalHeaders, SESSION_COOKIE, sessionToken } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  const token = sessionToken(request);
  if (token) {
    const headers = internalHeaders();
    headers.set("authorization", `Bearer ${token}`);
    const forwardedFor = request.headers.get("x-forwarded-for") ?? request.headers.get("x-real-ip") ?? "";
    const userAgent = request.headers.get("user-agent") ?? "";
    if (forwardedFor) headers.set("x-client-ip", forwardedFor.split(",")[0].trim());
    if (userAgent) headers.set("x-client-user-agent", userAgent);
    await fetch(`${backendBase()}/auth/logout`, {
      method: "POST",
      headers,
      cache: "no-store",
    }).catch(() => undefined);
  }
  const response = NextResponse.json({ ok: true });
  response.cookies.set({ name: SESSION_COOKIE, value: "", path: "/", maxAge: 0 });
  return response;
}
