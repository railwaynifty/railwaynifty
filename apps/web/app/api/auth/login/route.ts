import { NextRequest, NextResponse } from "next/server";
import { backendBase, internalHeaders, SESSION_COOKIE } from "@/lib/backend";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  const body = await request.text();
  const headers = internalHeaders({ "content-type": "application/json" });
  const forwardedFor = request.headers.get("x-forwarded-for") ?? request.headers.get("x-real-ip") ?? "";
  const userAgent = request.headers.get("user-agent") ?? "";
  if (forwardedFor) headers.set("x-client-ip", forwardedFor.split(",")[0].trim());
  if (userAgent) headers.set("x-client-user-agent", userAgent);
  const response = await fetch(`${backendBase()}/auth/login`, {
    method: "POST",
    headers,
    body,
    cache: "no-store",
  });
  const payload = await response.json().catch(() => ({ detail: "Invalid backend response" }));
  if (!response.ok) return NextResponse.json(payload, { status: response.status });

  const output = NextResponse.json({ user: payload.user });
  output.cookies.set({
    name: SESSION_COOKIE,
    value: payload.access_token,
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 7,
  });
  return output;
}
