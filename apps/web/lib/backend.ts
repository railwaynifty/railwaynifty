import { NextRequest, NextResponse } from "next/server";

export const SESSION_COOKIE = "nse360_session";

export function backendBase(): string {
  const value = process.env.BACKEND_URL?.replace(/\/$/, "");
  if (!value) throw new Error("BACKEND_URL is not configured");
  return value;
}

export function internalHeaders(extra: HeadersInit = {}): Headers {
  const headers = new Headers(extra);
  const key = process.env.INTERNAL_PROXY_KEY;
  if (!key) throw new Error("INTERNAL_PROXY_KEY is not configured");
  headers.set("x-internal-key", key);
  return headers;
}

export function sessionToken(request: NextRequest): string | null {
  return request.cookies.get(SESSION_COOKIE)?.value ?? null;
}

export async function proxyJson(
  request: NextRequest,
  path: string,
  options: RequestInit = {},
): Promise<NextResponse> {
  const headers = internalHeaders(options.headers);
  const forwardedFor = request.headers.get("x-forwarded-for") ?? request.headers.get("x-real-ip") ?? "";
  const userAgent = request.headers.get("user-agent") ?? "";
  if (forwardedFor) headers.set("x-client-ip", forwardedFor.split(",")[0].trim());
  if (userAgent) headers.set("x-client-user-agent", userAgent);
  const token = sessionToken(request);
  if (token) headers.set("authorization", `Bearer ${token}`);
  if (!headers.has("content-type") && options.body) {
    headers.set("content-type", "application/json");
  }

  const response = await fetch(`${backendBase()}${path}`, {
    ...options,
    headers,
    cache: "no-store",
  });
  const text = await response.text();
  return new NextResponse(text, {
    status: response.status,
    headers: { "content-type": response.headers.get("content-type") ?? "application/json" },
  });
}
