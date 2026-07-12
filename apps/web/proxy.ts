import { NextRequest, NextResponse } from "next/server";

export function proxy(request: NextRequest) {
  const token = request.cookies.get("nse360_session")?.value;
  const path = request.nextUrl.pathname;
  if (!token && (path.startsWith("/dashboard") || path.startsWith("/admin") || path.startsWith("/archive") || path.startsWith("/audit"))) {
    return NextResponse.redirect(new URL("/login", request.url));
  }
  if (token && path === "/login") {
    return NextResponse.redirect(new URL("/dashboard", request.url));
  }
  return NextResponse.next();
}

export const config = { matcher: ["/dashboard/:path*", "/admin/:path*", "/archive/:path*", "/audit/:path*", "/login"] };
