"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

type Me = { email: string; role: "admin" | "viewer" };

export default function DashboardPage() {
  const router = useRouter();
  const [me, setMe] = useState<Me | null>(null);

  useEffect(() => {
    let active = true;
    async function checkSession() {
      try {
        const response = await fetch("/api/auth/me", { cache: "no-store" });
        if (!response.ok) throw new Error("Session expired");
        const payload = await response.json();
        if (active) setMe(payload);
      } catch {
        if (active) router.replace("/login");
      }
    }
    checkSession();
    const timer = window.setInterval(checkSession, 15000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [router]);

  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    router.replace("/login");
    router.refresh();
  }

  return (
    <main>
      <header className="topbar">
        <span className="brand">NSE 360 Private</span>
        {me?.role === "admin" ? <><Link href="/audit">Audit Log</Link><Link href="/archive">Archive</Link><Link href="/admin">Users</Link></> : null}
        <span>{me?.email ?? "Loading…"}</span>
        <button className="button secondary" onClick={logout}>Logout</button>
      </header>
      <iframe className="frame" title="NSE 360 Dashboard" src="/api/dashboard/?v=cloud_private" />
    </main>
  );
}
