"use client";

import Link from "next/link";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

type Viewer = { id: number; email: string; role: "admin" | "viewer" };
type AuditEvent = {
  id: number;
  occurred_at: string;
  user_id: number | null;
  user_email: string;
  event_type: string;
  page: string;
  action: string;
  target: string;
  method: string;
  path: string;
  query_params: Record<string, unknown>;
  details: Record<string, unknown>;
  ip_address: string;
  user_agent: string;
  status_code: number | null;
  success: boolean;
  duration_ms: number | null;
  session_fingerprint: string | null;
};

function dateInput(daysAgo = 0) {
  const day = new Date();
  day.setDate(day.getDate() - daysAgo);
  return day.toISOString().slice(0, 10);
}

function compactObject(value: Record<string, unknown> | null | undefined) {
  if (!value || Object.keys(value).length === 0) return "";
  return JSON.stringify(value);
}

export default function AuditPage() {
  const router = useRouter();
  const [viewers, setViewers] = useState<Viewer[]>([]);
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [total, setTotal] = useState(0);
  const [dateFrom, setDateFrom] = useState(dateInput(7));
  const [dateTo, setDateTo] = useState(dateInput(0));
  const [userId, setUserId] = useState("");
  const [eventType, setEventType] = useState("");
  const [page, setPage] = useState("");
  const [query, setQuery] = useState("");
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const limit = 100;

  const params = useMemo(() => {
    const value = new URLSearchParams({ date_from: dateFrom, date_to: dateTo, limit: String(limit), offset: String(offset) });
    if (userId) value.set("user_id", userId);
    if (eventType) value.set("event_type", eventType);
    if (page) value.set("page", page);
    if (query) value.set("q", query);
    return value;
  }, [dateFrom, dateTo, eventType, offset, page, query, userId]);

  const exportParams = useMemo(() => {
    const value = new URLSearchParams(params);
    value.delete("limit");
    value.delete("offset");
    return value;
  }, [params]);

  const loadUsers = useCallback(async () => {
    const response = await fetch("/api/admin/users", { cache: "no-store" });
    if (response.status === 401 || response.status === 403) {
      router.replace("/dashboard");
      return;
    }
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || "Could not load users");
    setViewers((payload.users || []).filter((user: Viewer) => user.role === "viewer"));
  }, [router]);

  const loadAudit = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(`/api/admin/audit?${params.toString()}`, { cache: "no-store" });
      if (response.status === 401 || response.status === 403) {
        router.replace("/dashboard");
        return;
      }
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "Could not load audit log");
      setEvents(payload.events || []);
      setTotal(payload.total || 0);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not load audit log");
    } finally {
      setLoading(false);
    }
  }, [params, router]);

  useEffect(() => { loadUsers().catch((reason) => setError(reason.message)); }, [loadUsers]);
  useEffect(() => { loadAudit(); }, [loadAudit]);

  function applyFilters(event: FormEvent) {
    event.preventDefault();
    setOffset(0);
    loadAudit();
  }

  return (
    <main>
      <header className="topbar">
        <span className="brand">Viewer Audit Log</span>
        <Link href="/admin">Users</Link>
        <Link href="/archive">Archive</Link>
        <Link href="/dashboard">Dashboard</Link>
      </header>
      <div className="page">
        {error ? <div className="error">{error}</div> : null}
        <section className="panel">
          <div className="audit-title-row">
            <div>
              <h2>Viewer activity</h2>
              <p className="muted">Tracks login/logout, dashboard tabs, button actions, filter changes, exports, session replacement and administrator session actions.</p>
            </div>
            <a className="button secondary" href={`/api/admin/audit/export?${exportParams.toString()}`}>Export CSV</a>
          </div>
          <form className="grid audit-filters" onSubmit={applyFilters}>
            <div className="field"><label>From</label><input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} /></div>
            <div className="field"><label>To</label><input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} /></div>
            <div className="field"><label>Viewer</label><select value={userId} onChange={(e) => setUserId(e.target.value)}><option value="">All viewers</option>{viewers.map((viewer) => <option key={viewer.id} value={viewer.id}>{viewer.email}</option>)}</select></div>
            <div className="field"><label>Event</label><select value={eventType} onChange={(e) => setEventType(e.target.value)}><option value="">All events</option><option value="login_success">Login success</option><option value="login_failed">Login failed</option><option value="logout">Logout</option><option value="dashboard_open">Dashboard opened</option><option value="page_view">Tab/page viewed</option><option value="button_action">Button action</option><option value="filter_change">Filter changed</option><option value="export_action">Export action</option><option value="export_download">Export download</option><option value="dashboard_api_action">Dashboard API action</option><option value="session_revoked_by_admin">Session revoked</option><option value="password_reset_by_admin">Password reset</option><option value="account_status_changed">Account status</option></select></div>
            <div className="field"><label>Page contains</label><input value={page} onChange={(e) => setPage(e.target.value)} placeholder="e.g. Strike Full Day" /></div>
            <div className="field"><label>Search</label><input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="action, path or detail" /></div>
            <div className="field audit-filter-button"><button className="button" type="submit">Apply filters</button></div>
          </form>
        </section>

        <section className="panel">
          <div className="audit-title-row"><h2>{total.toLocaleString()} events</h2><button className="button secondary" onClick={loadAudit} disabled={loading}>{loading ? "Loading…" : "Refresh"}</button></div>
          <div className="audit-table-wrap">
            <table className="audit-table">
              <thead><tr><th>Time (IST)</th><th>Viewer</th><th>Event</th><th>Page / action</th><th>Selection / details</th><th>IP / device</th><th>Result</th></tr></thead>
              <tbody>
                {events.map((entry) => (
                  <tr key={entry.id}>
                    <td className="nowrap">{new Date(entry.occurred_at).toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })}</td>
                    <td><strong>{entry.user_email}</strong>{entry.session_fingerprint ? <><br /><small>Session {entry.session_fingerprint}</small></> : null}</td>
                    <td><span className={`badge ${entry.success ? "active" : "disabled"}`}>{entry.event_type}</span></td>
                    <td><strong>{entry.page || "—"}</strong><br /><span>{entry.action || "—"}</span>{entry.target ? <><br /><small>{entry.target}</small></> : null}</td>
                    <td><small>{compactObject(entry.details) || compactObject(entry.query_params) || "—"}</small>{entry.path ? <><br /><small>{entry.method} {entry.path}</small></> : null}</td>
                    <td><span>{entry.ip_address || "—"}</span><br /><small title={entry.user_agent}>{entry.user_agent ? entry.user_agent.slice(0, 90) : "—"}</small></td>
                    <td>{entry.success ? "Success" : "Failed"}{entry.status_code ? ` (${entry.status_code})` : ""}{entry.duration_ms !== null ? <><br /><small>{entry.duration_ms} ms</small></> : null}</td>
                  </tr>
                ))}
                {!loading && events.length === 0 ? <tr><td colSpan={7}>No audit events found for the selected filters.</td></tr> : null}
              </tbody>
            </table>
          </div>
          <div className="audit-pagination">
            <button className="button secondary" disabled={offset === 0 || loading} onClick={() => setOffset(Math.max(0, offset - limit))}>Previous</button>
            <span>Showing {total === 0 ? 0 : offset + 1}–{Math.min(total, offset + events.length)} of {total}</span>
            <button className="button secondary" disabled={offset + limit >= total || loading} onClick={() => setOffset(offset + limit)}>Next</button>
          </div>
        </section>
      </div>
    </main>
  );
}
