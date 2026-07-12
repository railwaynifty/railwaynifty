"use client";

import Link from "next/link";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

type StorageTable = {
  schema: string;
  table: string;
  date_column: string;
  min_date: string | null;
  max_date: string | null;
  row_count: number;
  size_bytes: number;
};

type ArchiveJob = {
  id: number;
  destination: "local" | "gdrive";
  date_from: string;
  date_to: string;
  purge_after: boolean;
  compact_mode: "none" | "vacuum" | "full";
  status: string;
  file_name?: string | null;
  sha256?: string | null;
  drive_md5?: string | null;
  row_count: number;
  table_count: number;
  drive_web_view_link?: string | null;
  download_url?: string | null;
  error?: string | null;
  created_at: string;
  completed_at?: string | null;
  downloaded_at?: string | null;
  purged_at?: string | null;
};

type ArchiveStatus = {
  storage: {
    total_size_bytes: number;
    total_rows: number;
    tables: StorageTable[];
  };
  jobs: ArchiveJob[];
  google_drive_configured: boolean;
  auto_archive_enabled: boolean;
  retention_days: number;
  auto_purge: boolean;
};

function bytes(value: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = Number(value || 0);
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index < 2 ? 0 : 2)} ${units[index]}`;
}

function number(value: number): string {
  return new Intl.NumberFormat("en-IN").format(Number(value || 0));
}

function day(value?: string | null): string {
  if (!value) return "—";
  return String(value).slice(0, 10);
}

const activeStatuses = new Set(["queued", "retry", "running", "purge_queued", "purging"]);

export default function ArchivePage() {
  const router = useRouter();
  const [data, setData] = useState<ArchiveStatus | null>(null);
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [destination, setDestination] = useState<"local" | "gdrive">("local");
  const [purgeAfter, setPurgeAfter] = useState(false);
  const [compactMode, setCompactMode] = useState<"none" | "vacuum" | "full">("vacuum");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    const response = await fetch("/api/admin/archive/status", { cache: "no-store" });
    if (response.status === 401 || response.status === 403) {
      router.replace("/dashboard");
      return;
    }
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "Could not load archive status");
    setData(payload);
    const tables = (payload.storage?.tables || []) as StorageTable[];
    const mins = tables.map((item) => item.min_date).filter(Boolean).sort() as string[];
    const maxs = tables.map((item) => item.max_date).filter(Boolean).sort() as string[];
    setDateFrom((current) => current || (mins[0] ? day(mins[0]) : ""));
    setDateTo((current) => current || (maxs.length ? day(maxs[maxs.length - 1]) : ""));
  }, [router]);

  useEffect(() => {
    load().catch((err) => setError(err instanceof Error ? err.message : "Could not load archive status"));
  }, [load]);

  const hasActiveJob = useMemo(
    () => Boolean(data?.jobs.some((job) => activeStatuses.has(job.status))),
    [data],
  );

  useEffect(() => {
    if (!hasActiveJob) return;
    const timer = window.setInterval(() => {
      load().catch(() => undefined);
    }, 5000);
    return () => window.clearInterval(timer);
  }, [hasActiveJob, load]);

  async function createJob(event: FormEvent) {
    event.preventDefault();
    setError("");
    if (!dateFrom || !dateTo) return setError("Select both dates.");
    if (destination === "gdrive" && !data?.google_drive_configured) {
      return setError("Google Drive is not configured on Railway.");
    }
    setBusy(true);
    try {
      const response = await fetch("/api/admin/archive/jobs", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          date_from: dateFrom,
          date_to: dateTo,
          destination,
          purge_after: destination === "gdrive" ? purgeAfter : false,
          compact_mode: compactMode,
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.detail || "Could not start archive job");
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not start archive job");
    } finally {
      setBusy(false);
    }
  }

  async function purge(job: ArchiveJob) {
    const compactText = job.compact_mode === "full"
      ? "VACUUM FULL will lock affected tables while compacting and can take time."
      : "The deleted space will become reusable inside PostgreSQL.";
    if (!window.confirm(`Delete archived rows for ${job.date_from} through ${job.date_to}?\n\n${compactText}\n\nThis cannot be undone.`)) return;
    setError("");
    const response = await fetch(`/api/admin/archive/jobs/${job.id}/purge`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ force: false }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return setError(payload.detail || "Could not queue purge");
    await load();
  }

  async function retry(job: ArchiveJob) {
    setError("");
    const response = await fetch(`/api/admin/archive/jobs/${job.id}/retry`, { method: "POST" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return setError(payload.detail || "Could not retry job");
    await load();
  }

  return (
    <main>
      <header className="topbar">
        <span className="brand">Data Archive & Railway Storage</span>
        <Link href="/audit">Audit Log</Link>
        <Link href="/dashboard">Dashboard</Link>
        <Link href="/admin">Users</Link>
      </header>
      <div className="page wide-page">
        {error ? <div className="error">{error}</div> : null}

        <section className="summary-grid">
          <div className="metric-card"><span>Market tables</span><strong>{data?.storage.tables.length ?? 0}</strong></div>
          <div className="metric-card"><span>Estimated rows</span><strong>{number(data?.storage.total_rows ?? 0)}</strong></div>
          <div className="metric-card"><span>PostgreSQL table size</span><strong>{bytes(data?.storage.total_size_bytes ?? 0)}</strong></div>
          <div className="metric-card"><span>Google Drive</span><strong>{data?.google_drive_configured ? "Configured" : "Not configured"}</strong></div>
        </section>

        <section className="panel">
          <h2>Create archive</h2>
          <p className="muted compact-copy">
            Local export prepares a ZIP for direct browser download. Google Drive upload can verify the upload and then remove the archived rows automatically.
          </p>
          <form className="grid archive-form" onSubmit={createJob}>
            <div className="field"><label>From date</label><input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} required /></div>
            <div className="field"><label>To date</label><input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} required /></div>
            <div className="field"><label>Destination</label><select value={destination} onChange={(event) => setDestination(event.target.value as "local" | "gdrive")}>
              <option value="local">Download locally as ZIP</option>
              <option value="gdrive" disabled={!data?.google_drive_configured}>Google Drive</option>
            </select></div>
            <div className="field"><label>After deletion</label><select value={compactMode} onChange={(event) => setCompactMode(event.target.value as "none" | "vacuum" | "full")}>
              <option value="vacuum">VACUUM — reuse database space</option>
              <option value="full">VACUUM FULL — release physical disk</option>
              <option value="none">No compaction</option>
            </select></div>
            {destination === "gdrive" ? <label className="check-row"><input type="checkbox" checked={purgeAfter} onChange={(event) => setPurgeAfter(event.target.checked)} /> Delete rows only after successful Drive upload</label> : null}
            <div className="field action-field"><button className="button" disabled={busy || hasActiveJob}>{busy ? "Starting…" : destination === "local" ? "Prepare ZIP" : "Upload to Google Drive"}</button></div>
          </form>
          <div className="notice warning-notice">
            <strong>Safe deletion:</strong> local exports are never deleted automatically. Download the ZIP, then use the job’s <em>Purge</em> button. A row-count safety check stops deletion if data changed after export.
          </div>
          <div className="notice">
            Regular VACUUM makes deleted space reusable by PostgreSQL. Choose VACUUM FULL only outside market hours when you need the physical Railway database size reduced; it temporarily locks each affected table.
          </div>
        </section>

        <section className="panel">
          <h2>Archive jobs</h2>
          <div className="table-scroll">
            <table>
              <thead><tr><th>ID</th><th>Range</th><th>Destination</th><th>Status</th><th>Rows</th><th>File / checksum</th><th>Created</th><th>Actions</th></tr></thead>
              <tbody>
                {(data?.jobs || []).map((job) => (
                  <tr key={job.id}>
                    <td>#{job.id}</td>
                    <td>{day(job.date_from)} → {day(job.date_to)}<br /><small>{job.compact_mode}</small></td>
                    <td>{job.destination === "gdrive" ? "Google Drive" : "Local ZIP"}</td>
                    <td><span className={`badge status-${job.status}`}>{job.status.replaceAll("_", " ")}</span>{job.error ? <div className="job-error">{job.error}</div> : null}</td>
                    <td>{number(job.row_count)}<br /><small>{job.table_count} tables</small></td>
                    <td><span className="file-name">{job.file_name || "—"}</span>{job.sha256 ? <><br /><code title={job.sha256}>{job.sha256.slice(0, 16)}…</code></> : null}</td>
                    <td>{new Date(job.created_at).toLocaleString()}</td>
                    <td><div className="actions">
                      {job.download_url ? <a className="button link-button" href={job.download_url}>Download ZIP</a> : null}
                      {job.drive_web_view_link ? <a className="button link-button" href={job.drive_web_view_link} target="_blank" rel="noreferrer">Open Drive</a> : null}
                      {(["ready", "downloaded", "completed"].includes(job.status)) ? <button className="button danger" onClick={() => purge(job)}>Purge</button> : null}
                      {(["failed", "purge_failed"].includes(job.status)) ? <button className="button secondary" onClick={() => retry(job)}>Retry</button> : null}
                    </div></td>
                  </tr>
                ))}
                {!data?.jobs.length ? <tr><td colSpan={8}>No archive jobs yet.</td></tr> : null}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel">
          <h2>Storage by table</h2>
          <div className="table-scroll">
            <table>
              <thead><tr><th>Schema</th><th>Table</th><th>Date field</th><th>Available dates</th><th>Rows</th><th>Size</th></tr></thead>
              <tbody>{(data?.storage.tables || []).map((item) => <tr key={`${item.schema}.${item.table}`}>
                <td>{item.schema}</td><td><code>{item.table}</code></td><td>{item.date_column}</td><td>{day(item.min_date)} → {day(item.max_date)}</td><td>{number(item.row_count)}</td><td>{bytes(item.size_bytes)}</td>
              </tr>)}</tbody>
            </table>
          </div>
        </section>

        <section className="panel">
          <h2>Automatic retention</h2>
          <p>
            Automatic Google Drive archiving is <strong>{data?.auto_archive_enabled ? "enabled" : "disabled"}</strong>. Retention is {data?.retention_days ?? 30} days and automatic purge is <strong>{data?.auto_purge ? "enabled" : "disabled"}</strong>.
          </p>
          <p className="muted compact-copy">These settings are controlled by Railway environment variables so a Viewer cannot change retention or deletion policy.</p>
        </section>
      </div>
    </main>
  );
}
