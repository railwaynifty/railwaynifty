# Architecture

```text
Browser
  |
  | HTTPS + HttpOnly session cookie
  v
Vercel: apps/web
  |- Login and logout
  |- Admin/Viewer interface
  |- User and active-session administration
  |- Admin-only Viewer audit-log search and CSV export
  |- Data archive/storage administration
  |- Protected dashboard iframe
  `- Server-side proxy adds X-Internal-Key + Bearer session
        |
        v
Railway: services/backend
  |- FastAPI authentication gateway
  |- Viewer single-session enforcement
  |- app_users + viewer_audit_logs + archive_jobs in PostgreSQL public schema
  |- 7-day JWT validation
  |- Local ZIP / Google Drive archive processor
  `- Private localhost proxy to legacy dashboard on port 8102
        |
        v
Railway PostgreSQL
  |- public.app_users          (never archived)
  |- public.viewer_audit_logs   (Viewer activity; never market-archived)
  |- public.archive_jobs       (archive audit trail)
  |- options.*              (NIFTY options + EOD analytics/cache)
  |- futures.*              (NIFTY futures)
  `- cash.*                 (NIFTY 50 cash money flow)

Railway: services/live-worker
  |- Runs continuously
  |- Fetches only Mon-Fri 09:14-15:50 IST
  |- NIFTY current + next expiry options
  |- NIFTY current-expiry futures
  `- Integrated NIFTY 50 cash money flow

Railway: services/eod-worker
  |- Cron starts at 14:30 UTC / 20:00 IST, Mon-Fri
  |- Downloads NSE reports
  |- Imports raw EOD tables
  |- Builds processed decision tables and dashboard cache
  `- Exits when complete
```

## Viewer single-session behavior

A Viewer login creates a random server-tracked session identifier. PostgreSQL stores only its SHA-256 digest. A new successful login replaces that digest, so any older Viewer token fails immediately. Admin accounts may use multiple sessions. Admin can also revoke a Viewer session from the Users page.


## Viewer audit behavior

Viewer activity is recorded at two layers:

1. Server-trusted events: successful/failed Viewer login, logout, dashboard opening, export responses and session/account administration.
2. Browser interaction events: selected dashboard tab, button action, filter/date/expiry/strike changes and client-side Excel exports.

Each row records the Viewer, UTC timestamp (shown as IST in the UI), event/page/action, sanitised selection details, IP address, browser user-agent, status and only a SHA-256-derived session fingerprint. Passwords, tokens, cookies and secrets are filtered out. Admin activity is not included unless it changes a Viewer account/session.

The audit page is Admin-only. `AUDIT_RETENTION_DAYS` defaults to 90 days.

## Archive workflow

1. Admin selects a market-data date range.
2. Backend streams each matching table to CSV inside a ZIP file.
3. ZIP contains `manifest.json` with table names, columns, row counts and date range.
4. Backend calculates SHA-256.
5. Local destination: a short-lived signed Railway download link is returned.
6. Google Drive destination: the ZIP is uploaded by resumable upload and Drive metadata is recorded.
7. Deletion is allowed only after an archive exists. Before deleting, row counts are compared with the manifest.
8. `VACUUM` makes space reusable inside PostgreSQL; `VACUUM FULL` rewrites and physically compacts affected tables.

Local ZIP files use Railway temporary storage and expire automatically. The ZIP itself is not retained in PostgreSQL.

## Why the original dashboard is retained

The uploaded dashboard contains all existing tabs, calculations, HTML, JavaScript and API handlers in one large Python file. Rewriting it into React would risk changing calculations and would require a much larger validation exercise. The cloud package therefore:

1. makes its paths and database connections cloud-safe;
2. limits the symbol universe to NIFTY;
3. runs it only on Railway localhost;
4. places a FastAPI authentication gateway in front of it; and
5. exposes it through an authenticated Vercel proxy.

This preserves the current screen and calculations while adding private user access.
