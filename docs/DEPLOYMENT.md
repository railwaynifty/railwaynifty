# Deployment Guide

## 1. Create the private GitHub repository

Create one private repository, for example `nse360-private-cloud`, and upload the complete contents of this monorepo.

Do not upload a real `.env` file. Only `.env.example` belongs in GitHub.

## 2. Generate shared secrets

Run locally:

```bash
python scripts/generate_secrets.py
```

Save both values securely:

- `JWT_SECRET`
- `INTERNAL_PROXY_KEY`

`JWT_SECRET` is configured only on the Railway backend. `INTERNAL_PROXY_KEY` must use the same value on the Railway backend and Vercel frontend.

## 3. Railway project and PostgreSQL

1. Create a new Railway project.
2. Add a PostgreSQL database.
3. Copy/reference its `DATABASE_URL` into all three services.
4. Historical market data is intentionally not imported. The database starts fresh.

The application automatically creates:

- `public.app_users`
- `public.archive_jobs`
- `public.viewer_audit_logs`
- `options` schema
- `futures` schema
- `cash` schema

## 4. Railway backend service

Create a service from the same GitHub repository.

Settings:

- Root Directory: `/services/backend`
- Config file path, when required: `/services/backend/railway.json`
- Generate a public Railway domain.
- Keep one backend replica unless archive jobs are moved to a dedicated queue worker.

Required variables:

```text
DATABASE_URL=<Railway PostgreSQL DATABASE_URL>
JWT_SECRET=<generated secret, minimum 32 characters>
INTERNAL_PROXY_KEY=<generated secret, minimum 32 characters>
INITIAL_ADMIN_EMAIL=<your admin email>
INITIAL_ADMIN_PASSWORD=<strong temporary password, minimum 10 characters>
SESSION_DAYS=7
APP_DATA_DIR=/app/data
SCHEMA_OPTIONS=options
SCHEMA_FUTURES=futures
SCHEMA_CASH=cash
ARCHIVE_TEMP_DIR=/tmp/nse360_archives
ARCHIVE_LOCAL_TTL_HOURS=6
AUDIT_RETENTION_DAYS=90
```

Optional automatic retention variables:

```text
ARCHIVE_AUTO_ENABLED=false
ARCHIVE_AUTO_HOUR_IST=21
ARCHIVE_RETENTION_DAYS=30
ARCHIVE_AUTO_PURGE=false
ARCHIVE_COMPACT_MODE=vacuum
```

Health check:

```text
/health
```

The backend domain is public because Vercel and temporary signed archive downloads must reach it. Normal API/dashboard requests without `INTERNAL_PROXY_KEY` are rejected. Archive download URLs are signed, expire after 30 minutes and work only while the temporary ZIP exists.

## 5. Google Drive configuration (optional)

The **Archive** page can always prepare a local ZIP. Google Drive requires a folder ID plus one of the authentication methods below.

### Option A — service account

Configure:

```text
GOOGLE_DRIVE_FOLDER_ID=<destination folder ID>
GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON=<raw JSON or base64-encoded service-account JSON>
```

A Google Shared Drive folder is preferred. Give the service account permission to add files to the destination folder.

### Option B — normal Google account OAuth

Configure:

```text
GOOGLE_DRIVE_FOLDER_ID=<destination folder ID>
GOOGLE_DRIVE_CLIENT_ID=<OAuth client ID>
GOOGLE_DRIVE_CLIENT_SECRET=<OAuth client secret>
GOOGLE_DRIVE_REFRESH_TOKEN=<offline refresh token>
```

Use only one authentication option. Store credentials only in Railway variables, never in GitHub.

### Automatic archive recommendation

Start with:

```text
ARCHIVE_AUTO_ENABLED=true
ARCHIVE_RETENTION_DAYS=30
ARCHIVE_AUTO_PURGE=false
ARCHIVE_COMPACT_MODE=vacuum
```

Confirm several Drive archives and checksums first. Then enable automatic deletion:

```text
ARCHIVE_AUTO_PURGE=true
```

`vacuum` makes deleted space reusable inside PostgreSQL. It may not lower the physical Railway disk figure immediately. `full` performs `VACUUM FULL`, which rewrites and locks affected tables and may require temporary free disk roughly comparable to the table being compacted. Use `full` only outside market hours and only when enough free storage exists.

## 6. Railway live worker

Create another service from the same repository.

Settings:

- Root Directory: `/services/live-worker`
- Config file path, when required: `/services/live-worker/railway.json`
- Do not generate a public domain.

Variables:

```text
DATABASE_URL=<same Railway PostgreSQL DATABASE_URL>
APP_DATA_DIR=/app/data
SCHEMA_OPTIONS=options
SCHEMA_FUTURES=futures
SCHEMA_CASH=cash
```

The worker remains alive but fetches only:

- Monday to Friday
- 09:14 through 15:50 IST
- NIFTY only

It also calls the integrated NIFTY 50 cash-money-flow module after every derivatives cycle.

## 7. Railway EOD cron worker

Create the third Railway service.

Settings:

- Root Directory: `/services/eod-worker`
- Config file path, when required: `/services/eod-worker/railway.json`
- Do not generate a public domain.
- Cron Schedule: `30 14 * * 1-5`

Railway evaluates cron in UTC. `14:30 UTC` equals `20:00 IST` throughout the year.

Variables:

```text
DATABASE_URL=<same Railway PostgreSQL DATABASE_URL>
APP_DATA_DIR=/app/data
SCHEMA_OPTIONS=options
SCHEMA_FUTURES=futures
SCHEMA_CASH=cash
```

The cron service must finish and exit. It is not an always-on service.

## 8. Vercel frontend

Create one Vercel project from the same GitHub repository.

Settings:

- Root Directory: `apps/web`
- Framework: Next.js

Variables:

```text
BACKEND_URL=https://<your-backend>.up.railway.app
INTERNAL_PROXY_KEY=<same value used by Railway backend>
```

`JWT_SECRET` and Google Drive credentials remain only in Railway.

Deploy and open the Vercel URL.

## 9. First login, users and single-session Viewer policy

1. Log in with `INITIAL_ADMIN_EMAIL` and `INITIAL_ADMIN_PASSWORD`.
2. Open **Users**.
3. Create Viewer accounts.
4. Reset the initial admin password.
5. Remove `INITIAL_ADMIN_PASSWORD` from Railway after the first admin has been created.

A Viewer can have only one active session. When that Viewer logs in on another browser/device, the older session is invalidated. The Users page shows whether a Viewer session is active and provides a **Sign out session** action.

Public signup is not implemented.


## 10. Viewer audit log

Open **Audit Log** as Admin. You can filter by:

- From/To date
- Viewer account
- event type
- dashboard page/tab
- free-text action/detail search

The page shows the event in IST, Viewer email, page/action, selected filter values, IP, browser/device user-agent, result and a one-way session fingerprint. Use **Export CSV** to download the currently filtered records.

Events include login success/failure, logout, dashboard open, tab/page view, button actions, symbol/date/expiry/strike filter changes, Excel/download actions, session replacement, Admin session revocation, password reset and enable/disable actions. Passwords, tokens, cookies and secrets are not captured.

Retention is controlled by:

```text
AUDIT_RETENTION_DAYS=90
```

Old entries are removed by the backend daily and at startup. This table is not part of market-data archive ZIPs and is not deleted by market-data purge.

## 11. Archive usage

### Local ZIP

1. Open **Archive** as Admin.
2. Select From and To dates.
3. Choose **Download locally as ZIP**.
4. Wait until status becomes `ready`.
5. Download and verify the ZIP.
6. Click **Purge** only after preserving the file safely.

The ZIP contains a CSV for each matching market table, `manifest.json`, row counts and SHA-256. The purge is refused if the database row counts changed after export.

### Google Drive

1. Configure Drive variables in Railway.
2. Choose **Google Drive** on the Archive page.
3. Optionally select **Delete rows only after successful Drive upload**.
4. The job uploads by resumable upload and stores the Drive link and checksum metadata.

## 12. Verification checklist

### Backend

- `/health` returns `{"ok":true,"legacy":true}`.
- Direct `/auth/login` without the internal proxy key returns 403.
- Vercel login works.
- A second Viewer login invalidates the first browser session.
- Admin can revoke an active Viewer session.
- Viewer login, dashboard tabs, button/filter actions and logout appear in **Audit Log**.
- Audit CSV export downloads the filtered rows.

### Live worker

During market hours, logs should show:

- NIFTY option expiry selection
- futures upsert
- options upsert
- cash-money-flow summary

After the first successful cycle, PostgreSQL should contain:

- `options."NIFTY"`
- `futures."NIFTY"`
- `cash.nifty50_cash_flow_1m`
- `cash.nifty50_cash_flow_summary_1m`

### Dashboard

- All existing tabs open through the Vercel dashboard.
- Browser developer tools should show requests under `/api/dashboard/api/...`.
- Opening the Railway legacy path directly should fail without Vercel's internal key and user token.

### EOD

After 8 PM IST, the EOD service logs should show download, raw import and processed result-table build stages.

### Archive

- Storage table list is visible to Admin only.
- A one-day local archive reaches `ready` and downloads as ZIP.
- `manifest.json` row counts match the CSVs.
- Purge is refused if rows changed after export.
- Google Drive upload returns a usable Drive link when configured.

## 13. NSE cloud-access warning

NSE endpoints use anti-bot/session controls. The code automatically primes sessions, refreshes cookies and tries endpoint fallbacks, but NSE can still restrict a cloud-provider IP. Test the Railway live-worker logs during a market session before treating the deployment as production-ready.

If Railway's outbound IP is blocked, the application code remains valid, but the fetch worker will need an allowed/static outbound route or a different hosting location for the NSE collector.
