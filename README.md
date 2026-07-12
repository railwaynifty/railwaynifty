# NSE 360 Cloud — NIFTY Private Dashboard

Private NIFTY-only cloud version of the local NSE 360 dashboard.

## Architecture

- `apps/web`: Next.js frontend on Vercel. Provides login, Admin/Viewer access, user management, Viewer audit-log review, data archive controls and a protected proxy for the existing dashboard UI.
- `services/backend`: FastAPI authentication/API gateway on Railway. It starts the existing dashboard locally, enforces sessions and runs archive/export jobs.
- `services/live-worker`: Railway always-on worker. Fetches NIFTY current/next-expiry options, futures and integrated NIFTY-50 cash money flow during market hours.
- `services/eod-worker`: Railway cron service. Downloads and processes NSE EOD reports at 8:00 PM IST.
- Railway PostgreSQL: one database with separate `options`, `futures`, and `cash` schemas.

The existing dashboard code is retained so all tabs and calculations remain available without rebuilding the large interface from scratch.

## Access model

- No public registration.
- Roles: `admin` and `viewer`.
- Seven-day signed sessions.
- A Viewer account has **only one active session at a time**. A new successful Viewer login invalidates the older Viewer session.
- Admin can create users, enable/disable users, reset passwords and force-sign-out a Viewer session.
- Admin has a searchable **Viewer Audit Log** showing login/logout, dashboard tabs, button actions, filter selections, exports, session replacement and account/session administration events.
- Railway backend rejects normal direct traffic unless the Vercel proxy sends `INTERNAL_PROXY_KEY`.


## Viewer audit log

The backend stores Viewer activity in `public.viewer_audit_logs`. The Admin-only **Audit Log** page supports date, Viewer, event, page and free-text filters, plus CSV export. Logged context includes IST timestamp, Viewer email, page/tab, action, selected filter value, IP address, browser user-agent, result and a one-way session fingerprint. Passwords, JWTs, cookies and secrets are never logged.

The default retention is 90 days and is configurable with `AUDIT_RETENTION_DAYS`. The audit table is intentionally excluded from market-data archive ZIPs and market-data purge operations.

## Data archive and Railway storage

Admin users have an **Archive** page with:

- date-range export to a local ZIP download;
- upload to Google Drive;
- SHA-256 archive checksum, row counts and a JSON manifest;
- delete-after-verified-Drive-upload option;
- a separate, confirmed purge action for local downloads;
- row-count safety checks before deletion;
- `VACUUM` for reusable PostgreSQL space; and
- optional `VACUUM FULL` to compact physical table files and reduce Railway disk usage.

The archive includes only market schemas (`options`, `futures`, `cash`). Authentication data in `public.app_users` is never included.

Optional automatic retention can archive data older than a configurable number of days to Google Drive after 9:00 PM IST. Automatic deletion is disabled by default.

## Deployment summary

Detailed steps: [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md)

1. Push this folder to one private GitHub repository.
2. Create Railway PostgreSQL.
3. Create three Railway services from the same repository:
   - Backend root: `/services/backend`
   - Live worker root: `/services/live-worker`
   - EOD worker root: `/services/eod-worker`
4. Configure EOD service Cron Schedule as `30 14 * * 1-5` (14:30 UTC = 20:00 IST).
5. Create one Vercel project with root `/apps/web`.
6. Set matching secrets in Railway and Vercel.
7. Optionally configure Google Drive credentials on the Railway backend.

## First login

The backend creates the initial admin only when no admin exists. Values come from:

- `INITIAL_ADMIN_EMAIL`
- `INITIAL_ADMIN_PASSWORD`

After the first login, create Viewer accounts from **Admin → Users**, then reset the initial password.
