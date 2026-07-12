# Validation completed before packaging

- Python syntax compilation passed for backend, archive module, legacy dashboard, live worker, cash-flow module and EOD worker.
- Backend imports passed in a clean Python virtual environment using the pinned requirements, including Google Drive libraries.
- Next.js 16.2.10 production build passed, including authentication, active Viewer session administration, archive administration and dashboard-proxy routes.
- `npm audit` reported zero known dependency vulnerabilities at packaging time.
- Repository scan found no remaining fixed Windows paths in deployable source files.
- User-facing symbol selectors and worker defaults are restricted to NIFTY.
- EOD Railway cron is documented as `30 14 * * 1-5`, corresponding to 8:00 PM IST.
- Viewer accounts are server-tracked and limited to one active session.
- Archive scope excludes the `public` schema, so user credentials are not exported.

## Deployment validation still required

External NSE calls, Railway PostgreSQL archive execution, Google Drive upload and live Railway/Vercel deployment cannot be fully tested without the owner's cloud accounts, a Railway PostgreSQL instance and market-session access. After deployment, follow the verification checklist in `DEPLOYMENT.md`.

## Viewer audit additions

- Python compilation includes `services/backend/app/audit.py`.
- Next.js production build includes `/audit`, `/api/audit/event`, `/api/admin/audit` and `/api/admin/audit/export`.
- Audit data excludes passwords, JWTs, cookies and secrets.
- Viewer interaction logging is injected only into authenticated proxied dashboard HTML.
- Audit records are retained separately from market-data archive/purge tables.
