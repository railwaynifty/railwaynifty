# V3 — Viewer Audit Log

This release adds an Admin-only Viewer activity audit trail.

## Recorded events

- Viewer login success/failure and logout
- replacement of an earlier single Viewer session
- dashboard opening
- dashboard tab/page selection
- button actions such as Refresh, Load, Use Current ATM and Reset
- symbol, date, expiry, strike and other filter changes
- client-side Excel/export actions and server download responses
- Admin disable/enable, password reset and session revocation affecting a Viewer

## Audit fields

- timestamp (displayed in IST)
- Viewer email and user ID
- event type, page/tab, action and control target
- sanitised selected values/query parameters
- IP address and browser user-agent
- result/status
- one-way session fingerprint (never the session ID or JWT)

Passwords, tokens, cookies, authorization headers and secrets are excluded.

## Administration

Open **Audit Log** from the Admin navigation. Filters and CSV export are available. Default retention is 90 days via `AUDIT_RETENTION_DAYS`.
