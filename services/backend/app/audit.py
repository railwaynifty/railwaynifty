from __future__ import annotations

import csv
import io
import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import Request
from sqlalchemy import text

from .auth import session_hash
from .database import ENGINE

MAX_TEXT = 500


def _clean_text(value: Any, limit: int = MAX_TEXT) -> str:
    text_value = str(value or "").strip()
    return text_value[:limit]


def _safe_json(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    safe: dict[str, Any] = {}
    for key, item in value.items():
        key_text = _clean_text(key, 100)
        if not key_text or key_text.lower() in {"password", "token", "authorization", "cookie", "secret"}:
            continue
        if isinstance(item, (str, int, float, bool)) or item is None:
            safe[key_text] = _clean_text(item, 500) if isinstance(item, str) else item
        elif isinstance(item, (list, tuple)):
            safe[key_text] = [_clean_text(entry, 200) for entry in list(item)[:20]]
        elif isinstance(item, dict):
            safe[key_text] = {str(k)[:100]: _clean_text(v, 200) for k, v in list(item.items())[:20]}
        else:
            safe[key_text] = _clean_text(item, 500)
    return safe


def init_audit_database() -> None:
    with ENGINE.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS viewer_audit_logs (
                id bigserial PRIMARY KEY,
                occurred_at timestamptz NOT NULL DEFAULT now(),
                user_id bigint REFERENCES app_users(id) ON DELETE SET NULL,
                user_email text NOT NULL,
                role text NOT NULL DEFAULT 'viewer',
                session_fingerprint text,
                event_type text NOT NULL,
                page text,
                action text,
                target text,
                method text,
                path text,
                query_params jsonb NOT NULL DEFAULT '{}'::jsonb,
                details jsonb NOT NULL DEFAULT '{}'::jsonb,
                ip_address text,
                user_agent text,
                status_code integer,
                success boolean NOT NULL DEFAULT true,
                duration_ms integer,
                created_at timestamptz NOT NULL DEFAULT now()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_viewer_audit_time ON viewer_audit_logs (occurred_at DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_viewer_audit_user_time ON viewer_audit_logs (user_id, occurred_at DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_viewer_audit_event_time ON viewer_audit_logs (event_type, occurred_at DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_viewer_audit_page_time ON viewer_audit_logs (page, occurred_at DESC)"))


def request_client_ip(request: Request) -> str:
    for header in ("x-client-ip", "x-forwarded-for", "x-real-ip"):
        value = request.headers.get(header, "").split(",")[0].strip()
        if value:
            return _clean_text(value, 100)
    return _clean_text(request.client.host if request.client else "unknown", 100)


def request_user_agent(request: Request) -> str:
    return _clean_text(
        request.headers.get("x-client-user-agent") or request.headers.get("user-agent") or "",
        500,
    )


def request_query_params(request: Request) -> dict[str, Any]:
    blocked = {"password", "token", "access_token", "authorization", "cookie", "secret"}
    result: dict[str, Any] = {}
    for key, value in request.query_params.multi_items():
        if key.lower() in blocked:
            continue
        clean_key = _clean_text(key, 100)
        clean_value = _clean_text(value, 300)
        if clean_key in result:
            existing = result[clean_key]
            if isinstance(existing, list):
                if len(existing) < 20:
                    existing.append(clean_value)
            else:
                result[clean_key] = [existing, clean_value]
        else:
            result[clean_key] = clean_value
    return result


def session_fingerprint_from_payload(payload: dict[str, Any] | None) -> str | None:
    session_id = str((payload or {}).get("sid") or "")
    if not session_id:
        return None
    return session_hash(session_id)[:20]


def write_audit_event(
    *,
    user: dict[str, Any],
    request: Request | None,
    event_type: str,
    page: str = "",
    action: str = "",
    target: str = "",
    method: str = "",
    path: str = "",
    details: dict[str, Any] | None = None,
    query_params: dict[str, Any] | None = None,
    status_code: int | None = None,
    success: bool = True,
    duration_ms: int | None = None,
    payload: dict[str, Any] | None = None,
    force: bool = False,
) -> None:
    """Write a viewer audit event. Admin events are skipped unless force=True."""
    role = str(user.get("role") or "")
    if role != "viewer" and not force:
        return
    email = _clean_text(user.get("email"), 320)
    if not email:
        return

    ip_address = request_client_ip(request) if request is not None else ""
    user_agent = request_user_agent(request) if request is not None else ""
    request_path = path or (request.url.path if request is not None else "")
    request_method = method or (request.method if request is not None else "")
    params = query_params if query_params is not None else (request_query_params(request) if request is not None else {})

    with ENGINE.begin() as conn:
        conn.execute(text("""
            INSERT INTO viewer_audit_logs (
                user_id, user_email, role, session_fingerprint,
                event_type, page, action, target, method, path,
                query_params, details, ip_address, user_agent,
                status_code, success, duration_ms
            ) VALUES (
                :user_id, :user_email, :role, :session_fingerprint,
                :event_type, :page, :action, :target, :method, :path,
                CAST(:query_params AS jsonb), CAST(:details AS jsonb), :ip_address, :user_agent,
                :status_code, :success, :duration_ms
            )
        """), {
            "user_id": int(user["id"]) if user.get("id") is not None else None,
            "user_email": email,
            "role": role or "viewer",
            "session_fingerprint": session_fingerprint_from_payload(payload),
            "event_type": _clean_text(event_type, 100) or "action",
            "page": _clean_text(page, 200),
            "action": _clean_text(action, 300),
            "target": _clean_text(target, 500),
            "method": _clean_text(request_method, 20).upper(),
            "path": _clean_text(request_path, 500),
            "query_params": json.dumps(_safe_json(params), ensure_ascii=False, default=str),
            "details": json.dumps(_safe_json(details or {}), ensure_ascii=False, default=str),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "status_code": status_code,
            "success": bool(success),
            "duration_ms": max(0, int(duration_ms)) if duration_ms is not None else None,
        })


def write_viewer_event_by_id(
    *,
    user_id: int,
    event_type: str,
    action: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Record an administrative event against a Viewer account."""
    with ENGINE.connect() as conn:
        row = conn.execute(text("SELECT id, email, role FROM app_users WHERE id=:id"), {"id": user_id}).mappings().first()
    if not row or row.get("role") != "viewer":
        return
    write_audit_event(
        user=dict(row),
        request=None,
        event_type=event_type,
        page="User Administration",
        action=action,
        details=details or {},
        force=True,
    )


def cleanup_audit_logs() -> int:
    days = max(7, min(3650, int(os.getenv("AUDIT_RETENTION_DAYS", "90"))))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with ENGINE.begin() as conn:
        result = conn.execute(text("DELETE FROM viewer_audit_logs WHERE occurred_at < :cutoff"), {"cutoff": cutoff})
    return int(result.rowcount or 0)


def _build_filters(
    *,
    date_from: date | None,
    date_to: date | None,
    user_id: int | None,
    event_type: str | None,
    page: str | None,
    query: str | None,
) -> tuple[str, dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}
    if date_from:
        conditions.append("occurred_at >= (CAST(:date_from AS date)::timestamp AT TIME ZONE 'Asia/Kolkata')")
        params["date_from"] = date_from.isoformat()
    if date_to:
        conditions.append("occurred_at < ((CAST(:date_to AS date) + 1)::timestamp AT TIME ZONE 'Asia/Kolkata')")
        params["date_to"] = date_to.isoformat()
    if user_id:
        conditions.append("user_id = :user_id")
        params["user_id"] = int(user_id)
    if event_type:
        conditions.append("lower(event_type) = lower(:event_type)")
        params["event_type"] = _clean_text(event_type, 100)
    if page:
        conditions.append("page ILIKE :page")
        params["page"] = f"%{_clean_text(page, 200)}%"
    if query:
        conditions.append("(user_email ILIKE :query OR action ILIKE :query OR target ILIKE :query OR path ILIKE :query OR details::text ILIKE :query)")
        params["query"] = f"%{_clean_text(query, 200)}%"
    return (" WHERE " + " AND ".join(conditions)) if conditions else "", params


def list_audit_events(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    user_id: int | None = None,
    event_type: str | None = None,
    page: str | None = None,
    query: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    where_sql, params = _build_filters(
        date_from=date_from,
        date_to=date_to,
        user_id=user_id,
        event_type=event_type,
        page=page,
        query=query,
    )
    safe_limit = max(1, min(50000, int(limit)))
    safe_offset = max(0, int(offset))
    with ENGINE.connect() as conn:
        total = int(conn.execute(text(f"SELECT count(*) FROM viewer_audit_logs{where_sql}"), params).scalar_one() or 0)
        rows = conn.execute(text(f"""
            SELECT id, occurred_at, user_id, user_email, role, session_fingerprint,
                   event_type, page, action, target, method, path,
                   query_params, details, ip_address, user_agent,
                   status_code, success, duration_ms
            FROM viewer_audit_logs
            {where_sql}
            ORDER BY occurred_at DESC, id DESC
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": safe_limit, "offset": safe_offset}).mappings().all()
    return {"total": total, "limit": safe_limit, "offset": safe_offset, "events": [dict(row) for row in rows]}


def audit_csv_bytes(**filters: Any) -> bytes:
    result = list_audit_events(limit=50000, offset=0, **filters)
    output = io.StringIO()
    columns = [
        "occurred_at", "user_email", "event_type", "page", "action", "target",
        "method", "path", "query_params", "details", "ip_address", "user_agent",
        "status_code", "success", "duration_ms", "session_fingerprint",
    ]
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for row in result["events"]:
        writer.writerow({
            key: json.dumps(row.get(key), ensure_ascii=False, default=str) if key in {"query_params", "details"} else row.get(key)
            for key in columns
        })
    return output.getvalue().encode("utf-8-sig")
