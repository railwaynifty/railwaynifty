from __future__ import annotations

import asyncio
import hmac
import os
import socket
import subprocess
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, JSONResponse
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import text

from .auth import (
    authenticated_session,
    create_token,
    current_user,
    get_user_by_email,
    hash_password,
    public_user,
    require_admin,
    revoke_current_session,
    seed_initial_admin,
    start_login_session,
    verify_password,
)
from .database import ENGINE, init_database
from .audit import (
    audit_csv_bytes,
    cleanup_audit_logs,
    init_audit_database,
    list_audit_events,
    write_audit_event,
    write_viewer_event_by_id,
)
from .archive import (
    cleanup_expired_local_archives,
    create_archive_job,
    google_drive_configured,
    init_archive_database,
    maybe_create_automatic_archive,
    public_job,
    queue_purge,
    resume_queued_jobs,
    retry_job,
    storage_status,
    verify_download_token,
    mark_downloaded,
    _job_row,
)

LEGACY_PORT = int(os.getenv("LEGACY_PORT", "8102"))
LEGACY_PATH = Path(__file__).resolve().parent.parent / "legacy" / "dashboard.py"
LEGACY_PROCESS: subprocess.Popen | None = None
LOGIN_ATTEMPTS: dict[str, deque[float]] = defaultdict(deque)
ARCHIVE_SCHEDULER_TASK: asyncio.Task | None = None
IST = ZoneInfo("Asia/Kolkata")


def require_internal_key(request: Request) -> None:
    expected = os.getenv("INTERNAL_PROXY_KEY", "").strip()
    if not expected:
        return
    supplied = request.headers.get("x-internal-key", "")
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=403, detail="Invalid internal proxy key")


def start_legacy() -> subprocess.Popen:
    env = dict(os.environ)
    env["DASHBOARD_PORT"] = str(LEGACY_PORT)
    env["DASHBOARD_HOST"] = "127.0.0.1"
    env["OPEN_BROWSER"] = "0"
    process = subprocess.Popen([sys.executable, str(LEGACY_PATH)], env=env)
    return process


async def wait_for_port(port: int, timeout: float = 45.0) -> None:
    started = asyncio.get_running_loop().time()
    while asyncio.get_running_loop().time() - started < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            await asyncio.sleep(0.5)
    raise RuntimeError(f"Legacy dashboard did not start on port {port}")


async def archive_scheduler_loop() -> None:
    """Run archive checks and once-daily audit retention cleanup."""
    last_audit_cleanup_date = None
    while True:
        try:
            now = datetime.now(IST)
            if last_audit_cleanup_date != now.date():
                await asyncio.to_thread(cleanup_audit_logs)
                last_audit_cleanup_date = now.date()
            hour = max(0, min(23, int(os.getenv("ARCHIVE_AUTO_HOUR_IST", "21"))))
            if now.hour >= hour:
                await asyncio.to_thread(maybe_create_automatic_archive)
            await asyncio.sleep(15 * 60)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[ARCHIVE] Automatic archive check failed: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(15 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global LEGACY_PROCESS, ARCHIVE_SCHEDULER_TASK
    if len(os.getenv("INTERNAL_PROXY_KEY", "").strip()) < 32:
        raise RuntimeError("INTERNAL_PROXY_KEY must be configured with at least 32 characters")
    # Validate the signing secret before accepting any requests.
    from .auth import jwt_secret
    jwt_secret()
    init_database()
    init_audit_database()
    cleanup_audit_logs()
    init_archive_database()
    seed_initial_admin()
    resume_queued_jobs()
    ARCHIVE_SCHEDULER_TASK = asyncio.create_task(archive_scheduler_loop())
    LEGACY_PROCESS = start_legacy()
    await wait_for_port(LEGACY_PORT)
    print(f"[READY] Legacy dashboard is listening on 127.0.0.1:{LEGACY_PORT}", flush=True)
    yield
    if ARCHIVE_SCHEDULER_TASK:
        ARCHIVE_SCHEDULER_TASK.cancel()
        try:
            await ARCHIVE_SCHEDULER_TASK
        except asyncio.CancelledError:
            pass
    if LEGACY_PROCESS and LEGACY_PROCESS.poll() is None:
        LEGACY_PROCESS.terminate()
        try:
            LEGACY_PROCESS.wait(timeout=10)
        except subprocess.TimeoutExpired:
            LEGACY_PROCESS.kill()


app = FastAPI(title="NSE 360 Private Gateway", lifespan=lifespan)


@app.middleware("http")
async def internal_key_middleware(request: Request, call_next):
    if request.url.path != "/health" and not request.url.path.startswith("/archive-download/"):
        expected = os.getenv("INTERNAL_PROXY_KEY", "").strip()
        supplied = request.headers.get("x-internal-key", "")
        if expected and not hmac.compare_digest(supplied, expected):
            return JSONResponse({"detail": "Invalid internal proxy key"}, status_code=403)
    return await call_next(request)


class LoginBody(BaseModel):
    email: EmailStr
    password: str


class CreateUserBody(BaseModel):
    email: EmailStr
    password: str
    role: str = "viewer"


class StatusBody(BaseModel):
    is_active: bool


class PasswordBody(BaseModel):
    password: str


class ArchiveJobBody(BaseModel):
    date_from: date
    date_to: date
    destination: str = "local"
    purge_after: bool = False
    compact_mode: str = "vacuum"


class PurgeBody(BaseModel):
    force: bool = False


class AuditEventBody(BaseModel):
    event_type: str
    page: str = ""
    action: str = ""
    target: str = ""
    details: dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health():
    process_ok = LEGACY_PROCESS is not None and LEGACY_PROCESS.poll() is None
    return {"ok": True, "legacy": process_ok}


@app.post("/auth/login")
def login(body: LoginBody, request: Request):
    client_ip = request.headers.get("x-client-ip", "").split(",")[0].strip() or request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (request.client.host if request.client else "unknown")
    key = f"{client_ip}:{body.email.lower()}"
    now = time.time()
    bucket = LOGIN_ATTEMPTS[key]
    while bucket and bucket[0] < now - 900:
        bucket.popleft()
    if len(bucket) >= 5:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again after 15 minutes")
    user = get_user_by_email(body.email)
    if not user or not user["is_active"] or not verify_password(body.password, user["password_hash"]):
        bucket.append(now)
        if user and user.get("role") == "viewer":
            write_audit_event(
                user=user, request=request, event_type="login_failed", page="Login",
                action="Invalid password or inactive account", status_code=401, success=False,
            )
        raise HTTPException(status_code=401, detail="Invalid email or password")
    LOGIN_ATTEMPTS.pop(key, None)
    replaced_previous_session = bool(user.get("role") == "viewer" and user.get("active_session_hash"))
    session_id = start_login_session(user)
    token = create_token(user, session_id=session_id)
    write_audit_event(
        user=user, request=request, event_type="login_success", page="Login",
        action="Viewer signed in; previous session replaced" if replaced_previous_session else "Viewer signed in", status_code=200, success=True,
        details={"previous_session_replaced": replaced_previous_session},
        payload={"sid": session_id} if session_id else None,
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": public_user(user),
        "single_session": user.get("role") == "viewer",
    }


@app.post("/auth/logout")
def logout(request: Request, session: dict[str, Any] = Depends(authenticated_session)):
    write_audit_event(
        user=session["user"], request=request, event_type="logout", page="Dashboard",
        action="Viewer signed out", status_code=200, success=True, payload=session.get("payload"),
    )
    revoke_current_session(session)
    return {"ok": True}


@app.get("/auth/me")
def me(user: dict[str, Any] = Depends(current_user)):
    return public_user(user)


@app.post("/audit/events", status_code=201)
def record_audit_event(body: AuditEventBody, request: Request, session: dict[str, Any] = Depends(authenticated_session)):
    write_audit_event(
        user=session["user"], request=request, event_type=body.event_type,
        page=body.page, action=body.action, target=body.target, details=body.details,
        status_code=201, success=True, payload=session.get("payload"),
    )
    return {"ok": True}


@app.get("/admin/audit")
def get_audit_log(
    date_from: date | None = None,
    date_to: date | None = None,
    user_id: int | None = None,
    event_type: str | None = None,
    page: str | None = None,
    q: str | None = None,
    limit: int = 100,
    offset: int = 0,
    _: dict[str, Any] = Depends(require_admin),
):
    return list_audit_events(
        date_from=date_from, date_to=date_to, user_id=user_id, event_type=event_type,
        page=page, query=q, limit=min(limit, 500), offset=offset,
    )


@app.get("/admin/audit/export")
def export_audit_log(
    date_from: date | None = None,
    date_to: date | None = None,
    user_id: int | None = None,
    event_type: str | None = None,
    page: str | None = None,
    q: str | None = None,
    _: dict[str, Any] = Depends(require_admin),
):
    content = audit_csv_bytes(
        date_from=date_from, date_to=date_to, user_id=user_id, event_type=event_type, page=page, query=q,
    )
    filename = f"viewer_audit_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=content, media_type="text/csv; charset=utf-8",
        headers={"content-disposition": f'attachment; filename="{filename}"', "cache-control": "no-store"},
    )


@app.get("/admin/users")
def list_users(_: dict[str, Any] = Depends(require_admin)):
    with ENGINE.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, email, role, is_active, created_at, updated_at,
                   active_session_started_at,
                   (active_session_hash IS NOT NULL) AS session_active
            FROM app_users ORDER BY created_at, id
        """)).mappings().all()
    return {"users": [dict(row) for row in rows]}


@app.post("/admin/users", status_code=201)
def create_user(body: CreateUserBody, _: dict[str, Any] = Depends(require_admin)):
    role = body.role.strip().lower()
    if role not in {"admin", "viewer"}:
        raise HTTPException(status_code=400, detail="Role must be admin or viewer")
    if get_user_by_email(body.email):
        raise HTTPException(status_code=409, detail="A user with this email already exists")
    with ENGINE.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO app_users (email, password_hash, role, is_active)
            VALUES (lower(:email), :password_hash, :role, true)
            RETURNING id, email, role, is_active, created_at, updated_at
        """), {"email": body.email, "password_hash": hash_password(body.password), "role": role}).mappings().one()
    return public_user(dict(row))


@app.patch("/admin/users/{user_id}/status")
def update_status(user_id: int, body: StatusBody, admin: dict[str, Any] = Depends(require_admin)):
    if user_id == int(admin["id"]) and not body.is_active:
        raise HTTPException(status_code=400, detail="You cannot disable your own account")
    with ENGINE.begin() as conn:
        row = conn.execute(text("""
            UPDATE app_users
            SET is_active=:is_active,
                active_session_hash = CASE WHEN :is_active THEN active_session_hash ELSE NULL END,
                active_session_started_at = CASE WHEN :is_active THEN active_session_started_at ELSE NULL END,
                updated_at=now()
            WHERE id=:id
            RETURNING id, email, role, is_active, created_at, updated_at
        """), {"id": user_id, "is_active": body.is_active}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    write_viewer_event_by_id(
        user_id=user_id, event_type="account_status_changed",
        action="Viewer account enabled" if body.is_active else "Viewer account disabled",
        details={"changed_by": admin.get("email")},
    )
    return public_user(dict(row))


@app.post("/admin/users/{user_id}/reset-password")
def reset_password(user_id: int, body: PasswordBody, _: dict[str, Any] = Depends(require_admin)):
    with ENGINE.begin() as conn:
        result = conn.execute(text("""
            UPDATE app_users
            SET password_hash=:password_hash, password_changed_at=now(),
                active_session_hash=NULL, active_session_started_at=NULL, updated_at=now()
            WHERE id=:id
        """), {"id": user_id, "password_hash": hash_password(body.password)})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="User not found")
    write_viewer_event_by_id(
        user_id=user_id, event_type="password_reset_by_admin", action="Viewer password reset; sessions revoked",
    )
    return {"ok": True}


@app.post("/admin/users/{user_id}/revoke-session")
def revoke_user_session(user_id: int, _: dict[str, Any] = Depends(require_admin)):
    with ENGINE.begin() as conn:
        result = conn.execute(text("""
            UPDATE app_users
            SET active_session_hash=NULL, active_session_started_at=NULL, updated_at=now()
            WHERE id=:id
        """), {"id": user_id})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="User not found")
    write_viewer_event_by_id(
        user_id=user_id, event_type="session_revoked_by_admin", action="Active Viewer session revoked",
    )
    return {"ok": True}


@app.get("/admin/archive/status")
def archive_status(request: Request, _: dict[str, Any] = Depends(require_admin)):
    cleanup_expired_local_archives()
    with ENGINE.connect() as conn:
        rows = conn.execute(text("""
            SELECT * FROM archive_jobs ORDER BY id DESC LIMIT 50
        """)).mappings().all()
    base_url = str(request.base_url).rstrip("/")
    return {
        "storage": storage_status(),
        "jobs": [public_job(dict(row), base_url) for row in rows],
        "google_drive_configured": google_drive_configured(),
        "auto_archive_enabled": os.getenv("ARCHIVE_AUTO_ENABLED", "false").strip().lower() in {"1", "true", "yes", "y"},
        "retention_days": int(os.getenv("ARCHIVE_RETENTION_DAYS", "30")),
        "auto_purge": os.getenv("ARCHIVE_AUTO_PURGE", "false").strip().lower() in {"1", "true", "yes", "y"},
    }


@app.post("/admin/archive/jobs", status_code=202)
def create_archive(body: ArchiveJobBody, request: Request, admin: dict[str, Any] = Depends(require_admin)):
    destination = body.destination.strip().lower()
    if destination == "gdrive" and not google_drive_configured():
        raise HTTPException(status_code=400, detail="Google Drive is not configured on Railway")
    try:
        job = create_archive_job(
            requested_by=int(admin["id"]),
            destination=destination,
            date_from=body.date_from,
            date_to=body.date_to,
            purge_after=body.purge_after,
            compact_mode=body.compact_mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return public_job(job, str(request.base_url).rstrip("/"))


@app.post("/admin/archive/jobs/{job_id}/purge", status_code=202)
def purge_archive(job_id: int, body: PurgeBody, _: dict[str, Any] = Depends(require_admin)):
    job = _job_row(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Archive job not found")
    if job.get("status") not in {"ready", "downloaded", "completed"}:
        raise HTTPException(status_code=409, detail=f"Archive cannot be purged from status {job.get('status')}")
    queue_purge(job_id, force=body.force)
    return {"ok": True, "status": "purge_queued"}


@app.post("/admin/archive/jobs/{job_id}/retry", status_code=202)
def retry_archive(job_id: int, _: dict[str, Any] = Depends(require_admin)):
    try:
        retry_job(job_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True, "status": "retry"}


@app.get("/archive-download/{token}")
def download_archive(token: str):
    try:
        job_id = verify_download_token(token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    job = _job_row(job_id)
    if not job or job.get("destination") != "local" or job.get("status") not in {"ready", "downloaded"}:
        raise HTTPException(status_code=404, detail="Archive file is not available")
    local_path = Path(str(job.get("local_path") or ""))
    if not local_path.exists():
        raise HTTPException(status_code=410, detail="Archive file has expired; prepare a new local export")
    mark_downloaded(job_id)
    return FileResponse(
        path=local_path,
        filename=str(job.get("file_name") or local_path.name),
        media_type="application/zip",
        headers={"cache-control": "no-store"},
    )


@app.api_route("/legacy", methods=["GET", "POST", "PATCH", "DELETE"])
@app.api_route("/legacy/", methods=["GET", "POST", "PATCH", "DELETE"])
@app.api_route("/legacy/{path:path}", methods=["GET", "POST", "PATCH", "DELETE"])
async def legacy_proxy(request: Request, path: str = "", session: dict[str, Any] = Depends(authenticated_session)):
    if LEGACY_PROCESS is None or LEGACY_PROCESS.poll() is not None:
        raise HTTPException(status_code=503, detail="Dashboard process is not available")
    target = httpx.URL(f"http://127.0.0.1:{LEGACY_PORT}/{path}", query=request.url.query.encode())
    body = await request.body()
    headers = {}
    if request.headers.get("content-type"):
        headers["content-type"] = request.headers["content-type"]
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=False) as client:
        upstream = await client.request(request.method, target, content=body or None, headers=headers)
    passthrough = {}
    for name in ("content-type", "content-disposition", "cache-control", "location"):
        value = upstream.headers.get(name)
        if value:
            passthrough[name] = value

    # Avoid logging every automatic background GET. Page opens, exports and
    # mutating actions are recorded here; interactive tab/filter/button events
    # are sent explicitly by the browser audit hook.
    content_disposition = upstream.headers.get("content-disposition", "")
    event_type = ""
    action = ""
    if request.method == "GET" and not path:
        event_type, action = "dashboard_open", "Opened NSE 360 dashboard"
    elif content_disposition:
        event_type, action = "export_download", "Downloaded dashboard export"
    elif request.method not in {"GET", "HEAD"}:
        event_type, action = "dashboard_api_action", f"{request.method} dashboard action"
    if event_type:
        await asyncio.to_thread(
            write_audit_event,
            user=session["user"], request=request, event_type=event_type,
            page="NSE 360 Dashboard", action=action, target=path,
            status_code=upstream.status_code, success=upstream.status_code < 400,
            payload=session.get("payload"),
        )
    return Response(content=upstream.content, status_code=upstream.status_code, headers=passthrough)
