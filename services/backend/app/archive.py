from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import secrets
import tempfile
import time
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import jwt
import psycopg2
from google.oauth2 import credentials as oauth_credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from sqlalchemy import text

from .auth import jwt_secret
from .database import ENGINE, database_url

IST = ZoneInfo("Asia/Kolkata")
ARCHIVE_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nse360-archive")
_ARCHIVE_LOCK = threading.RLock()
_STORAGE_CACHE: tuple[float, dict[str, Any]] | None = None

DATE_COLUMN_PRIORITY = (
    "trade_date",
    "scan_date",
    "signal_date",
    "snapshot_date",
    "as_of_date",
    "date",
    "timestamp",
    "created_at",
    "imported_at",
)


def market_schemas() -> tuple[str, ...]:
    values = (
        os.getenv("SCHEMA_OPTIONS", "options"),
        os.getenv("SCHEMA_FUTURES", "futures"),
        os.getenv("SCHEMA_CASH", "cash"),
    )
    return tuple(dict.fromkeys(value.strip() for value in values if value.strip()))


def archive_temp_dir() -> Path:
    path = Path(os.getenv("ARCHIVE_TEMP_DIR", "/tmp/nse360_archives"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def init_archive_database() -> None:
    with ENGINE.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS archive_jobs (
                id bigserial PRIMARY KEY,
                requested_by bigint,
                auto_run_date date,
                destination text NOT NULL CHECK (destination IN ('local', 'gdrive')),
                date_from date NOT NULL,
                date_to date NOT NULL,
                purge_after boolean NOT NULL DEFAULT false,
                compact_mode text NOT NULL DEFAULT 'vacuum'
                    CHECK (compact_mode IN ('none', 'vacuum', 'full')),
                status text NOT NULL DEFAULT 'queued',
                file_name text,
                local_path text,
                sha256 text,
                drive_md5 text,
                row_count bigint NOT NULL DEFAULT 0,
                table_count integer NOT NULL DEFAULT 0,
                drive_file_id text,
                drive_web_view_link text,
                manifest jsonb,
                error text,
                created_at timestamptz NOT NULL DEFAULT now(),
                started_at timestamptz,
                completed_at timestamptz,
                downloaded_at timestamptz,
                purged_at timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_archive_jobs_status ON archive_jobs(status, created_at)"))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_archive_jobs_auto_run_date
            ON archive_jobs(auto_run_date)
            WHERE auto_run_date IS NOT NULL
        """))


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _text_date_expression(column_sql: str) -> str:
    return f"""(
        CASE
            WHEN {column_sql} IS NULL OR btrim({column_sql}::text) = '' THEN NULL
            WHEN btrim({column_sql}::text) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                THEN substr(btrim({column_sql}::text), 1, 10)::date
            WHEN btrim({column_sql}::text) ~ '^\\d{{2}}-[A-Za-z]{{3}}-\\d{{4}}'
                THEN to_date(substr(btrim({column_sql}::text), 1, 11), 'DD-Mon-YYYY')
            WHEN btrim({column_sql}::text) ~ '^\\d{{2}}-\\d{{2}}-\\d{{4}}'
                THEN to_date(substr(btrim({column_sql}::text), 1, 10), 'DD-MM-YYYY')
            WHEN btrim({column_sql}::text) ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}'
                THEN to_date(substr(btrim({column_sql}::text), 1, 10), 'DD/MM/YYYY')
            ELSE NULL
        END
    )"""


def _date_expression(column_name: str, data_type: str) -> str:
    column_sql = _quote_ident(column_name)
    lowered = str(data_type or "").lower()
    if lowered == "date" or "timestamp" in lowered:
        return f"{column_sql}::date"
    return _text_date_expression(column_sql)


def discover_archive_tables(raw_conn=None) -> list[dict[str, Any]]:
    own_conn = raw_conn is None
    conn = raw_conn or psycopg2.connect(database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = ANY(%s)
                ORDER BY table_schema, table_name, ordinal_position
                """,
                (list(market_schemas()),),
            )
            rows = cur.fetchall()
        grouped: dict[tuple[str, str], list[tuple[str, str, int]]] = {}
        for schema, table, column, data_type, position in rows:
            grouped.setdefault((schema, table), []).append((column, data_type, position))

        tables: list[dict[str, Any]] = []
        for (schema, table), columns in sorted(grouped.items()):
            by_lower = {column.lower(): (column, data_type) for column, data_type, _ in columns}
            selected = None
            for candidate in DATE_COLUMN_PRIORITY:
                if candidate in by_lower:
                    selected = by_lower[candidate]
                    break
            if selected is None:
                continue
            column_name, data_type = selected
            tables.append({
                "schema": schema,
                "table": table,
                "date_column": column_name,
                "date_type": data_type,
                "date_expression": _date_expression(column_name, data_type),
                "columns": [column for column, _, _ in columns],
            })
        return tables
    finally:
        if own_conn:
            conn.close()


def _table_fqn(table: dict[str, Any]) -> str:
    return f"{_quote_ident(table['schema'])}.{_quote_ident(table['table'])}"


def _range_count(cur, table: dict[str, Any], date_from: date, date_to: date) -> int:
    sql = (
        f"SELECT count(*) FROM {_table_fqn(table)} "
        f"WHERE {table['date_expression']} BETWEEN %s AND %s"
    )
    cur.execute(sql, (date_from, date_to))
    return int(cur.fetchone()[0] or 0)


def storage_status(force: bool = False) -> dict[str, Any]:
    global _STORAGE_CACHE
    now = time.time()
    if not force and _STORAGE_CACHE and now - _STORAGE_CACHE[0] < 60:
        return _STORAGE_CACHE[1]
    raw = psycopg2.connect(database_url())
    try:
        tables = discover_archive_tables(raw)
        results = []
        with raw.cursor() as cur:
            for table in tables:
                fqn = _table_fqn(table)
                expr = table["date_expression"]
                try:
                    cur.execute(
                        f"SELECT min({expr}), max({expr}), count(*) FROM {fqn} WHERE {expr} IS NOT NULL"
                    )
                    min_date, max_date, row_count = cur.fetchone()
                    regclass_name = f'"{table["schema"].replace(chr(34), chr(34) * 2)}"."{table["table"].replace(chr(34), chr(34) * 2)}"'
                    cur.execute("SELECT pg_total_relation_size(%s::regclass)", (regclass_name,))
                    size_bytes = int(cur.fetchone()[0] or 0)
                except Exception:
                    raw.rollback()
                    continue
                results.append({
                    "schema": table["schema"],
                    "table": table["table"],
                    "date_column": table["date_column"],
                    "min_date": min_date,
                    "max_date": max_date,
                    "row_count": int(row_count or 0),
                    "size_bytes": size_bytes,
                })
        result = {
            "schemas": list(market_schemas()),
            "tables": sorted(results, key=lambda item: item["size_bytes"], reverse=True),
            "total_size_bytes": sum(item["size_bytes"] for item in results),
            "total_rows": sum(item["row_count"] for item in results),
        }
        _STORAGE_CACHE = (now, result)
        return result
    finally:
        raw.close()


def archive_date_bounds() -> tuple[date | None, date | None]:
    status = storage_status()
    mins = [item["min_date"] for item in status["tables"] if item.get("min_date")]
    maxs = [item["max_date"] for item in status["tables"] if item.get("max_date")]
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def _zip_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_archive_file(date_from: date, date_to: date, output_path: Path) -> dict[str, Any]:
    raw = psycopg2.connect(database_url())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        tables = discover_archive_tables(raw)
        manifest: dict[str, Any] = {
            "format_version": 1,
            "application": "NSE 360 Private",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "schemas": list(market_schemas()),
            "tables": [],
            "skipped_tables": [],
        }
        total_rows = 0
        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
            with raw.cursor() as cur:
                for table in tables:
                    try:
                        row_count = _range_count(cur, table, date_from, date_to)
                    except Exception as exc:
                        raw.rollback()
                        manifest["skipped_tables"].append({
                            "schema": table["schema"],
                            "table": table["table"],
                            "reason": f"count failed: {type(exc).__name__}: {exc}",
                        })
                        continue
                    if row_count <= 0:
                        continue

                    csv_name = f"{table['schema']}/{table['table']}.csv"
                    select_sql = (
                        f"SELECT * FROM {_table_fqn(table)} "
                        f"WHERE {table['date_expression']} BETWEEN %s AND %s"
                    )
                    copy_query = cur.mogrify(
                        f"COPY ({select_sql}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')",
                        (date_from, date_to),
                    ).decode("utf-8")
                    with archive.open(csv_name, "w", force_zip64=True) as binary_stream:
                        text_stream = io.TextIOWrapper(binary_stream, encoding="utf-8", newline="")
                        try:
                            cur.copy_expert(copy_query, text_stream)
                            text_stream.flush()
                        finally:
                            text_stream.detach()

                    total_rows += row_count
                    manifest["tables"].append({
                        "schema": table["schema"],
                        "table": table["table"],
                        "date_column": table["date_column"],
                        "date_type": table["date_type"],
                        "row_count": row_count,
                        "csv_file": csv_name,
                        "columns": table["columns"],
                    })

            manifest["row_count"] = total_rows
            manifest["table_count"] = len(manifest["tables"])
            archive.writestr("manifest.json", json.dumps(manifest, indent=2, default=str, ensure_ascii=False))
            archive.writestr(
                "README.txt",
                "NSE 360 market-data archive. CSV files preserve PostgreSQL column names. "
                "See manifest.json for date range, row counts and table metadata.\n",
            )

        manifest["file_name"] = output_path.name
        manifest["file_size_bytes"] = output_path.stat().st_size
        manifest["sha256"] = _zip_sha256(output_path)
        return manifest
    finally:
        raw.close()


def _decode_service_account_json(value: str) -> dict[str, Any]:
    value = value.strip()
    if value.startswith("{"):
        return json.loads(value)
    try:
        return json.loads(base64.b64decode(value).decode("utf-8"))
    except Exception as exc:
        raise RuntimeError("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON must be JSON or base64-encoded JSON") from exc


def google_drive_credentials():
    service_json = os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    if service_json:
        info = _decode_service_account_json(service_json)
        return service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )

    client_id = os.getenv("GOOGLE_DRIVE_CLIENT_ID", "").strip()
    client_secret = os.getenv("GOOGLE_DRIVE_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("GOOGLE_DRIVE_REFRESH_TOKEN", "").strip()
    if client_id and client_secret and refresh_token:
        return oauth_credentials.Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
    raise RuntimeError(
        "Google Drive is not configured. Set service-account JSON or OAuth client ID/secret/refresh token."
    )


def google_drive_configured() -> bool:
    return bool(
        os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "").strip()
        or (
            os.getenv("GOOGLE_DRIVE_CLIENT_ID", "").strip()
            and os.getenv("GOOGLE_DRIVE_CLIENT_SECRET", "").strip()
            and os.getenv("GOOGLE_DRIVE_REFRESH_TOKEN", "").strip()
        )
    ) and bool(os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip())


def upload_to_google_drive(path: Path) -> dict[str, Any]:
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured")
    service = build("drive", "v3", credentials=google_drive_credentials(), cache_discovery=False)
    media = MediaFileUpload(
        str(path),
        mimetype="application/zip",
        resumable=True,
        chunksize=8 * 1024 * 1024,
    )
    request = service.files().create(
        body={"name": path.name, "parents": [folder_id]},
        media_body=media,
        fields="id,name,size,md5Checksum,webViewLink",
        supportsAllDrives=True,
    )
    response = None
    while response is None:
        _status, response = request.next_chunk()
    file_id = response["id"]
    verified = service.files().get(
        fileId=file_id,
        fields="id,name,size,md5Checksum,webViewLink",
        supportsAllDrives=True,
    ).execute()
    return verified


def _job_row(job_id: int) -> dict[str, Any] | None:
    with ENGINE.connect() as conn:
        row = conn.execute(text("SELECT * FROM archive_jobs WHERE id=:id"), {"id": job_id}).mappings().first()
    return dict(row) if row else None


def public_job(job: dict[str, Any], request_base_url: str | None = None) -> dict[str, Any]:
    result = {
        key: job.get(key)
        for key in (
            "id", "requested_by", "auto_run_date", "destination", "date_from", "date_to",
            "purge_after", "compact_mode", "status", "file_name", "sha256", "drive_md5",
            "row_count", "table_count", "drive_file_id", "drive_web_view_link", "manifest",
            "error", "created_at", "started_at", "completed_at", "downloaded_at", "purged_at",
            "updated_at",
        )
    }
    local_path = job.get("local_path")
    if job.get("destination") == "local" and job.get("status") in {"ready", "downloaded"} and local_path:
        path = Path(str(local_path))
        if path.exists() and request_base_url:
            token = create_download_token(int(job["id"]))
            result["download_url"] = request_base_url.rstrip("/") + "/archive-download/" + token
            result["local_file_expires_at"] = (
                datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                + timedelta(hours=max(1, int(os.getenv("ARCHIVE_LOCAL_TTL_HOURS", "6"))))
            ).isoformat()
    return result


def create_download_token(job_id: int) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "purpose": "archive-download",
            "job": int(job_id),
            "nonce": secrets.token_urlsafe(12),
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=30)).timestamp()),
        },
        jwt_secret(),
        algorithm="HS256",
    )


def verify_download_token(token: str) -> int:
    try:
        payload = jwt.decode(token, jwt_secret(), algorithms=["HS256"])
        if payload.get("purpose") != "archive-download":
            raise ValueError("wrong purpose")
        return int(payload["job"])
    except Exception as exc:
        raise ValueError("Invalid or expired archive download link") from exc


def create_archive_job(
    *,
    requested_by: int | None,
    destination: str,
    date_from: date,
    date_to: date,
    purge_after: bool,
    compact_mode: str,
    auto_run_date: date | None = None,
) -> dict[str, Any]:
    destination = destination.strip().lower()
    compact_mode = compact_mode.strip().lower()
    if destination not in {"local", "gdrive"}:
        raise ValueError("Destination must be local or gdrive")
    if compact_mode not in {"none", "vacuum", "full"}:
        raise ValueError("Compact mode must be none, vacuum or full")
    if date_from > date_to:
        raise ValueError("date_from cannot be after date_to")
    if destination == "local":
        purge_after = False
    with ENGINE.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO archive_jobs
                (requested_by, auto_run_date, destination, date_from, date_to, purge_after, compact_mode, status)
            VALUES
                (:requested_by, :auto_run_date, :destination, :date_from, :date_to, :purge_after, :compact_mode, 'queued')
            RETURNING *
        """), {
            "requested_by": requested_by,
            "auto_run_date": auto_run_date,
            "destination": destination,
            "date_from": date_from,
            "date_to": date_to,
            "purge_after": bool(purge_after),
            "compact_mode": compact_mode,
        }).mappings().one()
    job = dict(row)
    submit_archive_job(int(job["id"]))
    return job


def submit_archive_job(job_id: int) -> None:
    ARCHIVE_EXECUTOR.submit(process_archive_job, int(job_id))


def _update_job(job_id: int, **values: Any) -> None:
    if not values:
        return
    values["updated_at"] = datetime.now(timezone.utc)
    assignments = ", ".join(
        f"{key}=CAST(:{key} AS jsonb)" if key == "manifest" else f"{key}=:{key}"
        for key in values
    )
    values["id"] = job_id
    with ENGINE.begin() as conn:
        conn.execute(text(f"UPDATE archive_jobs SET {assignments} WHERE id=:id"), values)


def process_archive_job(job_id: int) -> None:
    with _ARCHIVE_LOCK:
        job = _job_row(job_id)
        if not job or job.get("status") not in {"queued", "retry"}:
            return
        _update_job(job_id, status="running", started_at=datetime.now(timezone.utc), error=None)
        date_from = job["date_from"]
        date_to = job["date_to"]
        filename = (
            f"nse360_{date_from.isoformat()}_to_{date_to.isoformat()}_"
            f"{datetime.now(IST).strftime('%Y%m%d_%H%M%S_IST')}.zip"
        )
        path = archive_temp_dir() / filename
        try:
            manifest = build_archive_file(date_from, date_to, path)
            if int(manifest.get("row_count") or 0) <= 0:
                path.unlink(missing_ok=True)
                raise RuntimeError("No archiveable market-data rows were found in the selected date range")

            update: dict[str, Any] = {
                "file_name": filename,
                "sha256": manifest["sha256"],
                "row_count": manifest["row_count"],
                "table_count": manifest["table_count"],
                "manifest": json.dumps(manifest, default=str),
                "completed_at": datetime.now(timezone.utc),
            }
            if job["destination"] == "local":
                update.update({"status": "ready", "local_path": str(path)})
                _update_job(job_id, **update)
                return

            drive = upload_to_google_drive(path)
            update.update({
                "status": "completed",
                "drive_file_id": drive.get("id"),
                "drive_web_view_link": drive.get("webViewLink"),
                "drive_md5": drive.get("md5Checksum"),
                "local_path": None,
            })
            _update_job(job_id, **update)
            path.unlink(missing_ok=True)
            if job.get("purge_after"):
                try:
                    process_purge_job(job_id, force=False)
                except Exception:
                    # process_purge_job records purge_failed and the exact error.
                    return
        except Exception as exc:
            path.unlink(missing_ok=True)
            _update_job(job_id, status="failed", error=f"{type(exc).__name__}: {exc}")


def _manifest_dict(job: dict[str, Any]) -> dict[str, Any]:
    value = job.get("manifest")
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        return json.loads(value)
    raise RuntimeError("Archive manifest is missing")


def process_purge_job(job_id: int, force: bool = False) -> None:
    with _ARCHIVE_LOCK:
        job = _job_row(job_id)
        if not job:
            return
        if job.get("status") not in {"ready", "downloaded", "completed", "purge_queued"}:
            raise RuntimeError(f"Job {job_id} cannot be purged from status {job.get('status')}")
        manifest = _manifest_dict(job)
        _update_job(job_id, status="purging", error=None)
        raw = psycopg2.connect(database_url())
        affected: list[dict[str, Any]] = []
        try:
            known = {(item["schema"], item["table"]): item for item in discover_archive_tables(raw)}
            with raw.cursor() as cur:
                for archived in manifest.get("tables", []):
                    key = (archived["schema"], archived["table"])
                    table = known.get(key)
                    if not table:
                        raise RuntimeError(f"Table disappeared or no longer has a supported date column: {key}")
                    current_count = _range_count(cur, table, job["date_from"], job["date_to"])
                    archived_count = int(archived.get("row_count") or 0)
                    if not force and current_count != archived_count:
                        raise RuntimeError(
                            f"Safety check failed for {key[0]}.{key[1]}: "
                            f"archive has {archived_count:,} rows but database now has {current_count:,}. "
                            "Create a fresh archive before purging."
                        )
                for archived in manifest.get("tables", []):
                    table = known[(archived["schema"], archived["table"])]
                    delete_sql = (
                        f"DELETE FROM {_table_fqn(table)} "
                        f"WHERE {table['date_expression']} BETWEEN %s AND %s"
                    )
                    cur.execute(delete_sql, (job["date_from"], job["date_to"]))
                    affected.append(table)
            raw.commit()
        except Exception as exc:
            raw.rollback()
            _update_job(job_id, status="purge_failed", error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            raw.close()

        compact_mode = str(job.get("compact_mode") or "vacuum")
        compact_error = None
        if compact_mode != "none":
            vacuum_conn = psycopg2.connect(database_url())
            vacuum_conn.autocommit = True
            try:
                with vacuum_conn.cursor() as cur:
                    for table in affected:
                        if compact_mode == "full":
                            cur.execute(f"VACUUM (FULL, ANALYZE) {_table_fqn(table)}")
                        else:
                            cur.execute(f"VACUUM (ANALYZE) {_table_fqn(table)}")
            except Exception as exc:
                compact_error = f"Data was deleted, but compaction failed: {type(exc).__name__}: {exc}"
            finally:
                vacuum_conn.close()

        global _STORAGE_CACHE
        _STORAGE_CACHE = None
        _update_job(
            job_id,
            status="purged" if not compact_error else "purged_with_warning",
            purged_at=datetime.now(timezone.utc),
            error=compact_error,
        )


def queue_purge(job_id: int, force: bool = False) -> None:
    _update_job(job_id, status="purge_queued")
    ARCHIVE_EXECUTOR.submit(process_purge_job, int(job_id), bool(force))


def retry_job(job_id: int) -> None:
    job = _job_row(job_id)
    if not job or job.get("status") not in {"failed", "purge_failed"}:
        raise RuntimeError("Only failed archive or purge jobs can be retried")
    if job.get("status") == "purge_failed":
        _update_job(job_id, status="purge_queued", error=None)
        ARCHIVE_EXECUTOR.submit(process_purge_job, int(job_id), False)
        return
    _update_job(job_id, status="retry", error=None)
    submit_archive_job(job_id)


def mark_downloaded(job_id: int) -> None:
    _update_job(job_id, status="downloaded", downloaded_at=datetime.now(timezone.utc))


def cleanup_expired_local_archives() -> None:
    ttl = max(1, int(os.getenv("ARCHIVE_LOCAL_TTL_HOURS", "6")))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl)
    with ENGINE.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, local_path FROM archive_jobs
            WHERE destination='local' AND local_path IS NOT NULL
              AND completed_at IS NOT NULL AND completed_at < :cutoff
        """), {"cutoff": cutoff}).mappings().all()
    for row in rows:
        try:
            Path(str(row["local_path"])).unlink(missing_ok=True)
        except Exception:
            pass
        _update_job(int(row["id"]), local_path=None)


def resume_queued_jobs() -> None:
    cleanup_expired_local_archives()
    with ENGINE.begin() as conn:
        conn.execute(text("""
            UPDATE archive_jobs
            SET status='failed', error='Backend restarted while this job was running', updated_at=now()
            WHERE status IN ('running', 'purging')
        """))
        queued = conn.execute(text("""
            SELECT id FROM archive_jobs WHERE status IN ('queued', 'retry', 'purge_queued') ORDER BY id
        """)).scalars().all()
    for job_id in queued:
        job = _job_row(int(job_id))
        if job and job.get("status") == "purge_queued":
            ARCHIVE_EXECUTOR.submit(process_purge_job, int(job_id), False)
        else:
            submit_archive_job(int(job_id))


def maybe_create_automatic_archive() -> dict[str, Any] | None:
    if os.getenv("ARCHIVE_AUTO_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "y"}:
        return None
    if not google_drive_configured():
        return None
    retention_days = max(1, int(os.getenv("ARCHIVE_RETENTION_DAYS", "30")))
    today_ist = datetime.now(IST).date()
    cutoff = today_ist - timedelta(days=retention_days)
    earliest, _latest = archive_date_bounds()
    if earliest is None or earliest > cutoff:
        return None
    purge_after = os.getenv("ARCHIVE_AUTO_PURGE", "false").strip().lower() in {"1", "true", "yes", "y"}
    compact_mode = os.getenv("ARCHIVE_COMPACT_MODE", "vacuum").strip().lower()
    try:
        return create_archive_job(
            requested_by=None,
            auto_run_date=today_ist,
            destination="gdrive",
            date_from=earliest,
            date_to=cutoff,
            purge_after=purge_after,
            compact_mode=compact_mode,
        )
    except Exception as exc:
        # Unique auto-run date means another replica already created today's job.
        if "uq_archive_jobs_auto_run_date" in str(exc) or "duplicate key" in str(exc).lower():
            return None
        raise
