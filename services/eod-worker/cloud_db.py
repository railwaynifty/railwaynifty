from __future__ import annotations

import os
from urllib.parse import urlparse

import psycopg2
import sqlalchemy
from sqlalchemy import text

LOGICAL_SCHEMA_MAP = {
    "idxoptionsdata_current": os.getenv("SCHEMA_OPTIONS", "options"),
    "idxfuturesdata_current": os.getenv("SCHEMA_FUTURES", "futures"),
    "idxcashdata_current": os.getenv("SCHEMA_CASH", "cash"),
}


def database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if not value:
        raise RuntimeError("DATABASE_URL is not configured")
    if value.startswith("postgres://"):
        value = "postgresql://" + value[len("postgres://"):]
    return value


def schema_for(logical_name: str | None) -> str:
    return LOGICAL_SCHEMA_MAP.get(str(logical_name or ""), os.getenv("SCHEMA_OPTIONS", "options"))


def ensure_schema(schema: str) -> None:
    safe = "".join(ch for ch in schema if ch.isalnum() or ch == "_")
    if not safe or safe != schema:
        raise ValueError(f"Invalid PostgreSQL schema name: {schema!r}")
    engine = sqlalchemy.create_engine(database_url(), pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{safe}"'))
    finally:
        engine.dispose()


def ensure_logical_database(logical_name: str, *_args, **_kwargs) -> None:
    ensure_schema(schema_for(logical_name))


def make_schema_engine(logical_name: str, *_args, **_kwargs) -> sqlalchemy.Engine:
    schema = schema_for(logical_name)
    ensure_schema(schema)
    return sqlalchemy.create_engine(
        database_url(),
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"options": f"-c search_path={schema},public"},
    )


def psycopg_connect(logical_name: str, autocommit: bool = False):
    schema = schema_for(logical_name)
    ensure_schema(schema)
    conn = psycopg2.connect(database_url(), options=f"-c search_path={schema},public")
    conn.autocommit = autocommit
    return conn
