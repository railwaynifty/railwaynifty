import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if not value:
        raise RuntimeError("DATABASE_URL is not configured")
    if value.startswith("postgres://"):
        value = "postgresql://" + value[len("postgres://"):]
    return value


ENGINE: Engine = create_engine(database_url(), pool_pre_ping=True, pool_recycle=300)


def init_database() -> None:
    with ENGINE.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS app_users (
                id bigserial PRIMARY KEY,
                email text NOT NULL UNIQUE,
                password_hash text NOT NULL,
                role text NOT NULL CHECK (role IN ('admin', 'viewer')),
                is_active boolean NOT NULL DEFAULT true,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                password_changed_at timestamptz NOT NULL DEFAULT now(),
                active_session_hash text,
                active_session_started_at timestamptz
            )
        """))
        # Safe idempotent migration for databases created by the earlier package.
        conn.execute(text("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS active_session_hash text"))
        conn.execute(text("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS active_session_started_at timestamptz"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_app_users_active ON app_users (is_active)"))
