from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from cloud_db import psycopg_connect


IST = ZoneInfo("Asia/Kolkata")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def table_exists(cur, qualified_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (qualified_name,))
    return cur.fetchone()[0] is not None


def truncate_if_exists(cur, table_names: list[str]) -> list[str]:
    existing = [name for name in table_names if table_exists(cur, name)]
    if existing:
        cur.execute("TRUNCATE TABLE " + ", ".join(existing))
    return existing


def purge_eod_raw_storage(trade_date: str) -> None:
    """
    Preserve compact processed EOD tables but remove bulky raw imports after a
    successful result build. Keep the small participant history and the
    NIFTY-50-only cash bhavcopy history used by the STD-200 scanner.
    """
    if not env_bool("EOD_STORAGE_SAFE_PURGE", True):
        print("[EOD-STORAGE] Raw-data purge disabled by EOD_STORAGE_SAFE_PURGE=0", flush=True)
        return

    participant_days = max(5, int(os.getenv("EOD_PARTICIPANT_RAW_DAYS", "15")))
    cash_calendar_days = max(250, int(os.getenv("EOD_CASH_HISTORY_CALENDAR_DAYS", "450")))
    registry_days = max(7, int(os.getenv("EOD_FILE_REGISTRY_DAYS", "30")))

    conn = psycopg_connect("idxoptionsdata_current", autocommit=False)
    try:
        with conn.cursor() as cur:
            truncated = truncate_if_exists(
                cur,
                [
                    "nse_fo_bhavcopy_raw",
                    "nse_cm_bhavcopy_raw",
                    "nse_sec_bhavdata_raw",
                    "nse_fii_stats_raw",
                ],
            )

            participant_deleted = 0
            for table_name in ("nse_participant_oi_raw", "nse_participant_vol_raw"):
                if table_exists(cur, table_name):
                    cur.execute(
                        f"DELETE FROM {table_name} "
                        "WHERE trade_date < (CURRENT_DATE - %s::integer)",
                        (participant_days,),
                    )
                    participant_deleted += int(cur.rowcount or 0)

            registry_deleted = 0
            if table_exists(cur, "nse_eod_file_registry"):
                cur.execute(
                    "DELETE FROM nse_eod_file_registry "
                    "WHERE trade_date < (CURRENT_DATE - %s::integer)",
                    (registry_days,),
                )
                registry_deleted = int(cur.rowcount or 0)

            cash_deleted = 0
            cash_mode = "date-only"
            if table_exists(cur, "nse_bhavcopy_eod"):
                # Prefer the live-worker's current NIFTY 50 constituent table.
                # This keeps about 50 x 200 sessions instead of the full market.
                if table_exists(cur, "cash.nifty50_cash_constituents"):
                    cur.execute(
                        "DELETE FROM nse_bhavcopy_eod b "
                        "WHERE NOT EXISTS ("
                        "  SELECT 1 FROM cash.nifty50_cash_constituents c "
                        "  WHERE UPPER(c.symbol) = UPPER(b.symbol)"
                        ")"
                    )
                    cash_deleted += int(cur.rowcount or 0)
                    cash_mode = "NIFTY50-only"
                cur.execute(
                    "DELETE FROM nse_bhavcopy_eod "
                    "WHERE trade_date < (CURRENT_DATE - %s::integer)",
                    (cash_calendar_days,),
                )
                cash_deleted += int(cur.rowcount or 0)

        conn.commit()
        print(
            f"[EOD-STORAGE] success date={trade_date} truncated={truncated} "
            f"participant_deleted={participant_deleted} registry_deleted={registry_deleted} "
            f"cash_deleted={cash_deleted} cash_mode={cash_mode}",
            flush=True,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    here = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("APP_DATA_DIR", str(here / "data")))
    eod_dir = data_dir / "nse_eod_reports"
    eod_dir.mkdir(parents=True, exist_ok=True)
    trade_date = datetime.now(IST).date().isoformat()

    command = [
        sys.executable,
        str(here / "eod_import.py"),
        "--date", trade_date,
        "--symbols", "NIFTY",
        "--database", "idxoptionsdata_current",
        "--base-dir", str(data_dir),
        "--eod-dir", str(eod_dir),
        "--dashboard-script", str(here / "dashboard.py"),
        "--non-interactive",
        "--no-install-deps",
    ]
    print(f"[EOD] Running automatic NSE EOD processing for {trade_date} at 20:00 IST", flush=True)
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        print(
            f"[EOD] Import returned code {completed.returncode}. On NSE holidays this can mean "
            "reports were not published. Raw tables were not purged.",
            flush=True,
        )
        return completed.returncode

    try:
        purge_eod_raw_storage(trade_date)
    except Exception as exc:
        # Processing succeeded, so preserve the EOD success status but make the
        # cleanup failure prominent in Railway logs.
        print(f"[EOD-STORAGE] WARNING purge failed: {type(exc).__name__}: {exc}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
