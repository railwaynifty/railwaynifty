from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def _run_once(here: Path, trade_date: str, data_dir: Path, eod_dir: Path, attempt: int, max_attempts: int) -> int:
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
        "--force",
    ]
    print(
        f"[EOD] Attempt {attempt}/{max_attempts}: processing {trade_date} "
        f"at {datetime.now(IST):%d-%b-%Y %H:%M:%S IST}",
        flush=True,
    )
    completed = subprocess.run(command, check=False)
    print(f"[EOD] Attempt {attempt}/{max_attempts} returned code {completed.returncode}.", flush=True)
    return int(completed.returncode)


def main() -> int:
    here = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("APP_DATA_DIR", str(here / "data")))
    eod_dir = data_dir / "nse_eod_reports"
    eod_dir.mkdir(parents=True, exist_ok=True)

    trade_date = os.getenv("EOD_TRADE_DATE", "").strip() or datetime.now(IST).date().isoformat()
    retry_minutes = max(1, int(os.getenv("EOD_RETRY_MINUTES", "15")))
    max_attempts = max(1, int(os.getenv("EOD_MAX_ATTEMPTS", "5")))

    print(
        f"[EOD] Automatic NSE EOD processing started for {trade_date}. "
        f"Maximum attempts={max_attempts}, retry interval={retry_minutes} minutes.",
        flush=True,
    )

    last_code = 1
    for attempt in range(1, max_attempts + 1):
        last_code = _run_once(here, trade_date, data_dir, eod_dir, attempt, max_attempts)
        if last_code == 0:
            print(f"[EOD] Full EOD processing completed successfully for {trade_date}.", flush=True)
            return 0

        if attempt < max_attempts:
            wait_seconds = retry_minutes * 60
            next_run = datetime.now(IST).timestamp() + wait_seconds
            next_label = datetime.fromtimestamp(next_run, IST).strftime("%H:%M:%S IST")
            print(
                f"[EOD] Processing incomplete. Retrying in {retry_minutes} minutes "
                f"(around {next_label}).",
                flush=True,
            )
            time.sleep(wait_seconds)

    print(
        f"[EOD] All {max_attempts} attempts failed for {trade_date}. "
        "On an NSE holiday this is expected; otherwise inspect the final import logs.",
        flush=True,
    )
    return last_code or 2


if __name__ == "__main__":
    raise SystemExit(main())
