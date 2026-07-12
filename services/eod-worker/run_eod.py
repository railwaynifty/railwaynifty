from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def main() -> int:
    here = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("APP_DATA_DIR", str(here / "data")))
    eod_dir = data_dir / "nse_eod_reports"
    eod_dir.mkdir(parents=True, exist_ok=True)
    trade_date = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()

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
        print(f"[EOD] Import returned code {completed.returncode}. On NSE holidays this can mean reports were not published.", flush=True)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
