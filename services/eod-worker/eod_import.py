# -*- coding: utf-8 -*-
"""
NSE EOD Import + Processing + DB Cache Builder

Purpose
-------
1) Import NSE EOD files into PostgreSQL raw tables.
2) Calculate daily institutional / cash / FO / CM regime results once.
3) Store result tables and a JSON payload cache used by the dashboard.
4) Calculate 200-day standard-deviation volume spurt scan for cash stocks.

Default DB
----------
The default database is idxoptionsdata_current so EOD analytics/cache
stay with the live option dashboard database. Override with --database if required.

Typical run
-----------
cd C:\\IntradayOC\\Datafile
python nse_eod_import_process_to_db.py --all

Then run atm_roc_dashboard_360_live_eod_db_cached_8100.py. The EOD Participation tab will read cached results from
PostgreSQL first and only fall back to file parsing when cache is missing.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import sys
import zipfile
import shutil
import tempfile
import subprocess
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener
from http.cookiejar import CookieJar

import pandas as pd
import psycopg2
from cloud_db import psycopg_connect, ensure_logical_database
from psycopg2.extras import Json, execute_values


# -------------------- DEFAULT CONFIG --------------------
BASE_DIR = os.getenv("APP_DATA_DIR", str(Path(__file__).resolve().parent / "data"))
os.makedirs(BASE_DIR, exist_ok=True)
DEFAULT_EOD_DIR = os.path.join(BASE_DIR, "nse_eod_reports")
DEFAULT_DBINFO_PATH = os.path.join(BASE_DIR, "dbinfo.txt")
DEFAULT_DB_NAME = "idxoptionsdata_current"
DEFAULT_SYMBOLS = ("NIFTY",)
DEFAULT_DASHBOARD_SCRIPT = str(Path(__file__).resolve().parent / "dashboard.py")

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# 200-day STD deviation volume-spurt logic. Weekly/monthly lookbacks are kept
# from the user's scanner for practical historical requirements.
DAILY_LOOKBACK = 200
WEEKLY_LOOKBACK = 26
MONTHLY_LOOKBACK = 12
MIN_NORM_VOL = 6.0


# -------------------- GENERAL HELPERS --------------------
def read_dbinfo(path: str) -> Tuple[str, str, str]:
    """Read 3-line dbinfo.txt: user, password, host."""
    text = Path(path).read_text(encoding="utf-8").split()
    if len(text) < 3:
        raise ValueError(f"dbinfo.txt must contain user, password, host. Got: {path}")
    return text[0], text[1], text[2]


def connect(dbinfo_path: str, database: str, autocommit: bool = False):
    return psycopg_connect(database, autocommit=autocommit)


def ensure_database(dbinfo_path: str, database: str):
    """Railway uses one PostgreSQL database; logical databases map to schemas."""
    ensure_logical_database(database)

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_col(name: Any) -> str:
    return re.sub(r"\s+", " ", str(name).strip())


def to_num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value in {"", "-", "xx", "None", "nan", "NaN"}:
                return None
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except Exception:
        return None


def to_int(value: Any) -> Optional[int]:
    number = to_num(value)
    if number is None:
        return None
    return int(round(number))


def parse_date_any(value: Any) -> Optional[date]:
    """Parse dates like 2026-05-27, 27-05-2026, 27052026, or 270526."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.lower() in {"none", "nan", "nat", "-", "xx"}:
        return None
    raw = raw.split()[0]

    # Common compact NSE/backfill formats requested by user.
    if re.fullmatch(r"\d{6}", raw):
        try:
            return datetime.strptime(raw, "%d%m%y").date()
        except Exception:
            pass
    if re.fullmatch(r"\d{8}", raw):
        for fmt in ("%d%m%Y", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except Exception:
                pass

    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            pass
    try:
        parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True)
        if pd.notna(parsed):
            return parsed.date()
    except Exception:
        pass
    return None


def normalize_date_arg(value: Any) -> str:
    parsed = parse_date_any(value)
    if parsed is None:
        raise ValueError(f"Could not parse date: {value!r}. Use YYYY-MM-DD, DDMMYYYY, or DDMMYY.")
    return parsed.isoformat()


def calendar_date_range(start_value: Any, end_value: Any) -> List[str]:
    start = parse_date_any(start_value)
    end = parse_date_any(end_value)
    if start is None or end is None:
        raise ValueError("Both --from-date and --to-date are required and must be valid dates.")
    if start > end:
        start, end = end, start
    out: List[str] = []
    day = start
    while day <= end:
        out.append(day.isoformat())
        day += timedelta(days=1)
    return out


def normalize_index_symbol(value: str) -> str:
    """Normalize common user/display aliases to NSE F&O ticker symbols."""
    symbol = str(value or "").strip().upper().replace(" ", "")
    aliases = {
        "MIDCAPNIFTY": "MIDCPNIFTY",
        "NIFTYMIDCAP": "MIDCPNIFTY",
        "MIDCAP": "MIDCPNIFTY",
    }
    return aliases.get(symbol, symbol)


def date_tokens(day: str | date) -> Dict[str, str]:
    parsed = parse_date_any(day)
    if parsed is None:
        raise ValueError(f"Invalid date: {day!r}")
    d = datetime.combine(parsed, datetime.min.time())
    return {
        "date": d.strftime("%Y-%m-%d"),
        "yyyymmdd": d.strftime("%Y%m%d"),
        "ddmmyyyy": d.strftime("%d%m%Y"),
        "dd-Mon-yyyy": d.strftime("%d-%b-%Y"),
    }


def pct_change(current: Any, previous: Any) -> Optional[float]:
    c = to_num(current)
    p = to_num(previous)
    if c is None or p in (None, 0):
        return None
    return ((c - p) / abs(p)) * 100.0


def json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj
    if isinstance(obj, (date, datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return str(obj)


def row_json(row: pd.Series) -> Dict[str, Any]:
    return {str(k): json_safe(v) for k, v in row.to_dict().items()}


# -------------------- NSE DOWNLOAD HELPERS --------------------
def nse_report_urls(day: str | date) -> List[Tuple[str, str, bool]]:
    tok = date_tokens(day)
    return [
        (f"fao_participant_oi_{tok['ddmmyyyy']}.csv", f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{tok['ddmmyyyy']}.csv", True),
        (f"fao_participant_vol_{tok['ddmmyyyy']}.csv", f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{tok['ddmmyyyy']}.csv", True),
        (f"fii_stats_{tok['dd-Mon-yyyy']}.xls", f"https://nsearchives.nseindia.com/content/fo/fii_stats_{tok['dd-Mon-yyyy']}.xls", False),
        (f"BhavCopy_NSE_FO_0_0_0_{tok['yyyymmdd']}_F_0000.csv.zip", f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{tok['yyyymmdd']}_F_0000.csv.zip", True),
        (f"BhavCopy_NSE_CM_0_0_0_{tok['yyyymmdd']}_F_0000.csv.zip", f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{tok['yyyymmdd']}_F_0000.csv.zip", True),
        (f"sec_bhavdata_full_{tok['ddmmyyyy']}.csv", f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{tok['ddmmyyyy']}.csv", True),
    ]


def build_nse_opener():
    opener = build_opener(HTTPCookieProcessor(CookieJar()))
    try:
        opener.open(Request("https://www.nseindia.com/", headers=NSE_HEADERS), timeout=20).read()
    except Exception:
        pass
    return opener


def download_one_file(opener, url: str, path: Path, force: bool = False) -> bool:
    if path.exists() and path.stat().st_size > 0 and not force:
        return True
    request = Request(url, headers=NSE_HEADERS)
    with opener.open(request, timeout=45) as response:
        if getattr(response, "status", 200) != 200:
            return False
        content = response.read()
    if not content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


def download_day_reports(opener, day: str | date, output_dir: str | Path, force: bool = False) -> Tuple[bool, List[str]]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    messages: List[str] = []
    required_ok = True
    for filename, url, required in nse_report_urls(day):
        path = output_dir / filename
        try:
            ok = download_one_file(opener, url, path, force=force)
        except (OSError, URLError, TimeoutError) as exc:
            ok = False
            messages.append(f"{filename}: {type(exc).__name__}: {exc}")
        except Exception as exc:
            ok = False
            messages.append(f"{filename}: {type(exc).__name__}: {exc}")
        else:
            messages.append(f"{filename}: {'OK' if ok else 'missing'}")
        if required and not ok:
            required_ok = False
    return required_ok, messages


def download_recent_sessions(end_date: str | date, sessions: int, output_dir: str | Path, force: bool = False, max_calendar_days: int = 30) -> List[str]:
    opener = build_nse_opener()
    day = pd.to_datetime(end_date).date()
    successes = 0
    tried = 0
    dates: List[str] = []
    while successes < sessions and tried < max_calendar_days:
        ok, messages = download_day_reports(opener, day, output_dir, force=force)
        print(f"{day.isoformat()}: {'SESSION OK' if ok else 'skip'}")
        for message in messages:
            print(f"  {message}")
        if ok:
            successes += 1
            dates.append(day.isoformat())
        day -= timedelta(days=1)
        tried += 1
    return sorted(dates)


def ensure_reports_available(dates: Sequence[str], direct: bool, eod_dir: str, keep_downloads: bool, force_download: bool, sessions: int = 0, end_date: Optional[str] = None) -> Tuple[str, Optional[str], List[str]]:
    """
    Returns (working_eod_dir, temp_dir_to_cleanup, dates_with_reports).
    In direct mode, files are downloaded from NSE into a temporary staging folder
    unless keep_downloads=True, in which case eod_dir is used as the staging path.
    """
    if not direct:
        return eod_dir, None, list(dates)

    if keep_downloads:
        staging_dir = eod_dir
        temp_dir = None
    else:
        temp_dir = tempfile.mkdtemp(prefix="nse_eod_direct_")
        staging_dir = temp_dir

    opener = build_nse_opener()
    success_dates: List[str] = []
    if dates:
        for d in dates:
            ok, messages = download_day_reports(opener, d, staging_dir, force=force_download)
            print(f"{d}: {'SESSION OK' if ok else 'download failed'}")
            for message in messages:
                print(f"  {message}")
            if ok:
                success_dates.append(str(pd.to_datetime(d).date()))
    elif sessions > 0:
        anchor = end_date or date.today().isoformat()
        success_dates = download_recent_sessions(anchor, sessions=sessions, output_dir=staging_dir, force=force_download)

    return staging_dir, temp_dir, sorted(success_dates)


# -------------------- FILE DISCOVERY --------------------
def search_dirs(eod_dir: str, base_dir: str) -> List[Path]:
    candidates = [Path(eod_dir), Path(base_dir), Path("/mnt/data")]
    out, seen = [], set()
    for folder in candidates:
        try:
            key = str(folder.resolve())
        except Exception:
            key = str(folder)
        if key not in seen and folder.exists():
            out.append(folder)
            seen.add(key)
    return out


def find_file(kind: str, trade_date: str, eod_dir: str, base_dir: str) -> Optional[Path]:
    tok = date_tokens(trade_date)
    patterns = {
        "participant_oi": [f"fao_participant_oi_{tok['ddmmyyyy']}*.csv"],
        "participant_vol": [f"fao_participant_vol_{tok['ddmmyyyy']}*.csv"],
        "fo_bhavcopy": [
            f"BhavCopy_NSE_FO_0_0_0_{tok['yyyymmdd']}_F_0000*.csv.zip",
            f"BhavCopy_NSE_FO_0_0_0_{tok['yyyymmdd']}_F_0000*.csv",
        ],
        "cm_bhavcopy": [
            f"BhavCopy_NSE_CM_0_0_0_{tok['yyyymmdd']}_F_0000*.csv.zip",
            f"BhavCopy_NSE_CM_0_0_0_{tok['yyyymmdd']}_F_0000*.csv",
        ],
        "sec_bhav": [f"sec_bhavdata_full_{tok['ddmmyyyy']}*.csv"],
        "fii_stats": [f"fii_stats_{tok['dd-Mon-yyyy']}*.xls"],
    }.get(kind, [])

    matches: List[Path] = []
    for folder in search_dirs(eod_dir, base_dir):
        for pattern in patterns:
            matches.extend(folder.glob(pattern))
    matches = [p for p in matches if p.exists()]
    if not matches:
        return None
    return sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def discover_dates(eod_dir: str, base_dir: str) -> List[str]:
    dates = set()
    for folder in search_dirs(eod_dir, base_dir):
        for pattern in [
            "fao_participant_oi_*.csv",
            "fao_participant_vol_*.csv",
            "sec_bhavdata_full_*.csv",
        ]:
            for path in folder.glob(pattern):
                match = re.search(r"(\d{8})", path.name)
                if match:
                    fmt = "%d%m%Y"
                    try:
                        dates.add(datetime.strptime(match.group(1), fmt).strftime("%Y-%m-%d"))
                    except Exception:
                        pass
        for pattern in ["BhavCopy_NSE_FO_0_0_0_*_F_0000*", "BhavCopy_NSE_CM_0_0_0_*_F_0000*"]:
            for path in folder.glob(pattern):
                match = re.search(r"_(\d{8})_F_0000", path.name)
                if match:
                    try:
                        dates.add(datetime.strptime(match.group(1), "%Y%m%d").strftime("%Y-%m-%d"))
                    except Exception:
                        pass
    return sorted(dates)


def read_csv_or_zip(path: Path, **kwargs) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                return pd.DataFrame()
            with zf.open(names[0]) as handle:
                return pd.read_csv(handle, **kwargs)
    return pd.read_csv(path, **kwargs)


# -------------------- OPTIONAL PYTHON DEPENDENCIES --------------------
def ensure_optional_dependencies(auto_install: bool = True):
    """Ensure optional parsers for NSE legacy .xls/html tables are available.

    NSE fii_stats_*.xls is a real old Excel/BIFF file, so pandas needs xlrd.
    html5lib/lxml are only fallbacks for HTML-disguised files.
    """
    requirements = [
        ("xlrd", "xlrd>=2.0.1"),
        ("html5lib", "html5lib"),
        ("lxml", "lxml"),
    ]
    missing = []
    for module_name, package_spec in requirements:
        try:
            __import__(module_name)
        except Exception:
            missing.append(package_spec)
    if not missing:
        return
    if not auto_install:
        print("Optional parser packages missing: " + ", ".join(missing))
        print("Install them with: python -m pip install " + " ".join(missing))
        return
    print("Installing optional parser packages for FII stats: " + ", ".join(missing))
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    except Exception as exc:
        print(f"WARNING: automatic dependency install failed: {type(exc).__name__}: {exc}")
        print("Please run manually: python -m pip install " + " ".join(missing))


def count_existing_rows(conn, table: str, trade_date: str, file_hash: Optional[str] = None, date_col: str = "trade_date") -> int:
    cur = conn.cursor()
    try:
        if file_hash:
            cur.execute(f"SELECT count(*) FROM {table} WHERE {date_col} = %s AND file_hash = %s", (trade_date, file_hash))
        else:
            cur.execute(f"SELECT count(*) FROM {table} WHERE {date_col} = %s", (trade_date,))
        return int(cur.fetchone()[0] or 0)
    finally:
        cur.close()


# -------------------- DDL --------------------
def create_tables(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_eod_file_registry (
            id bigserial PRIMARY KEY,
            trade_date date NOT NULL,
            report_type text NOT NULL,
            file_name text NOT NULL,
            file_path text NOT NULL,
            file_hash text NOT NULL,
            file_size bigint,
            imported_at timestamp DEFAULT now(),
            rows_imported integer DEFAULT 0,
            status text DEFAULT 'imported',
            message text,
            UNIQUE (report_type, trade_date, file_hash)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_participant_oi_raw (
            trade_date date NOT NULL,
            client_type text NOT NULL,
            future_index_long numeric, future_index_short numeric,
            future_stock_long numeric, future_stock_short numeric,
            option_index_call_long numeric, option_index_put_long numeric,
            option_index_call_short numeric, option_index_put_short numeric,
            option_stock_call_long numeric, option_stock_put_long numeric,
            option_stock_call_short numeric, option_stock_put_short numeric,
            total_long_contracts numeric, total_short_contracts numeric,
            file_hash text, row_no integer, raw jsonb,
            imported_at timestamp DEFAULT now(),
            PRIMARY KEY (trade_date, client_type)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_participant_vol_raw (
            trade_date date NOT NULL,
            client_type text NOT NULL,
            future_index_long numeric, future_index_short numeric,
            future_stock_long numeric, future_stock_short numeric,
            option_index_call_long numeric, option_index_put_long numeric,
            option_index_call_short numeric, option_index_put_short numeric,
            option_stock_call_long numeric, option_stock_put_long numeric,
            option_stock_call_short numeric, option_stock_put_short numeric,
            total_long_contracts numeric, total_short_contracts numeric,
            file_hash text, row_no integer, raw jsonb,
            imported_at timestamp DEFAULT now(),
            PRIMARY KEY (trade_date, client_type)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_fo_bhavcopy_raw (
            id bigserial PRIMARY KEY,
            trade_date date NOT NULL,
            symbol text, instrument_type text, expiry_date date,
            strike_price numeric, option_type text, instrument_name text,
            open_price numeric, high_price numeric, low_price numeric,
            close_price numeric, last_price numeric, prev_close numeric,
            underlying_price numeric, settlement_price numeric,
            open_interest numeric, change_oi numeric,
            traded_volume numeric, turnover_value numeric, no_of_trades numeric,
            file_hash text, row_no integer, raw jsonb,
            imported_at timestamp DEFAULT now(),
            UNIQUE(file_hash, row_no)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fo_raw_date_symbol ON nse_fo_bhavcopy_raw(trade_date, symbol, instrument_type, expiry_date);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_cm_bhavcopy_raw (
            id bigserial PRIMARY KEY,
            trade_date date NOT NULL,
            symbol text, series text, instrument_type text,
            open_price numeric, high_price numeric, low_price numeric,
            close_price numeric, last_price numeric, prev_close numeric,
            traded_volume numeric, turnover_value numeric, no_of_trades numeric,
            file_hash text, row_no integer, raw jsonb,
            imported_at timestamp DEFAULT now(),
            UNIQUE(file_hash, row_no)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cm_raw_date_symbol ON nse_cm_bhavcopy_raw(trade_date, symbol, series);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_sec_bhavdata_raw (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            series text NOT NULL,
            prev_close numeric, open_price numeric, high_price numeric, low_price numeric,
            last_price numeric, close_price numeric, avg_price numeric,
            traded_volume bigint, turnover_lacs numeric, no_of_trades bigint,
            delivery_qty bigint, delivery_pct numeric,
            file_hash text, row_no integer, raw jsonb,
            imported_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol, series)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_fii_stats_raw (
            id bigserial PRIMARY KEY,
            trade_date date NOT NULL,
            row_no integer NOT NULL,
            data jsonb,
            file_hash text,
            imported_at timestamp DEFAULT now(),
            UNIQUE(file_hash, row_no)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_fii_derivatives_stats_daily (
            trade_date date NOT NULL,
            segment text NOT NULL,
            buy_contracts numeric,
            buy_amt_cr numeric,
            sell_contracts numeric,
            sell_amt_cr numeric,
            oi_contracts numeric,
            oi_amt_cr numeric,
            file_hash text,
            raw jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, segment)
        );
        """
    )
    # Compatibility table for the user's volume-spurt scanner.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_bhavcopy_eod (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            open numeric,
            high numeric,
            low numeric,
            close numeric,
            volume bigint,
            created_at timestamp DEFAULT now(),
            PRIMARY KEY (trade_date, symbol)
        );
        """
    )

    # Result tables.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_participant_bias_daily (
            trade_date date NOT NULL,
            participant text NOT NULL,
            raw_participant text,
            direction text,
            score numeric,
            score_change numeric,
            future_net numeric,
            future_net_change numeric,
            future_score numeric,
            option_score numeric,
            buyer_score numeric,
            writer_score numeric,
            call_short_minus_put_short numeric,
            index_option_volume numeric,
            index_option_volume_score numeric,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, participant)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_option_wall_daily (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            expiry_date text NOT NULL,
            side text NOT NULL,
            strike numeric NOT NULL,
            wall_kind text,
            build text,
            meaning text,
            activity_score numeric,
            oi numeric,
            coi numeric,
            volume numeric,
            close numeric,
            price_change numeric,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol, expiry_date, side, strike, COALESCE(wall_kind, 'ROW'))
        );
        """.replace("PRIMARY KEY(trade_date, symbol, expiry_date, side, strike, COALESCE(wall_kind, 'ROW'))", "UNIQUE(trade_date, symbol, expiry_date, side, strike, wall_kind)")
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_participant_expiry_attribution_daily (
            trade_date date NOT NULL,
            participant text NOT NULL,
            action text NOT NULL,
            symbol text NOT NULL,
            expiry_date text NOT NULL,
            estimated_contracts numeric,
            candidate_change_oi numeric,
            candidate_volume numeric,
            max_action_score numeric,
            top_strikes text,
            top_confidence text,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, participant, action, symbol, expiry_date)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_participant_strike_attribution_daily (
            trade_date date NOT NULL,
            participant text NOT NULL,
            raw_participant text,
            action text NOT NULL,
            action_meaning text,
            source_change_column text,
            action_rank integer,
            shown_in_top_report boolean,
            participant_action_contracts numeric,
            symbol text NOT NULL,
            expiry_date text NOT NULL,
            strike numeric NOT NULL,
            side text NOT NULL,
            estimated_participant_contracts numeric,
            share_of_participant_action_pct numeric,
            candidate_change_oi numeric,
            candidate_oi numeric,
            candidate_volume numeric,
            close numeric,
            price_change numeric,
            underlying numeric,
            build text,
            actor text,
            action_score numeric,
            confidence text,
            exact_price_confirm boolean,
            model_note text,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, participant, action, symbol, expiry_date, strike, side)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nse_part_expiry_attr ON nse_participant_expiry_attribution_daily(trade_date, participant, symbol, expiry_date, estimated_contracts DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nse_part_strike_attr ON nse_participant_strike_attribution_daily(trade_date, participant, symbol, expiry_date, action_rank);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_futures_build_up_daily (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            expiry_date text NOT NULL,
            direction text,
            build text,
            close numeric,
            price_change numeric,
            price_pct numeric,
            oi numeric,
            coi numeric,
            volume numeric,
            basis numeric,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol, expiry_date)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_cash_delivery_footprint_daily (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            signal text,
            price_pct numeric,
            close numeric,
            delivery_pct numeric,
            delivery_value_lacs numeric,
            turnover_lacs numeric,
            delivery_qty numeric,
            delivery_qty_change_pct numeric,
            volume_change_pct numeric,
            activity_score numeric,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_cm_participation_regime_daily (
            trade_date date PRIMARY KEY,
            regime text,
            conviction text,
            participation_score numeric,
            traded_value_crores numeric,
            traded_qty_lakhs numeric,
            no_of_trades numeric,
            securities_traded numeric,
            avg_trade_size numeric,
            traded_value_change_pct numeric,
            traded_qty_change_pct numeric,
            trades_change_pct numeric,
            avg_trade_size_change_pct numeric,
            payload jsonb,
            updated_at timestamp DEFAULT now()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_360_decision_daily (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            expiry_date text NOT NULL,
            spot_close numeric,
            fii_bias text,
            prop_bias text,
            big_money_direction text,
            big_money_score numeric,
            future_direction text,
            future_build text,
            ce_resistance_strike numeric,
            pe_support_strike numeric,
            cash_direction text,
            cm_regime text,
            final_360_bias text,
            confidence_score numeric,
            decision_text text,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol, expiry_date)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_360_memory_validation (
            signal_date date NOT NULL,
            validation_date date NOT NULL,
            symbol text NOT NULL,
            expected_bias text,
            big_money_score numeric,
            signal_spot numeric,
            next_spot numeric,
            move_pts numeric,
            move_pct numeric,
            ce_wall numeric,
            pe_wall numeric,
            decision text,
            validation text,
            reason text,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(signal_date, validation_date, symbol)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_volume_spurt_scan (
            scan_date date NOT NULL,
            timeframe text NOT NULL,
            symbol text NOT NULL,
            close numeric,
            volume bigint,
            norm_vol numeric,
            bubble text,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(scan_date, timeframe, symbol)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nse_volume_spurt_scan ON nse_volume_spurt_scan(scan_date, timeframe, norm_vol DESC);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_nt_calibration_daily (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            segment text NOT NULL,
            instrument_type text NOT NULL,
            option_type text NOT NULL DEFAULT '',
            moneyness_bucket text NOT NULL DEFAULT 'NA',
            premium_bucket text NOT NULL DEFAULT 'NA',
            dte_bucket text NOT NULL DEFAULT 'NA',
            contracts_per_trade numeric,
            row_count integer,
            median_traded_volume numeric,
            median_no_of_trades numeric,
            payload jsonb,
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol, segment, instrument_type, option_type, moneyness_bucket, premium_bucket, dte_bucket)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nse_nt_calibration_lookup ON nse_nt_calibration_daily(symbol, trade_date DESC, segment, option_type);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nse_eod_payload_cache (
            trade_date date NOT NULL,
            symbol text NOT NULL,
            expiry_date text NOT NULL DEFAULT '',
            payload jsonb NOT NULL,
            source text DEFAULT 'db_import_processor',
            updated_at timestamp DEFAULT now(),
            PRIMARY KEY(trade_date, symbol, expiry_date)
        );
        """
    )
    conn.commit()
    cur.close()


def register_file(conn, trade_date: str, report_type: str, path: Path, file_hash: str, rows: int, status: str = "imported", message: str = ""):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO nse_eod_file_registry
            (trade_date, report_type, file_name, file_path, file_hash, file_size, rows_imported, status, message)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (report_type, trade_date, file_hash)
        DO UPDATE SET imported_at = now(), rows_imported = EXCLUDED.rows_imported,
                      status = EXCLUDED.status, message = EXCLUDED.message;
        """,
        (trade_date, report_type, path.name, str(path), file_hash, path.stat().st_size, rows, status, message),
    )
    conn.commit()
    cur.close()


def already_imported(conn, report_type: str, trade_date: str, file_hash: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM nse_eod_file_registry WHERE report_type=%s AND trade_date=%s AND file_hash=%s AND status='imported'",
        (report_type, trade_date, file_hash),
    )
    out = cur.fetchone() is not None
    cur.close()
    return out


def delete_trade_date(conn, trade_date: str):
    cur = conn.cursor()
    trade_date_tables = [
        "nse_participant_oi_raw", "nse_participant_vol_raw", "nse_fo_bhavcopy_raw", "nse_cm_bhavcopy_raw",
        "nse_sec_bhavdata_raw", "nse_fii_stats_raw", "nse_fii_derivatives_stats_daily", "nse_bhavcopy_eod",
        "nse_participant_bias_daily", "nse_option_wall_daily", "nse_futures_build_up_daily",
        "nse_participant_expiry_attribution_daily", "nse_participant_strike_attribution_daily",
        "nse_cash_delivery_footprint_daily", "nse_cm_participation_regime_daily", "nse_360_decision_daily",
        "nse_nt_calibration_daily", "nse_eod_payload_cache",
    ]
    for table in trade_date_tables:
        cur.execute(f"DELETE FROM {table} WHERE trade_date = %s", (trade_date,))
    # This result table uses scan_date, not trade_date.
    cur.execute("DELETE FROM nse_volume_spurt_scan WHERE scan_date = %s", (trade_date,))
    cur.execute("DELETE FROM nse_360_memory_validation WHERE signal_date = %s OR validation_date = %s", (trade_date, trade_date))
    cur.execute("DELETE FROM nse_eod_file_registry WHERE trade_date = %s", (trade_date,))
    conn.commit()
    cur.close()


# -------------------- IMPORT RAW FILES --------------------
PART_COLS = {
    "Future Index Long": "future_index_long",
    "Future Index Short": "future_index_short",
    "Future Stock Long": "future_stock_long",
    "Future Stock Short": "future_stock_short",
    "Option Index Call Long": "option_index_call_long",
    "Option Index Put Long": "option_index_put_long",
    "Option Index Call Short": "option_index_call_short",
    "Option Index Put Short": "option_index_put_short",
    "Option Stock Call Long": "option_stock_call_long",
    "Option Stock Put Long": "option_stock_put_long",
    "Option Stock Call Short": "option_stock_call_short",
    "Option Stock Put Short": "option_stock_put_short",
    "Total Long Contracts": "total_long_contracts",
    "Total Short Contracts": "total_short_contracts",
}


def import_participant(conn, trade_date: str, kind: str, path: Path, force: bool = False) -> int:
    report_type = "participant_oi" if kind == "oi" else "participant_vol"
    table = "nse_participant_oi_raw" if kind == "oi" else "nse_participant_vol_raw"
    file_hash = sha256_file(path)
    if not force and already_imported(conn, report_type, trade_date, file_hash):
        return count_existing_rows(conn, table, trade_date, file_hash)
    df = pd.read_csv(path, skiprows=1)
    df.columns = [clean_col(c) for c in df.columns]
    df = df.rename(columns={c: PART_COLS.get(c, c) for c in df.columns})
    if "Client Type" in df.columns:
        df = df.rename(columns={"Client Type": "client_type"})
    rows = []
    for i, row in df.iterrows():
        client = str(row.get("client_type", "")).strip()
        if not client or client.lower() == "nan":
            continue
        rows.append((
            trade_date, client,
            *[to_num(row.get(col)) for col in PART_COLS.values()],
            file_hash, int(i), Json(row_json(row)),
        ))
    if rows:
        cols = [
            "trade_date", "client_type", *PART_COLS.values(), "file_hash", "row_no", "raw"
        ]
        sql = f"""
            INSERT INTO {table} ({','.join(cols)}) VALUES %s
            ON CONFLICT (trade_date, client_type) DO UPDATE SET
            {','.join([f'{c}=EXCLUDED.{c}' for c in PART_COLS.values()])},
            file_hash=EXCLUDED.file_hash, row_no=EXCLUDED.row_no, raw=EXCLUDED.raw, imported_at=now();
        """
        cur = conn.cursor()
        execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
        cur.close()
    register_file(conn, trade_date, report_type, path, file_hash, len(rows))
    return len(rows)


def import_fo_bhavcopy(conn, trade_date: str, path: Path, force: bool = False) -> int:
    file_hash = sha256_file(path)
    if not force and already_imported(conn, "fo_bhavcopy", trade_date, file_hash):
        return count_existing_rows(conn, "nse_fo_bhavcopy_raw", trade_date, file_hash)
    df = read_csv_or_zip(path)
    df.columns = [clean_col(c) for c in df.columns]
    rows = []
    for i, row in df.iterrows():
        rows.append((
            parse_date_any(row.get("TradDt")) or trade_date,
            str(row.get("TckrSymb", "")).strip().upper(),
            str(row.get("FinInstrmTp", "")).strip().upper(),
            parse_date_any(row.get("XpryDt")),
            to_num(row.get("StrkPric")),
            str(row.get("OptnTp", "")).strip().upper(),
            str(row.get("FinInstrmNm", "")).strip(),
            to_num(row.get("OpnPric")), to_num(row.get("HghPric")), to_num(row.get("LwPric")),
            to_num(row.get("ClsPric")), to_num(row.get("LastPric")), to_num(row.get("PrvsClsgPric")),
            to_num(row.get("UndrlygPric")), to_num(row.get("SttlmPric")),
            to_num(row.get("OpnIntrst")), to_num(row.get("ChngInOpnIntrst")),
            to_num(row.get("TtlTradgVol")), to_num(row.get("TtlTrfVal")), to_num(row.get("TtlNbOfTxsExctd")),
            file_hash, int(i), Json(row_json(row)),
        ))
    if rows:
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO nse_fo_bhavcopy_raw
            (trade_date,symbol,instrument_type,expiry_date,strike_price,option_type,instrument_name,
             open_price,high_price,low_price,close_price,last_price,prev_close,underlying_price,settlement_price,
             open_interest,change_oi,traded_volume,turnover_value,no_of_trades,file_hash,row_no,raw)
            VALUES %s ON CONFLICT(file_hash,row_no) DO NOTHING;
        """, rows, page_size=5000)
        conn.commit()
        cur.close()
    register_file(conn, trade_date, "fo_bhavcopy", path, file_hash, len(rows))
    return len(rows)


def import_cm_bhavcopy(conn, trade_date: str, path: Path, force: bool = False) -> int:
    file_hash = sha256_file(path)
    if not force and already_imported(conn, "cm_bhavcopy", trade_date, file_hash):
        return count_existing_rows(conn, "nse_cm_bhavcopy_raw", trade_date, file_hash)
    df = read_csv_or_zip(path)
    df.columns = [clean_col(c) for c in df.columns]
    rows = []
    bhav_rows = []
    for i, row in df.iterrows():
        td = parse_date_any(row.get("TradDt")) or pd.to_datetime(trade_date).date()
        symbol = str(row.get("TckrSymb", "")).strip().upper()
        series = str(row.get("SctySrs", "")).strip().upper()
        rows.append((
            td, symbol, series, str(row.get("FinInstrmTp", "")).strip().upper(),
            to_num(row.get("OpnPric")), to_num(row.get("HghPric")), to_num(row.get("LwPric")),
            to_num(row.get("ClsPric")), to_num(row.get("LastPric")), to_num(row.get("PrvsClsgPric")),
            to_num(row.get("TtlTradgVol")), to_num(row.get("TtlTrfVal")), to_num(row.get("TtlNbOfTxsExctd")),
            file_hash, int(i), Json(row_json(row)),
        ))
        if series == "EQ" and symbol:
            bhav_rows.append((td, symbol, to_num(row.get("OpnPric")), to_num(row.get("HghPric")), to_num(row.get("LwPric")), to_num(row.get("ClsPric")), to_int(row.get("TtlTradgVol"))))
    cur = conn.cursor()
    if rows:
        execute_values(cur, """
            INSERT INTO nse_cm_bhavcopy_raw
            (trade_date,symbol,series,instrument_type,open_price,high_price,low_price,close_price,last_price,prev_close,
             traded_volume,turnover_value,no_of_trades,file_hash,row_no,raw)
            VALUES %s ON CONFLICT(file_hash,row_no) DO NOTHING;
        """, rows, page_size=5000)
    if bhav_rows:
        execute_values(cur, """
            INSERT INTO nse_bhavcopy_eod(trade_date,symbol,open,high,low,close,volume)
            VALUES %s
            ON CONFLICT(trade_date,symbol) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume;
        """, bhav_rows, page_size=5000)
    conn.commit()
    cur.close()
    register_file(conn, trade_date, "cm_bhavcopy", path, file_hash, len(rows))
    return len(rows)


def import_sec_bhav(conn, trade_date: str, path: Path, force: bool = False) -> int:
    file_hash = sha256_file(path)
    if not force and already_imported(conn, "sec_bhav", trade_date, file_hash):
        return count_existing_rows(conn, "nse_sec_bhavdata_raw", trade_date, file_hash)
    df = pd.read_csv(path)
    df.columns = [clean_col(c) for c in df.columns]
    rows = []
    bhav_rows = []
    for i, row in df.iterrows():
        td = parse_date_any(row.get("DATE1")) or pd.to_datetime(trade_date).date()
        symbol = str(row.get("SYMBOL", "")).strip().upper()
        series = str(row.get("SERIES", "")).strip().upper()
        rows.append((
            td, symbol, series,
            to_num(row.get("PREV_CLOSE")), to_num(row.get("OPEN_PRICE")), to_num(row.get("HIGH_PRICE")), to_num(row.get("LOW_PRICE")),
            to_num(row.get("LAST_PRICE")), to_num(row.get("CLOSE_PRICE")), to_num(row.get("AVG_PRICE")),
            to_int(row.get("TTL_TRD_QNTY")), to_num(row.get("TURNOVER_LACS")), to_int(row.get("NO_OF_TRADES")),
            to_int(row.get("DELIV_QTY")), to_num(row.get("DELIV_PER")),
            file_hash, int(i), Json(row_json(row)),
        ))
        if series == "EQ" and symbol:
            bhav_rows.append((td, symbol, to_num(row.get("OPEN_PRICE")), to_num(row.get("HIGH_PRICE")), to_num(row.get("LOW_PRICE")), to_num(row.get("CLOSE_PRICE")), to_int(row.get("TTL_TRD_QNTY"))))
    cur = conn.cursor()
    if rows:
        execute_values(cur, """
            INSERT INTO nse_sec_bhavdata_raw
            (trade_date,symbol,series,prev_close,open_price,high_price,low_price,last_price,close_price,avg_price,
             traded_volume,turnover_lacs,no_of_trades,delivery_qty,delivery_pct,file_hash,row_no,raw)
            VALUES %s
            ON CONFLICT(trade_date,symbol,series) DO UPDATE SET
             prev_close=EXCLUDED.prev_close, open_price=EXCLUDED.open_price, high_price=EXCLUDED.high_price,
             low_price=EXCLUDED.low_price, last_price=EXCLUDED.last_price, close_price=EXCLUDED.close_price,
             avg_price=EXCLUDED.avg_price, traded_volume=EXCLUDED.traded_volume, turnover_lacs=EXCLUDED.turnover_lacs,
             no_of_trades=EXCLUDED.no_of_trades, delivery_qty=EXCLUDED.delivery_qty, delivery_pct=EXCLUDED.delivery_pct,
             file_hash=EXCLUDED.file_hash, row_no=EXCLUDED.row_no, raw=EXCLUDED.raw, imported_at=now();
        """, rows, page_size=5000)
    if bhav_rows:
        execute_values(cur, """
            INSERT INTO nse_bhavcopy_eod(trade_date,symbol,open,high,low,close,volume)
            VALUES %s
            ON CONFLICT(trade_date,symbol) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume;
        """, bhav_rows, page_size=5000)
    conn.commit()
    cur.close()
    register_file(conn, trade_date, "sec_bhav", path, file_hash, len(rows))
    return len(rows)


def _read_fii_stats_frames(path: Path) -> Tuple[List[pd.DataFrame], str]:
    """Read NSE fii_stats old .xls files with multiple fallbacks.

    NSE's fii_stats_*.xls is a legacy Excel/BIFF file. pandas needs xlrd for
    direct parsing. If xlrd is not installed, this function tries LibreOffice
    headless conversion to CSV. This prevents silent 0-row imports.
    """
    errors: List[str] = []

    # 1) Preferred: pandas + xlrd/openpyxl auto detection.
    try:
        df = pd.read_excel(path, header=None)
        if not df.empty:
            return [df], ""
    except Exception as exc:
        errors.append(f"read_excel: {type(exc).__name__}: {exc}")

    # 2) Some NSE files are HTML disguised as xls.
    try:
        frames = pd.read_html(path)
        frames = [frame for frame in frames if frame is not None and not frame.empty]
        if frames:
            return frames, ""
    except Exception as exc:
        errors.append(f"read_html: {type(exc).__name__}: {exc}")

    # 3) Last robust fallback: LibreOffice conversion to CSV, if installed.
    soffice = shutil.which("libreoffice") or shutil.which("soffice")
    if soffice:
        tmpdir = tempfile.mkdtemp(prefix="fii_stats_xls_")
        try:
            cmd = [soffice, "--headless", "--convert-to", "csv", "--outdir", tmpdir, str(path)]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=90)
            csv_files = sorted(Path(tmpdir).glob("*.csv"))
            if csv_files:
                df = pd.read_csv(csv_files[0], header=None)
                if not df.empty:
                    return [df], ""
            errors.append(f"libreoffice: rc={proc.returncode}; stdout={proc.stdout.strip()}; stderr={proc.stderr.strip()}")
        except Exception as exc:
            errors.append(f"libreoffice: {type(exc).__name__}: {exc}")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
    else:
        errors.append("libreoffice/soffice not found")

    return [], "; ".join(errors)


def _clean_fii_raw_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out = out.dropna(axis=0, how="all").dropna(axis=1, how="all")
    out = out.replace({pd.NA: None})
    # Make generic stable JSON keys because source rows are layout-oriented.
    out.columns = [f"c{i + 1}" for i in range(len(out.columns))]
    return out


def _normalize_fii_stats_rows(frame: pd.DataFrame, trade_date: str, file_hash: str) -> List[Tuple[Any, ...]]:
    """Normalize FII derivatives stats rows into one row per segment.

    Expected NSE layout after cleanup:
      c1 Segment
      c2 Buy contracts
      c3 Buy amt cr
      c4 Sell contracts
      c5 Sell amt cr
      c6 OI contracts
      c7 OI amt cr
    """
    rows: List[Tuple[Any, ...]] = []
    if frame.empty:
        return rows
    for _, row in frame.iterrows():
        segment = str(row.get("c1") or "").strip()
        if not segment:
            continue
        upper = segment.upper()
        if upper.startswith("FII DERIVATIVES") or upper in {"NOTES:", "NOTES"}:
            continue
        if "BUY" in upper or "NO. OF CONTRACTS" in upper or "BOTH BUY" in upper:
            continue
        if upper.startswith("OPTIONS VALUE") or upper.startswith("FUTURES VALUE") or upper.startswith("VALUE &"):
            continue
        # Keep only real market rows having at least one numeric value.
        vals = [to_num(row.get(f"c{i}")) for i in range(2, 8)]
        if not any(v is not None for v in vals):
            continue
        raw = {str(k): json_safe(v) for k, v in row.to_dict().items()}
        rows.append((
            trade_date,
            upper,
            vals[0], vals[1], vals[2], vals[3], vals[4], vals[5],
            file_hash,
            Json(raw),
        ))
    return rows


def import_fii_stats(conn, trade_date: str, path: Path, force: bool = False) -> int:
    file_hash = sha256_file(path)
    if not force and already_imported(conn, "fii_stats", trade_date, file_hash):
        # Return actual existing row count for clearer logs, not 0.
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM nse_fii_stats_raw WHERE trade_date = %s AND file_hash = %s", (trade_date, file_hash))
        count = int(cur.fetchone()[0] or 0)
        cur.close()
        return count

    frames, error = _read_fii_stats_frames(path)
    rows = []
    norm_rows: List[Tuple[Any, ...]] = []
    row_no = 0
    for frame in frames:
        clean = _clean_fii_raw_frame(frame)
        norm_rows.extend(_normalize_fii_stats_rows(clean, trade_date, file_hash))
        for _, row in clean.iterrows():
            # Skip completely empty rows after cleanup.
            data = row_json(row)
            if not any(v not in (None, "", "nan") for v in data.values()):
                continue
            rows.append((trade_date, row_no, Json(data), file_hash))
            row_no += 1

    cur = conn.cursor()
    if force:
        cur.execute("DELETE FROM nse_fii_stats_raw WHERE trade_date = %s", (trade_date,))
        cur.execute("DELETE FROM nse_fii_derivatives_stats_daily WHERE trade_date = %s", (trade_date,))
    if rows:
        execute_values(cur, """
            INSERT INTO nse_fii_stats_raw(trade_date,row_no,data,file_hash)
            VALUES %s ON CONFLICT(file_hash,row_no) DO NOTHING;
        """, rows, page_size=1000)
    if norm_rows:
        execute_values(cur, """
            INSERT INTO nse_fii_derivatives_stats_daily
            (trade_date,segment,buy_contracts,buy_amt_cr,sell_contracts,sell_amt_cr,oi_contracts,oi_amt_cr,file_hash,raw)
            VALUES %s
            ON CONFLICT(trade_date,segment) DO UPDATE SET
            buy_contracts=EXCLUDED.buy_contracts,
            buy_amt_cr=EXCLUDED.buy_amt_cr,
            sell_contracts=EXCLUDED.sell_contracts,
            sell_amt_cr=EXCLUDED.sell_amt_cr,
            oi_contracts=EXCLUDED.oi_contracts,
            oi_amt_cr=EXCLUDED.oi_amt_cr,
            file_hash=EXCLUDED.file_hash,
            raw=EXCLUDED.raw,
            updated_at=now();
        """, norm_rows, page_size=1000)
    conn.commit()
    cur.close()

    status = "imported" if rows else "warning"
    message = error
    if rows and not norm_rows:
        message = (message + "; " if message else "") + "raw rows imported but no normalized segment rows detected"
    if not rows and not error:
        message = "no rows parsed"
    register_file(conn, trade_date, "fii_stats", path, file_hash, len(rows), status, message)
    if error and not rows:
        print(f"{trade_date}: fii_stats parse warning: {error}")
    return len(rows)


def import_all_for_date(conn, trade_date: str, eod_dir: str, base_dir: str, force: bool = False):
    jobs = [
        ("participant_oi", lambda p: import_participant(conn, trade_date, "oi", p, force)),
        ("participant_vol", lambda p: import_participant(conn, trade_date, "vol", p, force)),
        ("fo_bhavcopy", lambda p: import_fo_bhavcopy(conn, trade_date, p, force)),
        ("cm_bhavcopy", lambda p: import_cm_bhavcopy(conn, trade_date, p, force)),
        ("sec_bhav", lambda p: import_sec_bhav(conn, trade_date, p, force)),
        ("fii_stats", lambda p: import_fii_stats(conn, trade_date, p, force)),
    ]
    for kind, func in jobs:
        path = find_file(kind, trade_date, eod_dir, base_dir)
        if not path:
            print(f"{trade_date}: {kind}: missing")
            continue
        try:
            rows = func(path)
            print(f"{trade_date}: {kind}: {rows} rows ({path.name})")
        except Exception as exc:
            print(f"{trade_date}: {kind}: ERROR {type(exc).__name__}: {exc}")
            try:
                register_file(conn, trade_date, kind, path, sha256_file(path), 0, "error", f"{type(exc).__name__}: {exc}")
            except Exception:
                pass


# -------------------- 200 STD DEV VOLUME SPURT --------------------
def load_bhavcopy_eod(conn, scan_dates: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Load enough cash EOD rows for 200-STD calculation.

    Uses cursor fetch instead of pandas.read_sql to avoid the DBAPI warning and
    applies a date window so old history does not make every run slow.
    """
    where_sql = ""
    params: Tuple[Any, ...] = tuple()
    if scan_dates:
        parsed_dates = sorted(pd.to_datetime(d).date() for d in scan_dates)
        min_d = parsed_dates[0]
        max_d = parsed_dates[-1]
        # 500 calendar days gives enough room for 200 trading sessions and 12 monthly bars.
        cutoff = min_d - timedelta(days=500)
        where_sql = "WHERE trade_date >= %s AND trade_date <= %s"
        params = (cutoff, max_d)
        print(f"Volume spurt: loading cash history from {cutoff} to {max_d} ...", flush=True)
    else:
        print("Volume spurt: loading all cash history ...", flush=True)

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT trade_date, symbol, open, high, low, close, volume
        FROM nse_bhavcopy_eod
        {where_sql}
        ORDER BY symbol, trade_date
        """,
        params,
    )
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()

    df = pd.DataFrame(rows, columns=columns)
    print(f"Volume spurt: loaded {len(df)} cash rows.", flush=True)
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["trade_date", "symbol", "open", "high", "low", "close", "volume"])


def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    data = df.sort_values(["symbol", "trade_date"]).copy()
    if timeframe == "D":
        data["bar_date"] = data["trade_date"]
        data["scan_date"] = data["trade_date"].dt.date
        return data[["symbol", "bar_date", "scan_date", "open", "high", "low", "close", "volume"]]
    rule = "W-FRI" if timeframe == "W" else "ME"
    data["actual_trade_date"] = data["trade_date"]
    out = (
        data.set_index("trade_date")
        .groupby("symbol")
        .resample(rule)
        .agg({
            "actual_trade_date": "max",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna(subset=["close"])
        .reset_index()
        .rename(columns={"trade_date": "bar_date", "actual_trade_date": "scan_date"})
    )
    out["scan_date"] = pd.to_datetime(out["scan_date"]).dt.date
    return out[["symbol", "bar_date", "scan_date", "open", "high", "low", "close", "volume"]]


def classify_bubble(norm_vol: float) -> str:
    if pd.isna(norm_vol):
        return ""
    if norm_vol > 6:
        return "HUGE"
    if norm_vol > 4:
        return "LARGE"
    if norm_vol > 3:
        return "NORMAL"
    if norm_vol > 1:
        return "SMALL"
    if norm_vol > 0:
        return "TINY"
    return ""


def prepare_volume_bars(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    lookback = {"D": DAILY_LOOKBACK, "W": WEEKLY_LOOKBACK, "M": MONTHLY_LOOKBACK}[timeframe]
    bars = resample_timeframe(df, timeframe)
    if bars.empty:
        return bars
    bars = bars.sort_values(["symbol", "bar_date"])
    print(f"Volume spurt: calculating {timeframe} rolling STD on {len(bars)} bars ...", flush=True)
    bars["vol_stdev"] = bars.groupby("symbol")["volume"].transform(
        lambda s: s.rolling(lookback, min_periods=lookback).std(ddof=0)
    )
    bars.loc[bars["vol_stdev"] == 0, "vol_stdev"] = pd.NA
    bars["norm_vol"] = bars["volume"] / bars["vol_stdev"]
    return bars


def scan_volume_spurt_from_bars(bars: pd.DataFrame, timeframe: str, scan_date: date) -> pd.DataFrame:
    cols = ["scan_date", "timeframe", "symbol", "close", "volume", "norm_vol", "bubble"]
    if bars.empty:
        return pd.DataFrame(columns=cols)
    latest = bars[bars["scan_date"] == scan_date].copy()
    latest = latest.dropna(subset=["norm_vol"])
    latest = latest[latest["norm_vol"] > MIN_NORM_VOL].copy()
    if latest.empty:
        return pd.DataFrame(columns=cols)
    latest["timeframe"] = timeframe
    latest["bubble"] = latest["norm_vol"].apply(classify_bubble)
    latest["close"] = latest["close"].round(2)
    latest["norm_vol"] = latest["norm_vol"].round(2)
    latest["volume"] = latest["volume"].astype("int64")
    return latest[cols].sort_values("norm_vol", ascending=False)



def run_nt_calibration(conn, dates: Sequence[str], symbols: Sequence[str]) -> Dict[str, int]:
    """Build EOD contracts-per-trade calibration used for live approx NT.

    NSE live option-chain snapshots do not provide number of trades. This table
    stores a daily median traded_volume/no_of_trades by useful buckets so the
    live dashboard can estimate approxNT from 1m volume.
    """
    print("\nStarting NT calibration from FO bhavcopy ...", flush=True)
    out: Dict[str, int] = {}
    cur = conn.cursor()
    try:
        for trade_date in dates:
            total_rows = 0
            for symbol in symbols:
                cur.execute("DELETE FROM nse_nt_calibration_daily WHERE trade_date = %s AND symbol = %s", (trade_date, symbol))
                cur.execute(
                    """
                    WITH base AS (
                        SELECT
                            trade_date,
                            symbol,
                            CASE
                                WHEN COALESCE(option_type, '') IN ('CE','PE') OR COALESCE(instrument_type, '') LIKE 'OPT%%' THEN 'OPTION'
                                WHEN COALESCE(instrument_type, '') LIKE 'FUT%%' THEN 'FUTURE'
                                ELSE 'OTHER'
                            END AS segment,
                            CASE
                                WHEN COALESCE(option_type, '') IN ('CE','PE') OR COALESCE(instrument_type, '') LIKE 'OPT%%' THEN 'OPTIDX'
                                WHEN COALESCE(instrument_type, '') LIKE 'FUT%%' THEN 'FUTIDX'
                                ELSE COALESCE(NULLIF(instrument_type, ''), 'NA')
                            END AS instrument_group,
                            COALESCE(NULLIF(option_type, ''), '') AS option_type,
                            CASE
                                WHEN COALESCE(option_type, '') IN ('CE','PE') AND strike_price IS NOT NULL AND underlying_price IS NOT NULL THEN
                                    CASE
                                        WHEN ABS(strike_price - underlying_price) <= 50 THEN 'ATM'
                                        WHEN ABS(strike_price - underlying_price) <= 250 THEN 'NEAR'
                                        ELSE 'FAR'
                                    END
                                ELSE 'NA'
                            END AS moneyness_bucket,
                            CASE
                                WHEN COALESCE(option_type, '') IN ('CE','PE') THEN
                                    CASE
                                        WHEN COALESCE(close_price, last_price, settlement_price, 0) < 25 THEN 'P0_25'
                                        WHEN COALESCE(close_price, last_price, settlement_price, 0) < 75 THEN 'P25_75'
                                        WHEN COALESCE(close_price, last_price, settlement_price, 0) < 150 THEN 'P75_150'
                                        ELSE 'P150_PLUS'
                                    END
                                ELSE 'NA'
                            END AS premium_bucket,
                            CASE
                                WHEN expiry_date IS NULL THEN 'NA'
                                WHEN expiry_date - trade_date <= 7 THEN 'DTE_0_7'
                                WHEN expiry_date - trade_date <= 14 THEN 'DTE_8_14'
                                WHEN expiry_date - trade_date <= 35 THEN 'DTE_15_35'
                                ELSE 'DTE_36_PLUS'
                            END AS dte_bucket,
                            traded_volume::numeric AS traded_volume,
                            no_of_trades::numeric AS no_of_trades,
                            traded_volume::numeric / NULLIF(no_of_trades::numeric, 0) AS contracts_per_trade
                        FROM nse_fo_bhavcopy_raw
                        WHERE trade_date = %s
                          AND symbol = %s
                          AND traded_volume > 0
                          AND no_of_trades > 0
                    ), grouped AS (
                        SELECT
                            trade_date, symbol, segment, instrument_group, option_type,
                            moneyness_bucket, premium_bucket, dte_bucket,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY contracts_per_trade)::numeric AS contracts_per_trade,
                            count(*)::integer AS row_count,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY traded_volume)::numeric AS median_traded_volume,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY no_of_trades)::numeric AS median_no_of_trades
                        FROM base
                        WHERE contracts_per_trade IS NOT NULL AND contracts_per_trade > 0
                        GROUP BY trade_date, symbol, segment, instrument_group, option_type, moneyness_bucket, premium_bucket, dte_bucket
                    )
                    INSERT INTO nse_nt_calibration_daily
                    (trade_date, symbol, segment, instrument_type, option_type, moneyness_bucket, premium_bucket, dte_bucket,
                     contracts_per_trade, row_count, median_traded_volume, median_no_of_trades, payload)
                    SELECT
                        trade_date, symbol, segment, instrument_group, option_type, moneyness_bucket, premium_bucket, dte_bucket,
                        contracts_per_trade, row_count, median_traded_volume, median_no_of_trades,
                        jsonb_build_object(
                            'method', 'median(traded_volume / no_of_trades)',
                            'purpose', 'live approxNT calibration',
                            'rowCount', row_count
                        )
                    FROM grouped
                    ON CONFLICT(trade_date, symbol, segment, instrument_type, option_type, moneyness_bucket, premium_bucket, dte_bucket)
                    DO UPDATE SET
                        contracts_per_trade=EXCLUDED.contracts_per_trade,
                        row_count=EXCLUDED.row_count,
                        median_traded_volume=EXCLUDED.median_traded_volume,
                        median_no_of_trades=EXCLUDED.median_no_of_trades,
                        payload=EXCLUDED.payload,
                        updated_at=now();
                    """,
                    (trade_date, symbol),
                )
                total_rows += max(cur.rowcount or 0, 0)
            conn.commit()
            out[str(trade_date)] = total_rows
            print(f"{trade_date}: NT calibration rows stored: {total_rows}", flush=True)
    finally:
        cur.close()
    return out
def run_volume_spurt(conn, scan_dates: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    print("\nStarting STD-200 volume-spurt scan ...", flush=True)
    df = load_bhavcopy_eod(conn, scan_dates=scan_dates)
    out: Dict[str, List[Dict[str, Any]]] = {}
    if df.empty:
        print("Volume spurt: no cash rows available.", flush=True)
        return out

    bars_by_tf = {tf: prepare_volume_bars(df, tf) for tf in ("D", "W", "M")}
    cur = conn.cursor()
    unique_dates = sorted(set(str(pd.to_datetime(d).date()) for d in scan_dates))
    total = len(unique_dates)
    for idx, d in enumerate(unique_dates, start=1):
        scan_dt = pd.to_datetime(d).date()
        print(f"Volume spurt: processing {idx}/{total} {scan_dt} ...", flush=True)
        frames = []
        for tf in ("D", "W", "M"):
            result = scan_volume_spurt_from_bars(bars_by_tf.get(tf, pd.DataFrame()), tf, scan_dt)
            if not result.empty:
                frames.append(result)
        final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["scan_date", "timeframe", "symbol", "close", "volume", "norm_vol", "bubble"])
        cur.execute("DELETE FROM nse_volume_spurt_scan WHERE scan_date = %s", (scan_dt,))
        if not final.empty:
            rows = [
                (r.scan_date, r.timeframe, r.symbol, float(r.close), int(r.volume), float(r.norm_vol), r.bubble, Json(json_safe(r._asdict())))
                for r in final.itertuples(index=False)
            ]
            execute_values(cur, """
                INSERT INTO nse_volume_spurt_scan(scan_date,timeframe,symbol,close,volume,norm_vol,bubble,payload)
                VALUES %s
                ON CONFLICT(scan_date,timeframe,symbol) DO UPDATE SET
                close=EXCLUDED.close, volume=EXCLUDED.volume, norm_vol=EXCLUDED.norm_vol,
                bubble=EXCLUDED.bubble, payload=EXCLUDED.payload, updated_at=now();
            """, rows, page_size=1000)
        out[str(scan_dt)] = final.to_dict("records")
    conn.commit()
    cur.close()
    print("Volume spurt: finished.", flush=True)
    return out


# -------------------- DASHBOARD PAYLOAD + RESULT TABLES --------------------


def resolve_dashboard_script(base_dir: str, requested: Optional[str]) -> str:
    """Resolve the dashboard builder supplied by this cloud package.

    The uploaded V11.8 dashboard contains build_eod_participation_payload() and
    the importer disables cache reads while building, so the explicit cloud copy
    is valid even though its filename is simplified to dashboard.py.
    """
    if requested:
        candidate = Path(str(requested))
        if candidate.exists() and candidate.is_file():
            print(f"Dashboard builder resolved to explicit file: {candidate}", flush=True)
            return str(candidate)
    candidate = Path(base_dir) / "dashboard.py"
    if candidate.exists():
        return str(candidate)
    raise FileNotFoundError(f"Dashboard builder file not found: {candidate}")

def load_dashboard_module(path: str):
    p = Path(path)
    if not p.exists() and not str(p).lower().endswith(".py"):
        p2 = Path(str(p) + ".py")
        if p2.exists():
            p = p2
    if not p.exists():
        print(f"Dashboard builder file not found: {p}", flush=True)
        return None
    spec = importlib.util.spec_from_file_location("nse_dashboard_for_eod_processing", str(p))
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def upsert_payload_results(conn, payload: Dict[str, Any], volume_spurts: Optional[List[Dict[str, Any]]] = None):
    payload = dict(payload or {})
    if volume_spurts is not None:
        payload["volumeSpurts"] = {
            "summary": {
                "lookback": DAILY_LOOKBACK,
                "minNormVol": MIN_NORM_VOL,
                "note": "Daily norm_vol = volume / rolling 200-day standard deviation of volume. Weekly/monthly use practical lookbacks.",
                "count": len(volume_spurts),
            },
            "rows": volume_spurts,
        }
    trade_date = payload.get("date")
    symbol = (payload.get("symbol") or "").upper()
    expiry = str(payload.get("expiry") or "")
    if not trade_date or not symbol:
        return

    cur = conn.cursor()
    # V11.6 cache safety: when a real expiry cache is being written, remove old blank-expiry cache rows
    # for the same date/symbol so the dashboard cannot accidentally pick stale incomplete cache.
    try:
        if str(expiry or "").strip():
            cur.execute(
                "DELETE FROM nse_eod_payload_cache WHERE trade_date = %s AND symbol = %s AND COALESCE(expiry_date,'') = ''",
                (trade_date, symbol),
            )
    except Exception:
        pass
    # Participants.
    for row in payload.get("participants") or []:
        cur.execute(
            """
            INSERT INTO nse_participant_bias_daily
            (trade_date,participant,raw_participant,direction,score,score_change,future_net,future_net_change,
             future_score,option_score,buyer_score,writer_score,call_short_minus_put_short,index_option_volume,
             index_option_volume_score,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date,participant) DO UPDATE SET
             raw_participant=EXCLUDED.raw_participant,direction=EXCLUDED.direction,score=EXCLUDED.score,
             score_change=EXCLUDED.score_change,future_net=EXCLUDED.future_net,future_net_change=EXCLUDED.future_net_change,
             future_score=EXCLUDED.future_score,option_score=EXCLUDED.option_score,buyer_score=EXCLUDED.buyer_score,
             writer_score=EXCLUDED.writer_score,call_short_minus_put_short=EXCLUDED.call_short_minus_put_short,
             index_option_volume=EXCLUDED.index_option_volume,index_option_volume_score=EXCLUDED.index_option_volume_score,
             payload=EXCLUDED.payload,updated_at=now();
            """,
            (
                trade_date, row.get("participant"), row.get("rawParticipant"), row.get("direction"), row.get("score"), row.get("scoreChange"),
                row.get("futureNet"), row.get("futureNetChange"), row.get("futureScore"), row.get("optionScore"), row.get("buyerScore"),
                row.get("writerScore"), row.get("callShortMinusPutShort"), row.get("indexOptionVolume"), row.get("indexOptionVolumeScore"), Json(json_safe(row)),
            ),
        )
    # Smart participant attribution: inferred FII/Prop expiry and strike allocation.
    attr = payload.get("participantAttribution") or {}
    expiry_attr_rows = payload.get("participantExpiryAttribution") or attr.get("expiryRows") or []
    for row in expiry_attr_rows:
        cur.execute(
            """
            INSERT INTO nse_participant_expiry_attribution_daily
            (trade_date,participant,action,symbol,expiry_date,estimated_contracts,candidate_change_oi,
             candidate_volume,max_action_score,top_strikes,top_confidence,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date, participant, action, symbol, expiry_date) DO UPDATE SET
            estimated_contracts=EXCLUDED.estimated_contracts,
            candidate_change_oi=EXCLUDED.candidate_change_oi,
            candidate_volume=EXCLUDED.candidate_volume,
            max_action_score=EXCLUDED.max_action_score,
            top_strikes=EXCLUDED.top_strikes,
            top_confidence=EXCLUDED.top_confidence,
            payload=EXCLUDED.payload,
            updated_at=now();
            """,
            (
                trade_date,
                row.get("participant"),
                row.get("action"),
                (row.get("symbol") or "").upper(),
                str(row.get("expiry") or ""),
                row.get("estimatedContracts"),
                row.get("candidateChangeOi"),
                row.get("candidateVolume"),
                row.get("maxActionScore"),
                row.get("topStrikes"),
                row.get("topConfidence"),
                Json(json_safe(row)),
            ),
        )

    strike_attr_rows = (
        payload.get("participantStrikeAttributionFull")
        or payload.get("participantStrikeAttributionAllTop")
        or payload.get("participantStrikeAttribution")
        or attr.get("fullRows")
        or attr.get("allTopStrikeRows")
        or attr.get("topStrikeRows")
        or []
    )
    for row in strike_attr_rows:
        if row.get("strike") is None:
            continue
        cur.execute(
            """
            INSERT INTO nse_participant_strike_attribution_daily
            (trade_date,participant,raw_participant,action,action_meaning,source_change_column,action_rank,
             shown_in_top_report,participant_action_contracts,symbol,expiry_date,strike,side,
             estimated_participant_contracts,share_of_participant_action_pct,candidate_change_oi,candidate_oi,
             candidate_volume,close,price_change,underlying,build,actor,action_score,confidence,
             exact_price_confirm,model_note,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date, participant, action, symbol, expiry_date, strike, side) DO UPDATE SET
            raw_participant=EXCLUDED.raw_participant,
            action_meaning=EXCLUDED.action_meaning,
            source_change_column=EXCLUDED.source_change_column,
            action_rank=EXCLUDED.action_rank,
            shown_in_top_report=EXCLUDED.shown_in_top_report,
            participant_action_contracts=EXCLUDED.participant_action_contracts,
            estimated_participant_contracts=EXCLUDED.estimated_participant_contracts,
            share_of_participant_action_pct=EXCLUDED.share_of_participant_action_pct,
            candidate_change_oi=EXCLUDED.candidate_change_oi,
            candidate_oi=EXCLUDED.candidate_oi,
            candidate_volume=EXCLUDED.candidate_volume,
            close=EXCLUDED.close,
            price_change=EXCLUDED.price_change,
            underlying=EXCLUDED.underlying,
            build=EXCLUDED.build,
            actor=EXCLUDED.actor,
            action_score=EXCLUDED.action_score,
            confidence=EXCLUDED.confidence,
            exact_price_confirm=EXCLUDED.exact_price_confirm,
            model_note=EXCLUDED.model_note,
            payload=EXCLUDED.payload,
            updated_at=now();
            """,
            (
                trade_date,
                row.get("participant"),
                row.get("rawParticipant"),
                row.get("action"),
                row.get("actionMeaning"),
                row.get("sourceChangeColumn"),
                row.get("actionRank"),
                row.get("shownInTopReport"),
                row.get("participantActionContracts"),
                (row.get("symbol") or "").upper(),
                str(row.get("expiry") or ""),
                row.get("strike"),
                row.get("side"),
                row.get("estimatedParticipantContracts"),
                row.get("shareOfParticipantActionPct"),
                row.get("candidateChangeOi"),
                row.get("candidateOi"),
                row.get("candidateVolume"),
                row.get("close"),
                row.get("priceChange"),
                row.get("underlying"),
                row.get("build"),
                row.get("actor"),
                row.get("actionScore"),
                row.get("confidence"),
                row.get("exactPriceConfirm"),
                row.get("modelNote"),
                Json(json_safe(row)),
            ),
        )
    # Option wall / rows.
    wall_rows = []
    walls = payload.get("walls") or {}
    for kind in ("resistance", "support"):
        r = walls.get(kind) or {}
        if r.get("strike") is not None:
            rr = dict(r); rr["wallKind"] = kind
            wall_rows.append(rr)
    for r in (payload.get("topCe") or []) + (payload.get("topPe") or []):
        rr = dict(r); rr["wallKind"] = "top"
        wall_rows.append(rr)
    for r in wall_rows:
        cur.execute(
            """
            INSERT INTO nse_option_wall_daily
            (trade_date,symbol,expiry_date,side,strike,wall_kind,build,meaning,activity_score,oi,coi,volume,close,price_change,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date,symbol,expiry_date,side,strike,wall_kind) DO UPDATE SET
            build=EXCLUDED.build,meaning=EXCLUDED.meaning,activity_score=EXCLUDED.activity_score,oi=EXCLUDED.oi,
            coi=EXCLUDED.coi,volume=EXCLUDED.volume,close=EXCLUDED.close,price_change=EXCLUDED.price_change,
            payload=EXCLUDED.payload,updated_at=now();
            """,
            (trade_date, symbol, expiry, r.get("side"), r.get("strike"), r.get("wallKind"), r.get("build"), r.get("meaning"), r.get("activityScore"), r.get("oi"), r.get("coi"), r.get("volume"), r.get("close"), r.get("priceChange"), Json(json_safe(r))),
        )
    # Futures.
    for row in ((payload.get("futures") or {}).get("rows") or []):
        cur.execute(
            """
            INSERT INTO nse_futures_build_up_daily
            (trade_date,symbol,expiry_date,direction,build,close,price_change,price_pct,oi,coi,volume,basis,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date,symbol,expiry_date) DO UPDATE SET
            direction=EXCLUDED.direction,build=EXCLUDED.build,close=EXCLUDED.close,price_change=EXCLUDED.price_change,
            price_pct=EXCLUDED.price_pct,oi=EXCLUDED.oi,coi=EXCLUDED.coi,volume=EXCLUDED.volume,basis=EXCLUDED.basis,
            payload=EXCLUDED.payload,updated_at=now();
            """,
            (trade_date, symbol, str(row.get("expiry") or ""), row.get("direction"), row.get("build"), row.get("close"), row.get("priceChange"), row.get("pricePct"), row.get("oi"), row.get("coi"), row.get("volume"), row.get("basis"), Json(json_safe(row))),
        )
    # Cash footprint.
    cash_rows = []
    cash = payload.get("cash") or {}
    for key in ("topRows", "accumulation", "distribution", "noAcceptance"):
        for r in cash.get(key) or []:
            if r.get("symbol"):
                cash_rows.append(r)
    seen_cash = set()
    for row in cash_rows:
        sym = row.get("symbol")
        if sym in seen_cash:
            continue
        seen_cash.add(sym)
        cur.execute(
            """
            INSERT INTO nse_cash_delivery_footprint_daily
            (trade_date,symbol,signal,price_pct,close,delivery_pct,delivery_value_lacs,turnover_lacs,delivery_qty,
             delivery_qty_change_pct,volume_change_pct,activity_score,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date,symbol) DO UPDATE SET
            signal=EXCLUDED.signal,price_pct=EXCLUDED.price_pct,close=EXCLUDED.close,delivery_pct=EXCLUDED.delivery_pct,
            delivery_value_lacs=EXCLUDED.delivery_value_lacs,turnover_lacs=EXCLUDED.turnover_lacs,delivery_qty=EXCLUDED.delivery_qty,
            delivery_qty_change_pct=EXCLUDED.delivery_qty_change_pct,volume_change_pct=EXCLUDED.volume_change_pct,
            activity_score=EXCLUDED.activity_score,payload=EXCLUDED.payload,updated_at=now();
            """,
            (trade_date, sym, row.get("signal"), row.get("pricePct"), row.get("close"), row.get("deliveryPct"), row.get("deliveryValueLacs"), row.get("turnoverLacs"), row.get("deliveryQty"), row.get("deliveryQtyChangePct"), row.get("volumeChangePct"), row.get("activityScore"), Json(json_safe(row))),
        )
    # CM regime.
    cm_summary = ((payload.get("cmMarket") or {}).get("summary") or {})
    if cm_summary:
        cur.execute(
            """
            INSERT INTO nse_cm_participation_regime_daily
            (trade_date,regime,conviction,participation_score,traded_value_crores,traded_qty_lakhs,no_of_trades,
             securities_traded,avg_trade_size,traded_value_change_pct,traded_qty_change_pct,trades_change_pct,
             avg_trade_size_change_pct,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(trade_date) DO UPDATE SET
            regime=EXCLUDED.regime,conviction=EXCLUDED.conviction,participation_score=EXCLUDED.participation_score,
            traded_value_crores=EXCLUDED.traded_value_crores,traded_qty_lakhs=EXCLUDED.traded_qty_lakhs,
            no_of_trades=EXCLUDED.no_of_trades,securities_traded=EXCLUDED.securities_traded,avg_trade_size=EXCLUDED.avg_trade_size,
            traded_value_change_pct=EXCLUDED.traded_value_change_pct,traded_qty_change_pct=EXCLUDED.traded_qty_change_pct,
            trades_change_pct=EXCLUDED.trades_change_pct,avg_trade_size_change_pct=EXCLUDED.avg_trade_size_change_pct,
            payload=EXCLUDED.payload,updated_at=now();
            """,
            (trade_date, cm_summary.get("regime"), cm_summary.get("conviction"), cm_summary.get("participationScore"), cm_summary.get("tradedValueCrores"), cm_summary.get("tradedQtyLakhs"), cm_summary.get("noOfTrades"), cm_summary.get("securitiesTraded"), cm_summary.get("avgTradeSize"), cm_summary.get("tradedValueChangePct"), cm_summary.get("tradedQtyChangePct"), cm_summary.get("tradesChangePct"), cm_summary.get("avgTradeSizeChangePct"), Json(json_safe(payload.get("cmMarket") or {}))),
        )
    # 360 decision.
    big = payload.get("bigMoney") or {}
    futures_summary = ((payload.get("futures") or {}).get("summary") or {})
    cash_summary = ((payload.get("cash") or {}).get("summary") or {})
    view = payload.get("view360") or {}
    resistance = (payload.get("walls") or {}).get("resistance") or {}
    support = (payload.get("walls") or {}).get("support") or {}
    participants = {r.get("rawParticipant"): r for r in payload.get("participants") or []}
    cur.execute(
        """
        INSERT INTO nse_360_decision_daily
        (trade_date,symbol,expiry_date,spot_close,fii_bias,prop_bias,big_money_direction,big_money_score,future_direction,
         future_build,ce_resistance_strike,pe_support_strike,cash_direction,cm_regime,final_360_bias,confidence_score,decision_text,payload)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(trade_date,symbol,expiry_date) DO UPDATE SET
        spot_close=EXCLUDED.spot_close,fii_bias=EXCLUDED.fii_bias,prop_bias=EXCLUDED.prop_bias,
        big_money_direction=EXCLUDED.big_money_direction,big_money_score=EXCLUDED.big_money_score,
        future_direction=EXCLUDED.future_direction,future_build=EXCLUDED.future_build,
        ce_resistance_strike=EXCLUDED.ce_resistance_strike,pe_support_strike=EXCLUDED.pe_support_strike,
        cash_direction=EXCLUDED.cash_direction,cm_regime=EXCLUDED.cm_regime,final_360_bias=EXCLUDED.final_360_bias,
        confidence_score=EXCLUDED.confidence_score,decision_text=EXCLUDED.decision_text,payload=EXCLUDED.payload,updated_at=now();
        """,
        (
            trade_date, symbol, expiry, payload.get("spot"),
            (participants.get("FII") or {}).get("direction"), (participants.get("Pro") or {}).get("direction"),
            big.get("direction"), big.get("score"), futures_summary.get("direction"), futures_summary.get("build"),
            resistance.get("strike"), support.get("strike"), cash_summary.get("cashDirection"), cm_summary.get("regime"),
            view.get("verdict"), view.get("score"), (payload.get("decision") or {}).get("text"), Json(json_safe(payload)),
        ),
    )
    # Memory.
    for row in payload.get("memory") or []:
        cur.execute(
            """
            INSERT INTO nse_360_memory_validation
            (signal_date,validation_date,symbol,expected_bias,big_money_score,signal_spot,next_spot,move_pts,move_pct,
             ce_wall,pe_wall,decision,validation,reason,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(signal_date,validation_date,symbol) DO UPDATE SET
            expected_bias=EXCLUDED.expected_bias,big_money_score=EXCLUDED.big_money_score,signal_spot=EXCLUDED.signal_spot,
            next_spot=EXCLUDED.next_spot,move_pts=EXCLUDED.move_pts,move_pct=EXCLUDED.move_pct,ce_wall=EXCLUDED.ce_wall,
            pe_wall=EXCLUDED.pe_wall,decision=EXCLUDED.decision,validation=EXCLUDED.validation,reason=EXCLUDED.reason,
            payload=EXCLUDED.payload,updated_at=now();
            """,
            (row.get("signalDate"), row.get("nextDate"), row.get("symbol"), row.get("bigMoneyDirection"), row.get("bigMoneyScore"), row.get("signalSpot"), row.get("nextSpot"), row.get("movePts"), row.get("movePct"), row.get("ceWall"), row.get("peWall"), row.get("decision"), row.get("validation"), row.get("reason"), Json(json_safe(row))),
        )
    # Payload cache.
    cur.execute(
        """
        INSERT INTO nse_eod_payload_cache(trade_date,symbol,expiry_date,payload,source)
        VALUES (%s,%s,%s,%s,'nse_eod_import_process_to_db')
        ON CONFLICT(trade_date,symbol,expiry_date) DO UPDATE SET
        payload=EXCLUDED.payload, source=EXCLUDED.source, updated_at=now();
        """,
        (trade_date, symbol, expiry, Json(json_safe(payload))),
    )
    conn.commit()
    cur.close()


def _validate_raw_payload(payload: Dict[str, Any], trade_date: str, symbol: str) -> List[str]:
    """Return blocking reasons when a raw EOD payload is incomplete.

    A neutral decision must never be stored merely because the raw builder could
    not see one of its source datasets.
    """
    issues: List[str] = []
    if not isinstance(payload, dict):
        return ["builder did not return a dictionary"]

    error = str(payload.get("error") or "").strip()
    if error:
        issues.append(f"builder error: {error}")

    participants = payload.get("participants") or []
    expiry = str(payload.get("expiry") or "").strip()
    strike_rows = payload.get("strikeRows") or []
    walls = payload.get("walls") or {}
    futures = payload.get("futures") or {}
    cash = payload.get("cash") or {}
    cm_market = payload.get("cmMarket") or {}

    if len(participants) < 3:
        issues.append(f"participant rows={len(participants)}; expected FII/Prop/DII/Client data")
    if not expiry:
        issues.append("selected expiry is blank")
    if not strike_rows and not (walls.get("resistance") or walls.get("support")):
        issues.append("no NIFTY option strike/wall rows")
    if not (futures.get("current") or futures.get("rows") or futures.get("summary")):
        issues.append("no NIFTY futures result")
    if not (cash.get("topRows") or cash.get("rows") or cash.get("summary")):
        issues.append("no cash-delivery footprint")
    if not (cm_market.get("summary") or cm_market.get("history")):
        issues.append("no CM participation result")

    payload_date = str(payload.get("date") or "")
    payload_symbol = str(payload.get("symbol") or "").upper()
    if payload_date and payload_date != str(trade_date):
        issues.append(f"payload date {payload_date} does not match requested {trade_date}")
    if payload_symbol and payload_symbol != str(symbol).upper():
        issues.append(f"payload symbol {payload_symbol} does not match requested {symbol}")

    return issues


def _processed_result_counts(conn, trade_date: str, symbol: str) -> Dict[str, int]:
    """Read compact result counts used for post-build verification."""
    checks = {
        "participants": ("nse_participant_bias_daily", "trade_date=%s", (trade_date,)),
        "walls": ("nse_option_wall_daily", "trade_date=%s AND UPPER(symbol)=UPPER(%s)", (trade_date, symbol)),
        "futures": ("nse_futures_build_up_daily", "trade_date=%s AND UPPER(symbol)=UPPER(%s)", (trade_date, symbol)),
        "cash": ("nse_cash_delivery_footprint_daily", "trade_date=%s", (trade_date,)),
        "cm": ("nse_cm_participation_regime_daily", "trade_date=%s", (trade_date,)),
        "decision": ("nse_360_decision_daily", "trade_date=%s AND UPPER(symbol)=UPPER(%s)", (trade_date, symbol)),
    }
    out: Dict[str, int] = {}
    cur = conn.cursor()
    try:
        for name, (table, where_sql, params) in checks.items():
            cur.execute(f"SELECT count(*) FROM {table} WHERE {where_sql}", params)
            out[name] = int(cur.fetchone()[0] or 0)
    finally:
        cur.close()
    return out


def process_payloads(conn, dates: Sequence[str], symbols: Sequence[str], dashboard_script: str, volume_by_date: Dict[str, List[Dict[str, Any]]], eod_dir: Optional[str] = None) -> bool:
    mod = load_dashboard_module(dashboard_script)
    if mod is None:
        raise RuntimeError(f"Could not load dashboard builder file from {dashboard_script}")
    if not hasattr(mod, "build_eod_participation_payload"):
        raise RuntimeError(f"Dashboard builder loaded but build_eod_participation_payload() is missing in {dashboard_script}")

    # Point the proven raw/file builder at the just-downloaded reports.
    if eod_dir and hasattr(mod, "EOD_REPORTS_DIR"):
        mod.EOD_REPORTS_DIR = eod_dir
    if hasattr(mod, "ALLOW_EOD_FILE_FALLBACK"):
        mod.ALLOW_EOD_FILE_FALLBACK = True

    # Critical cloud fix:
    # The dashboard file ends with a wrapper that first reads processed result
    # tables. During an importer run those tables are empty (or contain a prior
    # fallback row), so the wrapper returned a blank Neutral payload and never
    # reached the raw builder. Disable every processed/cache short-circuit while
    # the importer is generating those very tables.
    for name in (
        "read_eod_cached_payload",
        "read_eod_cached_payload_safe",
        "build_eod_participation_payload_from_db",
        "build_eod_participation_payload_from_results_fast",
    ):
        if hasattr(mod, name):
            setattr(mod, name, lambda *a, **k: None)

    # The cloud dashboard stores the proven raw builder in this alias before its
    # later cache-first viewer overrides. Prefer it when available.
    builder = getattr(mod, "_cloud_file_eod_builder", None)
    if not callable(builder):
        builder = mod.build_eod_participation_payload

    total_jobs = len(sorted(dates)) * len(symbols)
    job_no = 0
    all_ok = True
    print(f"\nStarting processed EOD result-table build: {total_jobs} date/symbol jobs ...", flush=True)
    for d in sorted(dates):
        for symbol in symbols:
            job_no += 1
            try:
                print(f"Result build: {job_no}/{total_jobs} {d} {symbol} ...", flush=True)
                payload = builder(symbol, trade_date=d, expiry=None)
                issues = _validate_raw_payload(payload, d, symbol)
                if issues:
                    all_ok = False
                    print(
                        f"Result build: {d} {symbol}: INCOMPLETE - "
                        + " | ".join(issues)
                        + ". Processed rows/cache were not updated.",
                        flush=True,
                    )
                    continue

                spurts = volume_by_date.get(str(pd.to_datetime(d).date()), [])
                upsert_payload_results(conn, payload, volume_spurts=spurts)

                counts = _processed_result_counts(conn, d, symbol)
                required_counts = (
                    counts.get("participants", 0) >= 3
                    and counts.get("walls", 0) > 0
                    and counts.get("futures", 0) > 0
                    and counts.get("cash", 0) > 0
                    and counts.get("cm", 0) > 0
                    and counts.get("decision", 0) > 0
                )
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT max(expiry_date) FROM nse_360_decision_daily "
                        "WHERE trade_date=%s AND UPPER(symbol)=UPPER(%s)",
                        (d, symbol),
                    )
                    max_exp = cur.fetchone()[0]
                finally:
                    cur.close()

                if not required_counts or not str(max_exp or "").strip():
                    all_ok = False
                    print(
                        f"Result build: {d} {symbol}: verification FAILED "
                        f"counts={counts}, latest expiry={max_exp!r}",
                        flush=True,
                    )
                else:
                    print(
                        f"Result build: {d} {symbol}: SUCCESS "
                        f"counts={counts}, latest expiry={max_exp}",
                        flush=True,
                    )
            except Exception as exc:
                all_ok = False
                print(f"Result build: {d} {symbol}: ERROR {type(exc).__name__}: {exc}", flush=True)

    return all_ok


# -------------------- MAIN --------------------
def _prompt_date_range() -> List[str]:
    """Ask only start/end date, like upstox_local_nse_eod_auto.py.

    Supported input examples:
      010526     -> 01-May-2026
      01052026   -> 01-May-2026
      2026-05-01 -> 01-May-2026

    Blank start date runs only for today's date. Blank end date runs only
    for the entered start date.
    """
    print("\nEnter EOD date range for NSE reports.")
    print("Format: ddmmyy, for example 010526 for 01-May-2026")
    start_input = input("Enter start date (ddmmyy) [Leave blank for TODAY only]: ").strip()

    if not start_input:
        today = date.today().isoformat()
        print(f"Start date blank. Running for today only: {today}")
        return [today]

    start_date = normalize_date_arg(start_input)
    end_input = input(f"Enter end date (ddmmyy) [Leave blank for {start_input}]: ").strip()
    end_date = normalize_date_arg(end_input) if end_input else start_date
    return calendar_date_range(start_date, end_date)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Import NSE EOD reports to PostgreSQL and build cached daily decision results. "
            "When no date arguments are passed, this script asks for start/end date interactively."
        )
    )
    parser.add_argument("--base-dir", default=BASE_DIR)
    parser.add_argument("--eod-dir", default=None)
    parser.add_argument("--dbinfo", default=None)
    parser.add_argument("--database", default=DEFAULT_DB_NAME)
    parser.add_argument("--dashboard-script", default=None)
    parser.add_argument("--date", default=None, help="Single trade date. Accepts YYYY-MM-DD, DDMMYYYY, or DDMMYY.")
    parser.add_argument("--from-date", default=None, help="Start date for range. Accepts YYYY-MM-DD, DDMMYYYY, or DDMMYY, e.g. 010526.")
    parser.add_argument("--to-date", default=None, help="End date for range. Accepts YYYY-MM-DD, DDMMYYYY, or DDMMYY, e.g. 270526.")
    parser.add_argument("--all", action="store_true", help="Import/process all dates discovered in the EOD folder, or recent sessions when direct download is enabled.")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated index symbols for EOD decision cache.")
    parser.add_argument("--force", action="store_true", help="Delete/re-import selected date(s).")

    # New default behavior: direct download from NSE, same style as upstox_local_nse_eod_auto.py.
    # --direct is kept for backward compatibility with old commands; --no-direct opts out.
    parser.add_argument("--direct", action="store_true", help="Backward compatible flag. Direct NSE download is already the default unless --no-direct is used.")
    parser.add_argument("--no-direct", action="store_true", help="Do not download from NSE; only use files already present in EOD folder.")

    # New default behavior: keep downloads in the EOD folder, same as upstox_local_nse_eod_auto.py.
    # --keep-downloads is kept for backward compatibility; --temp-downloads restores temp staging.
    parser.add_argument("--keep-downloads", action="store_true", help="Backward compatible flag. Downloads are kept by default unless --temp-downloads is used.")
    parser.add_argument("--temp-downloads", action="store_true", help="Use a temporary staging folder for downloads instead of keeping files in --eod-dir.")

    parser.add_argument("--sessions", type=int, default=8, help="When --all is used and no local dates are found, fetch this many recent successful NSE sessions.")
    parser.add_argument("--end-date", default=None, help="When using --all recent sessions, fetch backward from this date. Defaults to today.")
    parser.add_argument("--no-install-deps", action="store_true", help="Do not auto-install optional parsers such as xlrd/html5lib/lxml.")
    parser.add_argument("--skip-volume-spurt", action="store_true", help="Skip STD-200 volume spurt processing for this run.")
    parser.add_argument("--skip-payload-cache", action="store_true", help="Skip dashboard payload/result table building for this run.")
    parser.add_argument("--skip-result-build", action="store_true", help="Skip V11.6 processed result table build. Not recommended for dashboard speed.")
    parser.add_argument("--build-payload-cache", action="store_true", help="Optional old mode: build nse_eod_payload_cache after import. Not needed for V11.3 raw-DB dashboard.")
    parser.add_argument("--non-interactive", action="store_true", help="Do not prompt. Requires --date or --from-date/--to-date unless --all is used.")
    args = parser.parse_args()
    # V11.6: build compact processed result tables and validated cache once after import.
    # Dashboard V11.6 reads validated cache first and falls back to result tables.
    if getattr(args, "skip_result_build", False):
        args.skip_payload_cache = True

    ensure_optional_dependencies(auto_install=not args.no_install_deps)

    base_dir = args.base_dir
    eod_dir = args.eod_dir or os.path.join(base_dir, "nse_eod_reports")
    dbinfo = args.dbinfo or os.path.join(base_dir, "dbinfo.txt")
    dashboard_script = args.dashboard_script or DEFAULT_DASHBOARD_SCRIPT
    dashboard_script = resolve_dashboard_script(base_dir, dashboard_script)
    symbols = [normalize_index_symbol(s) for s in args.symbols.split(",") if s.strip()]

    # Keep destination DB unchanged: defaults still come from DEFAULT_DB_NAME above.
    direct = not args.no_direct
    keep_downloads = not args.temp_downloads

    ensure_database(dbinfo, args.database)
    conn = connect(dbinfo, args.database)
    create_tables(conn)

    cleanup_dir = None
    try:
        # Date selection. With no date arguments, ask start and end date from the user.
        if args.from_date or args.to_date:
            if not (args.from_date and args.to_date):
                raise ValueError("Use both --from-date and --to-date for date range processing.")
            dates = calendar_date_range(args.from_date, args.to_date)
        elif args.date:
            dates = [normalize_date_arg(args.date)]
        elif args.all:
            dates = discover_dates(eod_dir, base_dir)
        else:
            if args.non_interactive:
                raise ValueError("Use --date or --from-date/--to-date in non-interactive mode.")
            dates = _prompt_date_range()

        if direct:
            requested_dates = list(dates)
            if args.all and not requested_dates:
                requested_dates = []
            working_eod_dir, cleanup_dir, downloaded_dates = ensure_reports_available(
                requested_dates,
                direct=True,
                eod_dir=eod_dir,
                keep_downloads=keep_downloads,
                force_download=args.force,
                sessions=args.sessions if (args.all and not requested_dates) else 0,
                end_date=args.end_date,
            )
            eod_dir = working_eod_dir
            if downloaded_dates:
                dates = downloaded_dates
        else:
            # Local-file mode: keep existing behavior for users who already have files downloaded.
            if args.all and not dates:
                dates = discover_dates(eod_dir, base_dir)

        if not dates:
            print(f"No EOD dates found in {eod_dir} / {base_dir}")
            return 1

        print("=" * 80)
        print("NSE EOD Import")
        print(f"Database        : {args.database}")
        print(f"DB info         : {dbinfo}")
        print(f"EOD folder      : {eod_dir}")
        print(f"Dates           : {', '.join(dates)}")
        print(f"Symbols         : {', '.join(symbols)}")
        print(f"Source mode     : {'DIRECT NSE DOWNLOAD' if direct else 'LOCAL FILES ONLY'}")
        print(f"Keep downloads  : {keep_downloads}")
        print("=" * 80)

        print("\nStarting DB import stage ...", flush=True)
        for pos, d in enumerate(dates, start=1):
            print(f"\nImport stage: {pos}/{len(dates)} {d}", flush=True)
            if args.force:
                print(f"Force cleanup for {d} ...", flush=True)
                delete_trade_date(conn, d)
            import_all_for_date(conn, d, eod_dir, base_dir, force=args.force)

        nt_calibration_by_date = run_nt_calibration(conn, dates, symbols)
        if args.skip_volume_spurt:
            print("\nSTD-200 volume-spurt stage skipped by --skip-volume-spurt.", flush=True)
            volume_by_date = {}
        else:
            volume_by_date = run_volume_spurt(conn, dates)
            for d, rows in volume_by_date.items():
                print(f"{d}: volume spurt rows stored: {len(rows)}", flush=True)

        result_build_ok = True
        if args.skip_payload_cache:
            print("\nProcessed EOD result-table build skipped.", flush=True)
        else:
            # Build processed rows only from raw EOD inputs. A partial Neutral
            # fallback is treated as failure so the Railway runner can retry.
            result_build_ok = process_payloads(
                conn, dates, symbols, dashboard_script, volume_by_date, eod_dir=eod_dir
            )

        if not result_build_ok:
            print(
                "\nEOD processing is incomplete. Raw rows may be present, but the "
                "processed EOD result/cache was not published. Returning code 2 so "
                "the Railway runner can retry.",
                flush=True,
            )
            return 2

        print("\nDone. Raw EOD data, compact processed EOD result tables, and cache are in PostgreSQL. Full result validation passed.", flush=True)
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
        if cleanup_dir and os.path.isdir(cleanup_dir):
            shutil.rmtree(cleanup_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
