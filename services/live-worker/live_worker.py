# -*- coding: utf-8 -*-
"""
Index live-data fetcher - RAW ONLY version - quote-derivative 404 safe

Purpose:
- Fetch NSE derivative quote data for NIFTY and BANKNIFTY only.
- Store raw current-expiry futures rows into: idxfuturesdata_current
- Store raw options rows into: idxoptionsdata_current
  - NIFTY: current expiry + next expiry
  - BANKNIFTY: current expiry only

Removed:
- Short Covering logic
- daily_coi_summary table creation/update
- alert_logs table creation/update
- minute_bucket / alert dedupe logic

Notes:
- This version does NOT require openInterest to be present for filtering.
- If openInterest exists, it is retained in the data.
- If openInterest is missing from the NSE response, the script prints a warning and continues without crashing.
"""

import os
import json
import time
import random
import importlib.util
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import quote

import requests
import pandas as pd
import sqlalchemy
import autocookie
from cloud_db import make_schema_engine

# -------------------- CONFIG --------------------
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.getenv('APP_DATA_DIR', str(Path(__file__).resolve().parent / 'data'))
os.makedirs(BASE_DIR, exist_ok=True)
os.chdir(BASE_DIR)

COOKIES_PATH = os.path.join(BASE_DIR, 'cookies.txt')
DBINFO_PATH  = os.path.join(BASE_DIR, 'dbinfo.txt')
INDICES_PATH = os.path.join(BASE_DIR, 'indices1.txt')

# Cloud build is intentionally NIFTY-only.
ALLOWED_SYMBOLS = {'NIFTY'}
IST = ZoneInfo('Asia/Kolkata')
MARKET_START = dtime(9, 14)
MARKET_END = dtime(15, 50)

WAIT_SECONDS = 60  # 1 minute between cycles

# Dynamic safety fallback for option-chain-v3 when quote-derivative
# returns expiry metadata in an unexpected shape.
#
# Prefer NSE metadata whenever possible. This fallback is used only when NSE
# expiry metadata cannot be parsed.
# Python weekday: Monday=0, Tuesday=1, ..., Sunday=6.
OPTION_WEEKLY_FALLBACK_WEEKDAY = {
    'NIFTY': 1,
}

OPTION_MONTHLY_FALLBACK_WEEKDAY = {
    'BANKNIFTY': 1,
}

FUTURES_INDEX_PARAM = {
    'NIFTY': 'nse50_fut',
    'BANKNIFTY': 'nifty_bank_fut',
}

OPTION_INDEX_PARAM = {
    'NIFTY': 'nse50_opt',
    'BANKNIFTY': 'nifty_bank_opt',
}

LIVE_DERIVATIVE_EXTRA_COLUMN_TYPES = {
    # Keep this empty by default.
    # The existing raw tables already contain volume/noOfTrades/premiumTurnover/identifier.
    # We should not auto-add duplicate/new schema fields for liveEquity data.
}
# ------------------------------------------------

headers = {
    'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'accept': 'application/json,text/plain,*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
    'referer': 'https://www.nseindia.com/get-quotes/derivatives'
}

# ---- DB engines (one Railway database, separate schemas) ----
engine1 = make_schema_engine('idxfuturesdata_current')
engine2 = make_schema_engine('idxoptionsdata_current')

_OPTION_BUYING_AI_MODULE = None
_CASH_MONEY_FLOW_MODULE = None
_CASH_MONEY_FLOW_ENGINE = None
_CASH_MONEY_FLOW_READY = False
ENABLE_CASH_MONEY_FLOW = os.environ.get('NSE360_DISABLE_CASH_MONEY_FLOW', '0').strip().lower() not in {'1', 'true', 'yes', 'y'}
CASH_MONEY_FLOW_SCRIPT = os.path.join(PROJECT_DIR, 'cash_money_flow.py')


def refresh_option_buying_ai_cache(symbol, trade_date=None, expiry=None):
    """
    Refresh dashboard-readable option-buying DB rows + JSON cache.

    This is intentionally best-effort: raw data collection must continue even if
    the signal layer has an import/query issue.
    """
    global _OPTION_BUYING_AI_MODULE
    try:
        dashboard_path = os.path.join(
            PROJECT_DIR,
            'dashboard.py',
        )
        if _OPTION_BUYING_AI_MODULE is None:
            spec = importlib.util.spec_from_file_location('nse_360_dashboard_option_ai', dashboard_path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f'Could not load dashboard module from {dashboard_path}')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            _OPTION_BUYING_AI_MODULE = module

        payload = _OPTION_BUYING_AI_MODULE.build_option_buying_ai_payload(
            symbol,
            trade_date=trade_date,
            expiry=expiry,
            band=20,
        )
        result = payload.get('optionBuyingResultWrite') or {}
        if payload.get('error'):
            print(f"[WARN] Option Buying AI cache skipped for {symbol}: {payload.get('error')}")
        else:
            print(
                f"[INFO] Option Buying AI cache refreshed for {symbol}: "
                f"{result.get('storedCandidates', 0)} rows, JSON cached={result.get('payloadCached')}"
            )
    except Exception as exc:
        print(f"[WARN] Option Buying AI cache refresh failed for {symbol}: {type(exc).__name__}: {exc}")


def _load_cash_money_flow_module():
    """Load the NIFTY 50 cash money-flow collector so this script can run one combined cycle."""
    global _CASH_MONEY_FLOW_MODULE
    if _CASH_MONEY_FLOW_MODULE is not None:
        return _CASH_MONEY_FLOW_MODULE
    if not os.path.exists(CASH_MONEY_FLOW_SCRIPT):
        raise FileNotFoundError(CASH_MONEY_FLOW_SCRIPT)

    spec = importlib.util.spec_from_file_location('nse_360_cash_money_flow', CASH_MONEY_FLOW_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Could not load cash money-flow module from {CASH_MONEY_FLOW_SCRIPT}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _CASH_MONEY_FLOW_MODULE = module
    return module


def run_cash_money_flow_cycle():
    """Run one cash money-flow upsert cycle after options/futures have been collected."""
    if not ENABLE_CASH_MONEY_FLOW:
        return None

    global _CASH_MONEY_FLOW_ENGINE, _CASH_MONEY_FLOW_READY
    try:
        module = _load_cash_money_flow_module()
        index_name = getattr(module, 'DEFAULT_INDEX_NAME', 'NIFTY 50')
        database = getattr(module, 'DEFAULT_DATABASE', 'idxcashdata_current')
        dbinfo_path = getattr(module, 'DBINFO_PATH', DBINFO_PATH)

        if not _CASH_MONEY_FLOW_READY:
            module.ensure_database(database, dbinfo_path)
            _CASH_MONEY_FLOW_ENGINE = module.make_engine(database, dbinfo_path)
            module.create_tables(_CASH_MONEY_FLOW_ENGINE)
            module.bootstrap_cookies(index_name, force_refresh=False)
            _CASH_MONEY_FLOW_READY = True
            print(f"[READY] Integrated cash money-flow ready in database {database}.")

        return module.run_cycle(_CASH_MONEY_FLOW_ENGINE, index_name, force_upsert=True)
    except Exception as exc:
        print(f"[WARN] Integrated cash money-flow cycle skipped: {type(exc).__name__}: {exc}")
        return None


# ---- session ----
session = requests.session()


def load_symbols():
    """Cloud deployment is intentionally restricted to NIFTY."""
    return ['NIFTY']


symbols = load_symbols()
print(f"Symbols enabled for raw storage: {', '.join(symbols)}")


# ================== COOKIE HELPERS ==================
def _read_cookies_file():
    """
    Returns (cookie_header_str, cookie_dict_or_None).

    - If cookies.txt contains JSON dict, convert it to a cookie header.
    - If cookies.txt contains raw cookie header text, use it directly.
    """
    with open(COOKIES_PATH, 'r') as f:
        txt = f.read().strip()

    try:
        cookie_dict = json.loads(txt)
        header = '; '.join(f'{k}={v}' for k, v in cookie_dict.items())
        return header, cookie_dict
    except Exception:
        cookie_dict = {}
        for part in txt.split(';'):
            if '=' not in part:
                continue
            key, value = part.split('=', 1)
            key = key.strip()
            if key:
                cookie_dict[key] = value.strip()
        return txt, cookie_dict or None


def _install_cookies(cookie_header, cookie_dict):
    """Push cookies into requests session and headers."""
    if cookie_dict:
        for k, v in cookie_dict.items():
            session.cookies.set(k, v)
    # Let requests build Cookie from session.cookies so cookies collected during
    # NSE warm-up calls are included too.
    headers.pop('cookie', None)


def _prime_nse_session(symbol=None):
    """Warm NSE session so anti-bot/session cookies are attached before API calls."""
    session.get('https://www.nseindia.com/', headers=headers, timeout=15)
    if symbol:
        quote_headers = headers.copy()
        quote_headers['referer'] = f'https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}'
        session.get(quote_headers['referer'], headers=quote_headers, timeout=15)


def _bootstrap_cookies():
    """Ensure cookies are available. Refresh with autocookie if needed."""
    try:
        header, dct = _read_cookies_file()
        _install_cookies(header, dct)
        _prime_nse_session()
        return
    except Exception:
        pass

    print('[COOKIES] Refreshing via autocookie.getCookies() ...')
    autocookie.getCookies()
    header, dct = _read_cookies_file()
    _install_cookies(header, dct)
    _prime_nse_session()


_bootstrap_cookies()


# ============================== UTILS ==============================

def parse_expiry_date(value):
    """Parse NSE expiry text/date into a Python date."""
    if value is None:
        return None

    raw = str(value).strip()
    if not raw or raw.lower() in {'none', 'nan', 'nat', 'xx', '-'}:
        return None

    candidates = [raw, raw.split()[0]]
    for candidate in candidates:
        for fmt in ('%d-%b-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(candidate, fmt).date()
            except Exception:
                pass

        try:
            parsed = pd.to_datetime(candidate, errors='coerce', dayfirst=True)
            if pd.notna(parsed):
                return parsed.date()
        except Exception:
            pass

    return None


def format_expiry_date(expiry_date):
    """Format expiry date in NSE option-chain-v3 style, e.g. 26-May-2026."""
    if expiry_date is None:
        return None
    return expiry_date.strftime('%d-%b-%Y')


def get_next_weekly_expiries(symbol, count=1, from_date=None):
    """Dynamic fallback expiries from today. Used only when NSE metadata fails."""
    symbol = str(symbol).upper()
    weekday = OPTION_WEEKLY_FALLBACK_WEEKDAY.get(symbol, 1)
    current = from_date or date.today()

    days_ahead = (weekday - current.weekday()) % 7
    first_expiry = current + timedelta(days=days_ahead)

    return [
        format_expiry_date(first_expiry + timedelta(days=7 * i))
        for i in range(count)
    ]


def get_monthly_expiries(symbol, count=1, from_date=None):
    """Dynamic monthly fallback expiries from today. Used only when NSE metadata fails."""
    symbol = str(symbol).upper()
    weekday = OPTION_MONTHLY_FALLBACK_WEEKDAY.get(symbol)
    if weekday is None:
        return []

    current = from_date or date.today()
    month_start = date(current.year, current.month, 1)
    expiries = []

    while len(expiries) < count:
        if month_start.month == 12:
            next_month_start = date(month_start.year + 1, 1, 1)
        else:
            next_month_start = date(month_start.year, month_start.month + 1, 1)

        last_day = next_month_start - timedelta(days=1)
        days_back = (last_day.weekday() - weekday) % 7
        expiry = last_day - timedelta(days=days_back)

        if expiry >= current:
            expiries.append(format_expiry_date(expiry))

        month_start = next_month_start

    return expiries


def get_dynamic_option_expiries(symbol, count=1, from_date=None):
    """Return symbol-specific option fallback expiries when NSE metadata fails."""
    symbol = str(symbol).upper()
    if symbol in OPTION_MONTHLY_FALLBACK_WEEKDAY:
        return get_monthly_expiries(symbol, count=count, from_date=from_date)
    return get_next_weekly_expiries(symbol, count=count, from_date=from_date)


def get_nearest_expiries_from_list(expiry_dates, count=1):
    """Return nearest non-expired unique expiries from a list of expiry strings."""
    today = date.today()
    rows = []

    for expiry in expiry_dates or []:
        parsed = parse_expiry_date(expiry)
        if parsed is None or parsed < today:
            continue
        rows.append((parsed, format_expiry_date(parsed)))

    rows = sorted(rows, key=lambda item: item[0])
    expiries = []
    for _, expiry in rows:
        if expiry not in expiries:
            expiries.append(expiry)
        if len(expiries) >= count:
            break

    return expiries


def filter_by_expiry_date(df, expiry):
    """Filter rows by expiry date even if API returns expiry in a different text format."""
    if df.empty or not expiry or 'expiryDate' not in df.columns:
        return df

    target = parse_expiry_date(expiry)
    if target is None:
        return df[df['expiryDate'].astype(str) == str(expiry)].copy()

    out = df.copy()
    out['_expiry_dt_filter'] = out['expiryDate'].map(parse_expiry_date)
    out = out[out['_expiry_dt_filter'] == target].drop(columns=['_expiry_dt_filter']).copy()
    return out



def get_nearest_expiry(df, instrument_type):
    """Return nearest non-expired expiry date for given instrument type."""
    required_cols = {'instrumentType', 'expiryDate'}
    if df.empty or not required_cols.issubset(df.columns):
        return None

    temp = df[df['instrumentType'] == instrument_type].copy()
    if temp.empty:
        return None

    temp['expiryDate_dt'] = temp['expiryDate'].map(parse_expiry_date)
    today = date.today()
    future = temp[temp['expiryDate_dt'].notna() & (temp['expiryDate_dt'] >= today)].copy()
    if future.empty:
        return None

    nearest = future.sort_values('expiryDate_dt').iloc[0]['expiryDate_dt']
    return format_expiry_date(nearest)

def get_nearest_expiries(df, instrument_type, count=1):
    """Return nearest non-expired expiry dates for given instrument type."""
    required_cols = {'instrumentType', 'expiryDate'}
    if df.empty or not required_cols.issubset(df.columns):
        return []

    temp = df[df['instrumentType'] == instrument_type].copy()
    if temp.empty:
        return []

    return get_nearest_expiries_from_list(temp['expiryDate'].astype(str).tolist(), count=count)

def get_existing_timestamps(engine, symbol):
    """Read timestamps already stored in a symbol table to avoid duplicate inserts."""
    try:
        with engine.connect() as conn:
            result = pd.read_sql(f'SELECT DISTINCT "timestamp" FROM "{symbol}"', conn)
            return set(result['timestamp'].astype(str))
    except Exception as e:
        print(f"[WARN] Could not read existing timestamps for {symbol}: {type(e).__name__}: {e}")
        try:
            engine.dispose()
        except Exception:
            pass
        return set()



def get_existing_timestamp_expiry_pairs(engine, symbol):
    """Read timestamp + expiry pairs already stored to avoid duplicate expiry snapshots."""
    try:
        with engine.connect() as conn:
            result = pd.read_sql(f'SELECT DISTINCT "timestamp", "expiryDate" FROM "{symbol}"', conn)
            if result.empty or 'timestamp' not in result.columns or 'expiryDate' not in result.columns:
                return set()
            return set(zip(result['timestamp'].astype(str), result['expiryDate'].astype(str)))
    except Exception as e:
        print(f"[WARN] Could not read existing timestamp/expiry pairs for {symbol}: {type(e).__name__}: {e}")
        try:
            engine.dispose()
        except Exception:
            pass
        return set()


def symbol_table_exists(engine, symbol):
    """Return True when the raw symbol table already exists."""
    try:
        sqlalchemy.inspect(engine).get_columns(symbol)
        return True
    except Exception:
        return False


def _execute_sql(bind, statement, params):
    """Execute SQL on either an active Connection or an Engine."""
    if hasattr(bind, 'execute'):
        return bind.execute(statement, params)
    with bind.begin() as conn:
        return conn.execute(statement, params)


def delete_existing_timestamp_rows(bind, symbol, timestamp_value):
    """Replace/upsert helper: remove an old same-timestamp snapshot before appending fresh NSE rows."""
    timestamp_text = str(timestamp_value or '').strip()
    if not timestamp_text or not symbol_table_exists(bind, symbol):
        return 0
    try:
        result = _execute_sql(
            bind,
            sqlalchemy.text(f'DELETE FROM "{symbol}" WHERE "timestamp" = :ts'),
            {'ts': timestamp_text},
        )
        return int(result.rowcount or 0)
    except Exception as exc:
        print(f"[WARN] Could not replace existing rows for {symbol} @ {timestamp_text}: {type(exc).__name__}: {exc}")
        return 0


def delete_existing_option_snapshot_rows(bind, symbol, frame):
    """Replace option snapshots by timestamp+expiry so current and next expiry remain independent."""
    if frame is None or frame.empty or 'timestamp' not in frame.columns or not symbol_table_exists(bind, symbol):
        return 0

    deleted = 0
    try:
        if 'expiryDate' in frame.columns:
            pairs = sorted({
                (str(ts).strip(), str(expiry).strip())
                for ts, expiry in zip(frame['timestamp'], frame['expiryDate'])
                if str(ts).strip() and str(expiry).strip()
            })
            stmt = sqlalchemy.text(
                f'DELETE FROM "{symbol}" WHERE "timestamp" = :ts AND "expiryDate" = :expiry'
            )
            for ts, expiry in pairs:
                result = _execute_sql(bind, stmt, {'ts': ts, 'expiry': expiry})
                deleted += int(result.rowcount or 0)
        else:
            timestamps = sorted({str(ts).strip() for ts in frame['timestamp'] if str(ts).strip()})
            stmt = sqlalchemy.text(f'DELETE FROM "{symbol}" WHERE "timestamp" = :ts')
            for ts in timestamps:
                result = _execute_sql(bind, stmt, {'ts': ts})
                deleted += int(result.rowcount or 0)
    except Exception as exc:
        print(f"[WARN] Could not replace existing option rows for {symbol}: {type(exc).__name__}: {exc}")
        return 0
    return deleted


def valid_timestamp(value, fallback):
    """Use fallback when NSE sends blank placeholder timestamps."""
    if value is None:
        return fallback
    value = str(value).strip()
    if not value or value in {'-', 'xx', 'None', 'nan'}:
        return fallback
    return value


def get_option_chain_timestamp(data, fallback):
    """Extract timestamp from option-chain-v3 response if available."""
    records = data.get('records') if isinstance(data, dict) else None
    records = records or {}
    return valid_timestamp(
        records.get('timestamp') or data.get('timestamp') or data.get('opt_timestamp'),
        fallback
    )


def get_live_futures_timestamp(data, fallback):
    """Extract timestamp from liveEquity-derivatives response if available."""
    if not isinstance(data, dict):
        return fallback
    return valid_timestamp(data.get('timestamp'), fallback)


def align_to_existing_table(engine, symbol, df):
    """Match DataFrame columns to an existing PostgreSQL table before append."""
    if df.empty:
        return df

    try:
        columns = [col['name'] for col in sqlalchemy.inspect(engine).get_columns(symbol)]
    except Exception:
        return df

    if not columns:
        return df

    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = None
    return out[columns]


def ensure_live_derivative_columns(engine, symbol):
    """Schema-safe mode: do not add volume/noOfTrades columns because they already exist in the raw tables."""
    try:
        existing = {col['name'] for col in sqlalchemy.inspect(engine).get_columns(symbol)}
    except Exception:
        return

    missing = [
        (column, column_type)
        for column, column_type in LIVE_DERIVATIVE_EXTRA_COLUMN_TYPES.items()
        if column not in existing
    ]
    if not missing:
        return

    try:
        with engine.begin() as conn:
            for column, column_type in missing:
                conn.execute(sqlalchemy.text(
                    f'ALTER TABLE "{symbol}" ADD COLUMN IF NOT EXISTS "{column}" {column_type}'
                ))
        print(f"[DB] Added live derivative columns for {symbol}: {[name for name, _ in missing]}")
    except Exception as exc:
        print(f"[WARN] Could not add live derivative columns for {symbol}: {type(exc).__name__}: {exc}")


def normalize_live_option_type(value):
    text = str(value or '').strip().upper()
    if text in {'CE', 'CALL', 'C'} or text.startswith('CALL'):
        return 'CE'
    if text in {'PE', 'PUT', 'P'} or text.startswith('PUT'):
        return 'PE'
    return text


def is_live_value(value):
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    text = str(value).strip()
    return text not in {'', '-', 'xx', 'None', 'nan', 'NaN', 'NAT', 'NaT'}


def option_merge_key(row):
    expiry = parse_expiry_date(row.get('expiryDate'))
    expiry_key = expiry.isoformat() if expiry else str(row.get('expiryDate') or '').strip()
    side_key = normalize_live_option_type(row.get('optionType'))
    raw_strike = row.get('strikePrice')
    try:
        strike_key = round(float(str(raw_strike).replace(',', '').strip()), 6)
    except Exception:
        strike_key = str(raw_strike or '').strip()
    return expiry_key, side_key, strike_key


def merge_live_option_fields(base_df, live_df):
    """Overlay OHLC/volume/trade fields from liveEquity option rows onto option-chain rows."""
    if base_df.empty or live_df.empty:
        return base_df

    out = base_df.copy()
    live = live_df.copy()
    if 'optionType' in out.columns:
        out['optionType'] = out['optionType'].map(normalize_live_option_type)
    if 'optionType' in live.columns:
        live['optionType'] = live['optionType'].map(normalize_live_option_type)

    merge_fields = [
        'identifier', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'prevClose',
        'lastPrice', 'change', 'pChange', 'volume', 'tradedContracts', 'tradedVolume',
        'noOfTrades', 'premiumTurnover', 'totalTurnover', 'value', 'vwap',
        'openInterest', 'spotPrice'
    ]
    for field in merge_fields:
        if field not in out.columns:
            out[field] = None

    live_map = {}
    for _, row in live.iterrows():
        key = option_merge_key(row)
        live_map[key] = row

    matched = 0
    for idx, row in out.iterrows():
        live_row = live_map.get(option_merge_key(row))
        if live_row is None:
            continue
        matched += 1
        for field in merge_fields:
            if field in live_row.index and is_live_value(live_row.get(field)):
                out.at[idx, field] = live_row.get(field)

    if matched:
        print(f"Merged liveEquity option OHLC/volume fields into {matched} option-chain rows.")
    else:
        print("[WARN] liveEquity option rows fetched but no option-chain rows matched by expiry/type/strike.")
    return out


def get_nearest_expiry_from_list(expiry_dates):
    """Return nearest non-expired expiry from NSE expiry date strings."""
    expiries = get_nearest_expiries_from_list(expiry_dates, count=1)
    return expiries[0] if expiries else None

def flatten_expiry_dates(value):
    """Flatten NSE expiry metadata from lists/dicts into date strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        dates = []
        for item in value.values():
            dates.extend(flatten_expiry_dates(item))
        return dates
    if isinstance(value, (list, tuple, set)):
        dates = []
        for item in value:
            dates.extend(flatten_expiry_dates(item))
        return dates
    return []


def get_option_expiry_from_quote_response(data, symbol):
    """Pick current option expiry from quote-derivative metadata or dynamic fallback."""
    expiry_dates = []
    expiry_dates.extend(flatten_expiry_dates(data.get('expiryDates')))
    expiry_dates.extend(flatten_expiry_dates((data.get('expiryDatesByInstrument') or {}).get('Index Options')))
    expiry_dates.extend(flatten_expiry_dates(data.get('expiryDatesByInstrument')))

    nearest = get_nearest_expiry_from_list(expiry_dates)
    if nearest:
        return nearest

    fallback = get_dynamic_option_expiries(symbol, count=1)[0]
    print(f"[WARN] Could not derive option expiry from quote-derivative for {symbol}; using dynamic fallback {fallback}.")
    return fallback

def get_option_expiries_from_quote_response(data, symbol, count=1):
    """Pick current/next option expiries from quote-derivative metadata or dynamic fallback."""
    expiry_dates = []
    expiry_dates.extend(flatten_expiry_dates(data.get('expiryDates')))
    expiry_dates.extend(flatten_expiry_dates((data.get('expiryDatesByInstrument') or {}).get('Index Options')))
    expiry_dates.extend(flatten_expiry_dates(data.get('expiryDatesByInstrument')))

    expiries = get_nearest_expiries_from_list(expiry_dates, count=count)
    if len(expiries) >= count:
        return expiries

    fallback_expiries = get_dynamic_option_expiries(symbol, count=count)
    for expiry in fallback_expiries:
        if expiry not in expiries:
            expiries.append(expiry)
        if len(expiries) >= count:
            break

    selected = expiries[:count]
    print(f"[WARN] Could not derive enough option expiries from quote-derivative for {symbol}; using dynamic fallback {selected}.")
    return selected

def build_raw_dataframe(data, symbol):
    """Convert NSE quote-derivative JSON response into a flat raw DataFrame."""
    stocks = data.get('stocks', [])
    if not stocks:
        return pd.DataFrame()

    metadata_rows = []
    trade_info_rows = []
    other_info_rows = []

    for item in stocks:
        metadata_rows.append(item.get('metadata') or {})
        order_book = item.get('marketDeptOrderBook') or {}
        trade_info_rows.append(order_book.get('tradeInfo') or {})
        other_info_rows.append(order_book.get('otherInfo') or {})

    df1 = pd.DataFrame(metadata_rows).drop(columns=['identifier'], errors='ignore')
    df1.rename(columns={'numberOfContractsTraded': 'tradedContracts'}, inplace=True, errors='ignore')

    df2 = pd.DataFrame(trade_info_rows).rename(columns={
        'vmap': 'vwap',
        'changeinOpenInterest': 'changeinOI',
        'pchangeinOpenInterest': 'pchangeinOI'
    })

    df3 = pd.DataFrame(other_info_rows).drop(columns=[
        'settlementPrice',
        'clientWisePositionLimits',
        'marketWidePositionLimits'
    ], errors='ignore')

    df = pd.concat([df1, df2, df3], axis=1)

    # If the same column appears from multiple JSON sections, keep the first copy.
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Raw-only mode: do not crash or filter out rows when openInterest is missing.
    if 'openInterest' not in df.columns:
        print(f"[WARN] openInterest column missing for {symbol}; storing raw rows without OI filter.")

    actual_symbol = (data.get('info') or {}).get('symbol', symbol)
    df.insert(0, 'symbol', actual_symbol)
    df['spotPrice'] = data.get('underlyingValue')

    # Kept from your earlier script for DB compatibility.
    df.replace('-', 'xx', inplace=True)

    if 'tickSize' in df.columns:
        df.drop(columns=['tickSize'], inplace=True)

    return df


def build_option_chain_v3_dataframe(data, symbol):
    """Convert NSE option-chain-v3 JSON into rows compatible with options storage."""
    records = data.get('records') or data
    chain_rows = records.get('data') or data.get('data') or []
    if not chain_rows:
        return pd.DataFrame()

    spot_price = (
        records.get('underlyingValue')
        or data.get('underlyingValue')
        or (data.get('filtered') or {}).get('underlyingValue')
    )

    rows = []
    for item in chain_rows:
        for option_type in ('CE', 'PE'):
            option = item.get(option_type)
            if not isinstance(option, dict):
                continue

            row = option.copy()
            row.setdefault('strikePrice', item.get('strikePrice'))
            row.setdefault('expiryDate', item.get('expiryDate'))
            row['optionType'] = option_type
            row['instrumentType'] = 'Index Options'
            row['symbol'] = row.get('underlying') or symbol
            row['spotPrice'] = spot_price or row.get('underlyingValue')
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.rename(columns={
        'changeinOpenInterest': 'changeinOI',
        'pchangeinOpenInterest': 'pchangeinOI',
        'totalTradedVolume': 'tradedContracts'
    }, inplace=True, errors='ignore')
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df.replace('-', 'xx', inplace=True)
    return df


def build_live_options_dataframe(data, symbol):
    """Convert NSE liveEquity-derivatives JSON into current raw option rows."""
    rows = data.get('data') or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if 'underlying' in df.columns:
        df = df[df['underlying'].astype(str).str.upper() == symbol].copy()

    if 'instrumentType' in df.columns:
        df = df[df['instrumentType'].astype(str).str.upper() == 'OPTIDX'].copy()

    if df.empty:
        return df

    df.rename(columns={
        'underlying': 'symbol',
        'premiumTurnOver': 'premiumTurnover',
        'changeinOpenInterest': 'changeinOI',
        'pchangeinOpenInterest': 'pchangeinOI'
    }, inplace=True, errors='ignore')
    df['instrumentType'] = 'Index Options'
    if 'optionType' in df.columns:
        df['optionType'] = df['optionType'].map(normalize_live_option_type)
    if 'volume' in df.columns:
        df['tradedContracts'] = df['volume']
        df['tradedVolume'] = df['volume']
    if 'underlyingValue' in df.columns:
        df['spotPrice'] = df['underlyingValue']
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df.replace('-', 'xx', inplace=True)
    return df


def build_live_futures_dataframe(data, symbol):
    """Convert NSE liveEquity-derivatives JSON into current raw futures rows."""
    rows = data.get('data') or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if 'underlying' in df.columns:
        df = df[df['underlying'].astype(str).str.upper() == symbol].copy()

    if 'instrumentType' in df.columns:
        df = df[df['instrumentType'].astype(str).str.upper() == 'FUTIDX'].copy()

    if df.empty:
        return df

    df.rename(columns={
        'underlying': 'symbol',
        'premiumTurnOver': 'premiumTurnover',
        'totalTradedVolume': 'tradedContracts',
        'changeinOpenInterest': 'changeinOI',
        'pchangeinOpenInterest': 'pchangeinOI'
    }, inplace=True, errors='ignore')
    df['instrumentType'] = 'Index Futures'
    if 'volume' in df.columns:
        df['tradedContracts'] = df['volume']
        df['tradedVolume'] = df['volume']
    if 'underlyingValue' in df.columns:
        df['spotPrice'] = df['underlyingValue']
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df.replace('-', 'xx', inplace=True)
    return df


def fetch_live_options(symbol):
    """Fetch option rows from NSE liveEquity-derivatives endpoint."""
    index_param = OPTION_INDEX_PARAM.get(symbol)
    if not index_param:
        return pd.DataFrame(), None

    request_headers = headers.copy()
    request_headers['referer'] = 'https://www.nseindia.com/market-data/equity-derivatives-watch'
    url = f'https://www.nseindia.com/api/liveEquity-derivatives?index={quote(index_param)}'
    response = session.get(url, headers=request_headers, timeout=15)
    response.raise_for_status()
    opt_data = response.json()
    return build_live_options_dataframe(opt_data, symbol), opt_data


def fetch_live_futures(symbol):
    """Fetch futures rows from NSE liveEquity-derivatives endpoint."""
    index_param = FUTURES_INDEX_PARAM.get(symbol)
    if not index_param:
        return pd.DataFrame(), None

    request_headers = headers.copy()
    request_headers['referer'] = 'https://www.nseindia.com/market-data/equity-derivatives-watch'
    url = f'https://www.nseindia.com/api/liveEquity-derivatives?index={quote(index_param)}'
    response = session.get(url, headers=request_headers, timeout=15)
    response.raise_for_status()
    fut_data = response.json()
    return build_live_futures_dataframe(fut_data, symbol), fut_data



def fetch_option_contract_info_expiries(symbol, count=1):
    """
    Fetch option expiries from NSE contract-info endpoint.

    Source endpoint:
      https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY

    This endpoint returns the full expiryDates list used to select current
    and next expiry for option-chain-v3 calls.
    """
    request_headers = headers.copy()
    request_headers['referer'] = 'https://www.nseindia.com/option-chain'
    url = f'https://www.nseindia.com/api/option-chain-contract-info?symbol={quote(symbol)}'

    response = session.get(url, headers=request_headers, timeout=15)
    response.raise_for_status()
    data = response.json()

    expiry_dates = flatten_expiry_dates(data.get('expiryDates'))
    expiries = get_nearest_expiries_from_list(expiry_dates, count=count)

    if expiries:
        print(f"Contract-info expiries for {symbol}: {expiries}")
    else:
        print(f"[WARN] option-chain-contract-info did not return usable expiries for {symbol}. Response keys: {list(data.keys()) if isinstance(data, dict) else []}")

    return expiries, data



def fetch_option_chain_v3(symbol, expiry):
    """Fetch options rows from NSE's option-chain-v3 endpoint for one expiry."""
    if not expiry:
        return pd.DataFrame(), None

    request_headers = headers.copy()
    request_headers['referer'] = 'https://www.nseindia.com/option-chain'
    url = (
        'https://www.nseindia.com/api/option-chain-v3'
        f'?type=Indices&symbol={quote(symbol)}&expiry={quote(expiry)}'
    )
    response = session.get(url, headers=request_headers, timeout=15)
    response.raise_for_status()
    oc_data = response.json()
    return build_option_chain_v3_dataframe(oc_data, symbol), oc_data



def fetch_quote_derivative_optional(symbol):
    """
    Optional quote-derivative fetch.

    NSE can return 404 for /api/quote-derivative?symbol=NIFTY. This helper
    never stops the cycle. Options are fetched from contract-info +
    option-chain-v3, and futures are fetched from liveEquity-derivatives.
    """
    try:
        _prime_nse_session(symbol)
        request_headers = headers.copy()
        request_headers['referer'] = f'https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}'
        url = f'https://www.nseindia.com/api/quote-derivative?symbol={quote(symbol)}'
        response = session.get(url, headers=request_headers, timeout=15)

        if response.status_code == 404:
            print(f"[INFO] quote-derivative endpoint returned 404 for {symbol}; continuing with contract-info/option-chain-v3 + live futures.")
            return {}, pd.DataFrame()

        response.raise_for_status()
        data = response.json()
        return data, build_raw_dataframe(data, symbol)

    except (requests.exceptions.RequestException, json.JSONDecodeError) as exc:
        print(f"[INFO] Optional quote-derivative fetch failed for {symbol}: {type(exc).__name__}: {exc}")
        return {}, pd.DataFrame()



# ============================== CORE FETCH ==============================
def getIndexData(url, symbol):
    print('=' * 50)

    try:
        for attempt in range(1, 4):
            try:
                header, dct = _read_cookies_file()
                _install_cookies(header, dct)
                _prime_nse_session(symbol)

                now_ts = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
                fut_ts = now_ts
                opt_ts = now_ts

                option_expiry_count = 2 if symbol == 'NIFTY' else 1

                # 1) Option expiries: use contract-info first.
                try:
                    option_expiries, contract_info_data = fetch_option_contract_info_expiries(symbol, count=option_expiry_count)
                except Exception as expiry_error:
                    option_expiries = []
                    print(f"[WARN] Could not fetch option-chain-contract-info expiries for {symbol}: {type(expiry_error).__name__}: {expiry_error}")

                # 2) Optional quote-derivative. Do not fail the cycle if it is 404.
                data, df = fetch_quote_derivative_optional(symbol)

                if not option_expiries and isinstance(data, dict):
                    option_expiries = get_option_expiries_from_quote_response(data, symbol, count=option_expiry_count)

                if not option_expiries:
                    option_expiries = get_dynamic_option_expiries(symbol, count=option_expiry_count)
                    print(f"[WARN] Using dynamic option expiry fallback for {symbol}: {option_expiries}")

                print(f"Selected option expiries for {symbol}: {option_expiries}")

                # 3) Options: fetch ALL strikes for each selected expiry from option-chain-v3.
                fetched_option_frames = []
                for expiry in option_expiries:
                    print(f"Fetching Options data for {symbol} from option-chain-v3 expiry {expiry}...")
                    fetched_optdf, oc_data = fetch_option_chain_v3(symbol, expiry)
                    if fetched_optdf.empty:
                        print(f"[WARN] option-chain-v3 returned no rows for {symbol} expiry {expiry}. Response keys: {list(oc_data.keys()) if oc_data else []}")
                    else:
                        fetched_option_frames.append(fetched_optdf)
                        if oc_data:
                            opt_ts = get_option_chain_timestamp(oc_data, opt_ts)

                optdf = pd.concat(fetched_option_frames, ignore_index=True) if fetched_option_frames else pd.DataFrame()
                if not optdf.empty:
                    optdf = optdf.loc[:, ~optdf.columns.duplicated()].copy()

                # 3b) Rich option OHLC/volume/noOfTrades from liveEquity-derivatives.
                try:
                    print(f"Fetching rich Options data for {symbol} from liveEquity-derivatives...")
                    live_optdf, live_opt_data = fetch_live_options(symbol)
                    if live_opt_data:
                        opt_ts = get_live_futures_timestamp(live_opt_data, opt_ts)
                    if not live_optdf.empty:
                        live_optdf = live_optdf.loc[:, ~live_optdf.columns.duplicated()].copy()
                        if option_expiries and 'expiryDate' in live_optdf.columns:
                            live_optdf = pd.concat(
                                [filter_by_expiry_date(live_optdf, expiry) for expiry in option_expiries],
                                ignore_index=True,
                            )
                        if not optdf.empty:
                            optdf = merge_live_option_fields(optdf, live_optdf)
                        else:
                            optdf = live_optdf
                            print(f"[WARN] option-chain-v3 empty; using liveEquity option rows for {symbol}.")
                        live_cols_present = [c for c in ["volume", "noOfTrades", "openPrice", "highPrice", "lowPrice", "lastPrice", "openInterest", "premiumTurnover"] if c in optdf.columns]
                        print(f"[INFO] Option liveEquity fields available for {symbol}: {live_cols_present}")
                    else:
                        print(f"[WARN] liveEquity-derivatives returned no option rows for {symbol}. Response keys: {list(live_opt_data.keys()) if live_opt_data else []}")
                except Exception as live_opt_error:
                    print(f"[WARN] Rich live option fetch skipped for {symbol}: {type(live_opt_error).__name__}: {live_opt_error}")

                # 4) Futures: use quote-derivative if it still returns rows, otherwise liveEquity-derivatives.
                futdf = pd.DataFrame()
                nearest_fut_expiry = None

                if not df.empty:
                    nearest_fut_expiry = get_nearest_expiry(df, 'Index Futures')
                    if nearest_fut_expiry:
                        print(f"Selected futures expiry for {symbol}: {nearest_fut_expiry}")
                        futdf = df[df['instrumentType'] == 'Index Futures'].copy()
                        futdf = filter_by_expiry_date(futdf, nearest_fut_expiry)
                        if isinstance(data, dict):
                            fut_ts = valid_timestamp(data.get('fut_timestamp') or data.get('timestamp'), fut_ts)

                if futdf.empty:
                    print(f"Fetching Futures data for {symbol} from liveEquity-derivatives...")
                    futdf, fut_data = fetch_live_futures(symbol)
                    if fut_data:
                        fut_ts = get_live_futures_timestamp(fut_data, fut_ts)
                    nearest_fut_expiry = get_nearest_expiry(futdf, 'Index Futures')
                    if nearest_fut_expiry:
                        print(f"Selected futures expiry for {symbol}: {nearest_fut_expiry}")
                        futdf = filter_by_expiry_date(futdf, nearest_fut_expiry)
                    else:
                        print(f"[WARN] Could not derive futures expiry for {symbol}.")
                    if futdf.empty:
                        print(f"[WARN] liveEquity-derivatives returned no futures rows for {symbol}. Response keys: {list(fut_data.keys()) if fut_data else []}")

                if futdf.empty and optdf.empty:
                    if attempt < 3:
                        print('No option/futures rows received. Refreshing cookies/session and retrying...')
                        autocookie.getCookies()
                        header, dct = _read_cookies_file()
                        _install_cookies(header, dct)
                        time.sleep(3 * attempt + random.uniform(0, 1))
                        continue
                    break

                # ---- Futures raw write ----
                if not futdf.empty:
                    print(f"Upserting RAW Futures data for {symbol} @ {fut_ts}...")
                    if 'timestamp' in futdf.columns:
                        futdf.drop(columns=['timestamp'], inplace=True)
                    futdf.insert(len(futdf.columns), 'timestamp', fut_ts)
                    # Existing futures table already has volume/noOfTrades fields.
                    # Do not add new fields; align into current table schema only.
                    ensure_live_derivative_columns(engine1, symbol)
                    futdf = align_to_existing_table(engine1, symbol, futdf)
                    with engine1.begin() as con1:
                        replaced = delete_existing_timestamp_rows(con1, symbol, fut_ts)
                        if replaced:
                            print(f"[UPSERT] Replaced {replaced} existing futures rows for {symbol} @ {fut_ts}.")
                        futdf.to_sql(symbol, con=con1, if_exists='append', index=False)
                    print(f"Futures RAW data upserted @ {datetime.now()}")
                else:
                    print(f"No futures rows found for {symbol} @ {fut_ts}.")

                # ---- Options raw write ----
                if not optdf.empty:
                    print(f"Upserting RAW Options data for {symbol} @ {opt_ts}...")
                    if 'timestamp' in optdf.columns:
                        optdf.drop(columns=['timestamp'], inplace=True)
                    optdf.insert(len(optdf.columns), 'timestamp', opt_ts)
                    # Existing options table already has volume/noOfTrades fields.
                    # Do not add new fields; align into current table schema only.
                    ensure_live_derivative_columns(engine2, symbol)
                    optdf = align_to_existing_table(engine2, symbol, optdf)
                    with engine2.begin() as con2:
                        replaced = delete_existing_option_snapshot_rows(con2, symbol, optdf)
                        if replaced:
                            print(f"[UPSERT] Replaced {replaced} existing option rows for {symbol} @ {opt_ts}.")
                        optdf.to_sql(symbol, con=con2, if_exists='append', index=False)
                    print(f"Options RAW data upserted @ {datetime.now()}")
                else:
                    print(f"No option rows found for {symbol} @ {opt_ts}.")

                primary_option_expiry = option_expiries[0] if option_expiries else None
                refresh_option_buying_ai_cache(
                    symbol,
                    trade_date=datetime.now().strftime('%Y-%m-%d'),
                    expiry=primary_option_expiry,
                )

                break

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                print(f"[ERROR] Attempt {attempt} for {symbol} - {e}")
                print('Refreshing cookies and retrying...')
                autocookie.getCookies()
                try:
                    header, dct = _read_cookies_file()
                    _install_cookies(header, dct)
                except Exception:
                    pass
                time.sleep(3 * attempt + random.uniform(0, 1))

            except Exception as e:
                # Do not stop the whole market loop because of one symbol/data-shape issue.
                print(f"[ERROR] {symbol} failed: {type(e).__name__}: {e}")
                try:
                    engine1.dispose()
                    engine2.dispose()
                except Exception:
                    pass
                break

    finally:
        print('Connections closed.')
        print('=' * 50)



def ist_now():
    return datetime.now(IST)


def market_is_open(now=None):
    now = now or ist_now()
    return now.weekday() < 5 and MARKET_START <= now.time().replace(tzinfo=None) <= MARKET_END


def main():
    print('[READY] NIFTY cloud live worker started. Market window 09:14-15:50 IST.', flush=True)
    while True:
        now = ist_now()
        if not market_is_open(now):
            if now.minute % 15 == 0 and now.second < 5:
                print(f'[WAIT] Outside market hours: {now:%d-%b-%Y %H:%M:%S IST}', flush=True)
            time.sleep(30)
            continue

        for symbol in symbols:
            url = f'https://www.nseindia.com/api/quote-derivative?symbol={symbol}'
            getIndexData(url, symbol)

        run_cash_money_flow_cycle()
        print(f'[SLEEP] Next {WAIT_SECONDS}-second cycle at {ist_now():%d-%b-%Y %H:%M:%S IST}', flush=True)
        time.sleep(max(1.0, WAIT_SECONDS - time.time() % WAIT_SECONDS))


if __name__ == '__main__':
    main()
