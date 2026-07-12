# -*- coding: utf-8 -*-
"""
NIFTY 50 live cash-basket money-flow collector.

Fetches live NIFTY 50 constituent snapshots from NSE, stores them in a separate
PostgreSQL database, and calculates a 1-minute cash-flow proxy for the 360
Money Flow dashboard.

Default database: idxcashdata_current
Tables:
- nifty50_cash_constituents
- nifty50_cash_flow_1m
- nifty50_cash_flow_summary_1m

Formula:
    minute_volume = current cumulative volume - previous snapshot volume
    typical_price = (day_high + day_low + last_price) / 3
    gross_flow = minute_volume * typical_price
    mfm = ((last - low) - (high - last)) / (high - low), clipped -1..1
    signed_flow = gross_flow * mfm

First snapshot of the day stores raw rows but uses minute_volume = 0 because
we do not know how much of the existing cumulative stock volume belongs only
to the latest minute.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import requests
import sqlalchemy
from pathlib import Path
from cloud_db import make_schema_engine, ensure_logical_database
from sqlalchemy import text
from sqlalchemy.engine import URL

try:
    import psycopg2
    from psycopg2 import sql as pgsql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except Exception:
    psycopg2 = None
    pgsql = None
    ISOLATION_LEVEL_AUTOCOMMIT = None

try:
    import autocookie
except Exception:
    autocookie = None

BASE_DIR = os.getenv("APP_DATA_DIR", str(Path(__file__).resolve().parent / "data"))
os.makedirs(BASE_DIR, exist_ok=True)
COOKIES_PATH = os.path.join(BASE_DIR, "cookies.txt")
DBINFO_PATH = os.path.join(BASE_DIR, "dbinfo.txt")
DEFAULT_DATABASE = "idxcashdata_current"
DEFAULT_INDEX_NAME = "NIFTY 50"
WAIT_SECONDS = 60
MARKET_CLOSE = "15:35"
CONSTITUENT_TABLE = "nifty50_cash_constituents"
FLOW_TABLE = "nifty50_cash_flow_1m"
SUMMARY_TABLE = "nifty50_cash_flow_summary_1m"

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "accept": "application/json,text/plain,*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9,hi;q=0.8",
    "referer": "https://www.nseindia.com/market-data/live-equity-market",
}
SESSION = requests.Session()


def read_dbinfo(path: str = DBINFO_PATH) -> Tuple[str, str, str, int]:
    parts = open(path, "r", encoding="utf-8").read().split()
    if len(parts) < 3:
        raise ValueError(f"DB info file must contain: <user> <password> <host> [port]. Got {path}")
    user, password, host = parts[:3]
    port = int(parts[3]) if len(parts) >= 4 and str(parts[3]).isdigit() else 5432
    if ":" in host:
        host, port_text = host.rsplit(":", 1)
        if port_text.isdigit():
            port = int(port_text)
    return user, password, host, port


def make_url(database: str, dbinfo_path: str = DBINFO_PATH):
    return None


def make_engine(database: str, dbinfo_path: str = DBINFO_PATH) -> sqlalchemy.Engine:
    return make_schema_engine(database)


def ensure_database(database: str, dbinfo_path: str = DBINFO_PATH) -> None:
    ensure_logical_database(database)

def safe_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return float(value)
    raw = str(value).replace(",", "").strip()
    if not raw or raw.lower() in {"-", "--", "none", "nan", "null", "xx"}:
        return None
    try:
        number = float(raw)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def fmt_ts(value: Optional[Any] = None) -> str:
    if value is None:
        return datetime.now().strftime("%d-%b-%Y %H:%M:%S")
    raw = str(value).strip()
    if not raw:
        return datetime.now().strftime("%d-%b-%Y %H:%M:%S")
    for dayfirst in (True, False):
        try:
            parsed = pd.to_datetime(raw, dayfirst=dayfirst, errors="raise")
            return parsed.strftime("%d-%b-%Y %H:%M:%S")
        except Exception:
            pass
    return datetime.now().strftime("%d-%b-%Y %H:%M:%S")


def trade_date_from_ts(timestamp: str) -> str:
    return pd.to_datetime(timestamp, dayfirst=True).strftime("%Y-%m-%d")


def pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return ((current - previous) / previous) * 100.0


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=str, ensure_ascii=False)


def _read_cookies_file() -> Tuple[str, Optional[Dict[str, str]]]:
    with open(COOKIES_PATH, "r", encoding="utf-8") as f:
        txt = f.read().strip()
    try:
        cookie_dict = json.loads(txt)
        return "; ".join(f"{k}={v}" for k, v in cookie_dict.items()), cookie_dict
    except Exception:
        cookie_dict: Dict[str, str] = {}
        for part in txt.split(";"):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            key = key.strip()
            if key:
                cookie_dict[key] = value.strip()
        return txt, cookie_dict or None


def _install_cookies(cookie_dict: Optional[Dict[str, str]]) -> None:
    if cookie_dict:
        for key, value in cookie_dict.items():
            SESSION.cookies.set(key, value)
    HEADERS.pop("cookie", None)


def _prime_nse_session(index_name: str = DEFAULT_INDEX_NAME) -> None:
    SESSION.get("https://www.nseindia.com/", headers=HEADERS, timeout=15)
    referer = f"https://www.nseindia.com/market-data/live-equity-market?symbol={quote(index_name)}"
    req_headers = dict(HEADERS)
    req_headers["referer"] = referer
    SESSION.get(referer, headers=req_headers, timeout=15)


def bootstrap_cookies(index_name: str = DEFAULT_INDEX_NAME, force_refresh: bool = False) -> None:
    if not force_refresh:
        try:
            _header, cookie_dict = _read_cookies_file()
            _install_cookies(cookie_dict)
            _prime_nse_session(index_name)
            return
        except Exception as exc:
            print(f"[COOKIES] Existing cookies failed: {type(exc).__name__}: {exc}")
    if autocookie is None:
        raise RuntimeError("autocookie.py is not importable and NSE cookies are not ready")
    print("[COOKIES] Refreshing via autocookie.getCookies() ...")
    autocookie.getCookies()
    _header, cookie_dict = _read_cookies_file()
    _install_cookies(cookie_dict)
    _prime_nse_session(index_name)

def create_tables(engine: sqlalchemy.Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {CONSTITUENT_TABLE} (
                index_symbol text NOT NULL,
                symbol text NOT NULL,
                company_name text,
                industry text,
                series text,
                free_float_mcap numeric,
                index_weight_pct numeric,
                index_weight_source text,
                last_seen_date date,
                last_seen_timestamp text,
                raw_json jsonb,
                updated_at timestamptz DEFAULT now(),
                PRIMARY KEY (index_symbol, symbol)
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {FLOW_TABLE} (
                id bigserial PRIMARY KEY,
                index_symbol text NOT NULL,
                trade_date date NOT NULL,
                timestamp text NOT NULL,
                symbol text NOT NULL,
                company_name text,
                industry text,
                series text,
                open_price numeric,
                high_price numeric,
                low_price numeric,
                last_price numeric,
                previous_close numeric,
                previous_snapshot_price numeric,
                price_change numeric,
                price_change_pct numeric,
                day_volume numeric,
                previous_day_volume numeric,
                minute_volume numeric,
                day_value_raw numeric,
                typical_price numeric,
                money_flow_multiplier numeric,
                gross_flow numeric,
                signed_flow numeric,
                gross_flow_cr numeric,
                signed_flow_cr numeric,
                flow_direction text,
                free_float_mcap numeric,
                index_weight_pct numeric,
                weighted_signed_flow_cr numeric,
                raw_json jsonb,
                created_at timestamptz DEFAULT now(),
                UNIQUE (index_symbol, trade_date, timestamp, symbol)
            )
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{FLOW_TABLE}_date_ts
            ON {FLOW_TABLE} (index_symbol, trade_date, timestamp)
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{FLOW_TABLE}_symbol_date
            ON {FLOW_TABLE} (symbol, trade_date)
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
                id bigserial PRIMARY KEY,
                index_symbol text NOT NULL,
                trade_date date NOT NULL,
                timestamp text NOT NULL,
                symbol_count integer,
                advance_count integer,
                decline_count integer,
                flat_count integer,
                gross_flow_cr numeric,
                positive_flow_cr numeric,
                negative_flow_cr numeric,
                net_flow_cr numeric,
                weighted_net_flow_cr numeric,
                flow_score numeric,
                weighted_flow_score numeric,
                breadth_score numeric,
                final_cash_score numeric,
                cash_direction text,
                top_inflow_symbol text,
                top_inflow_cr numeric,
                top_outflow_symbol text,
                top_outflow_cr numeric,
                payload_json jsonb,
                created_at timestamptz DEFAULT now(),
                UNIQUE (index_symbol, trade_date, timestamp)
            )
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{SUMMARY_TABLE}_date_ts
            ON {SUMMARY_TABLE} (index_symbol, trade_date, timestamp)
        """))


def fetch_nifty50_snapshot(index_name: str = DEFAULT_INDEX_NAME, retry_refresh_cookie: bool = True) -> Dict[str, Any]:
    req_headers = dict(HEADERS)
    req_headers["referer"] = f"https://www.nseindia.com/market-data/live-equity-market?symbol={quote(index_name)}"
    req_headers["cache-control"] = "no-cache"
    req_headers["pragma"] = "no-cache"
    cachebuster = int(time.time() * 1000)
    endpoints = [
        f"https://www.nseindia.com/api/equity-stock-indices?index={quote(index_name)}&_={cachebuster}",
        f"https://www.nseindia.com/api/equity-stockIndex?index={quote(index_name)}&_={cachebuster}",
        f"https://www.nseindia.com/api/equity-stockIndices?index={quote(index_name)}&_={cachebuster}",
    ]
    last_error = None
    for pos, url in enumerate(endpoints):
        try:
            response = SESSION.get(url, headers=req_headers, timeout=20)
            if response.status_code in {401, 403} and retry_refresh_cookie:
                print(f"[NSE] {response.status_code}; refreshing cookies and retrying")
                bootstrap_cookies(index_name, force_refresh=True)
                response = SESSION.get(url, headers=req_headers, timeout=20)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("data"), list) and payload.get("data"):
                if pos:
                    print(f"[NSE] Using fallback endpoint: {url}")
                return payload
            last_error = RuntimeError(f"Endpoint returned no data rows: {url}")
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"No NSE NIFTY50 constituent endpoint returned data. Last error: {last_error}")


def snapshot_timestamp(payload: Dict[str, Any]) -> str:
    rows = payload.get("data") or []
    row_times = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol in {"NIFTY 50", "NIFTY50"}:
                continue
            for key in ("lastUpdateTime", "timestamp", "timeStamp"):
                value = row.get(key)
                if value:
                    try:
                        row_times.append(pd.to_datetime(value, errors="raise"))
                    except Exception:
                        pass
                    break
    if row_times:
        return max(row_times).strftime("%d-%b-%Y %H:%M:%S")

    metadata = payload.get("metadata") or {}
    if isinstance(metadata, dict):
        for key in ("lastUpdateTime", "timestamp", "timeStamp"):
            value = metadata.get(key)
            if value:
                return fmt_ts(value)
    for key in ("lastUpdateTime", "timestamp", "timeStamp"):
        value = payload.get(key)
        if value:
            return fmt_ts(value)
    return fmt_ts()


def stock_rows_from_payload(payload: Dict[str, Any], index_name: str) -> List[Dict[str, Any]]:
    rows = payload.get("data") or []
    if not isinstance(rows, list):
        return []
    index_keys = {index_name.upper(), index_name.upper().replace(" ", "")}
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol or symbol in index_keys:
            continue
        series = str(row.get("series") or "").strip().upper()
        if series and series != "EQ":
            continue
        out.append(row)
    return out


def row_symbol(row: Dict[str, Any]) -> str:
    return str(row.get("symbol") or "").strip().upper()


def row_text(row: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def row_number(row: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = safe_number(row.get(key))
        if value is not None:
            return value
    return None


def calculate_weights(rows: List[Dict[str, Any]]) -> Dict[str, Tuple[float, str]]:
    ffmc_by_symbol: Dict[str, float] = {}
    for row in rows:
        symbol = row_symbol(row)
        ffmc = row_number(row, "ffmc", "ffmcap", "freeFloatMarketCap", "marketCap")
        if symbol and ffmc is not None and ffmc > 0:
            ffmc_by_symbol[symbol] = ffmc
    total_ffmc = sum(ffmc_by_symbol.values())
    if total_ffmc > 0:
        return {symbol: ((ffmc / total_ffmc) * 100.0, "ffmc") for symbol, ffmc in ffmc_by_symbol.items()}
    count = max(1, len({row_symbol(row) for row in rows if row_symbol(row)}))
    return {row_symbol(row): (100.0 / count, "equal") for row in rows if row_symbol(row)}


def latest_stored_timestamp(engine: sqlalchemy.Engine, index_name: str, trade_date: str) -> Optional[str]:
    query = text(f"""
        SELECT timestamp
        FROM {SUMMARY_TABLE}
        WHERE index_symbol = :index_symbol
          AND trade_date = CAST(:trade_date AS date)
        ORDER BY id DESC
        LIMIT 1
    """)
    try:
        with engine.connect() as conn:
            latest_ts = conn.execute(query, {
                "index_symbol": index_name,
                "trade_date": trade_date,
            }).scalar()
        return str(latest_ts) if latest_ts else None
    except Exception:
        return None


def timestamp_position_vs_latest(engine: sqlalchemy.Engine, index_name: str, trade_date: str, timestamp: str) -> Tuple[str, Optional[str]]:
    """Return new/same/older compared with the latest stored cash summary timestamp."""
    latest_ts = latest_stored_timestamp(engine, index_name, trade_date)
    if not latest_ts:
        return "new", None
    try:
        current_dt = pd.to_datetime(timestamp, dayfirst=True, errors="raise")
        latest_dt = pd.to_datetime(latest_ts, dayfirst=True, errors="raise")
    except Exception:
        return "new", latest_ts
    if current_dt < latest_dt:
        return "older", latest_ts
    if current_dt == latest_dt:
        return "same", latest_ts
    return "new", latest_ts


def timestamp_already_stored(engine: sqlalchemy.Engine, index_name: str, trade_date: str, timestamp: str) -> bool:
    position, _latest_ts = timestamp_position_vs_latest(engine, index_name, trade_date, timestamp)
    return position in {"same", "older"}


def get_previous_snapshot_map(engine: sqlalchemy.Engine, index_name: str, trade_date: str, current_timestamp: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    current_filter = " AND timestamp <> :current_timestamp" if current_timestamp else ""
    query = text(f"""
        SELECT DISTINCT ON (symbol)
            symbol, timestamp, day_volume, last_price, typical_price, money_flow_multiplier, signed_flow
        FROM {FLOW_TABLE}
        WHERE index_symbol = :index_symbol
          AND trade_date = CAST(:trade_date AS date)
          {current_filter}
        ORDER BY symbol, id DESC
    """)
    params = {"index_symbol": index_name, "trade_date": trade_date}
    if current_timestamp:
        params["current_timestamp"] = current_timestamp
    try:
        df = pd.read_sql(query, engine, params=params)
    except Exception:
        return {}
    if df.empty:
        return {}
    return {str(row["symbol"]).upper(): row.to_dict() for _, row in df.iterrows()}


def cash_average_price(
    high_price: Optional[float],
    low_price: Optional[float],
    open_price: Optional[float],
    last_price: Optional[float],
) -> Optional[float]:
    if high_price is not None and low_price is not None and high_price > 0 and low_price > 0:
        return (high_price + low_price) / 2.0
    if open_price is not None and last_price is not None and open_price > 0 and last_price > 0:
        return (open_price + last_price) / 2.0
    if last_price is not None and last_price > 0:
        return last_price
    if open_price is not None and open_price > 0:
        return open_price
    return None


def money_flow_multiplier(
    average_price: Optional[float],
    previous_average_price: Optional[float],
    open_price: Optional[float],
    last_price: Optional[float],
    previous_multiplier: Optional[float],
    previous_signed_flow: Optional[float],
) -> float:
    eps = 1e-9
    if average_price is not None and previous_average_price not in (None, 0):
        if average_price > previous_average_price + eps:
            return 1.0
        if average_price < previous_average_price - eps:
            return -1.0
        prev_mult = safe_number(previous_multiplier)
        if prev_mult not in (None, 0):
            return 1.0 if prev_mult > 0 else -1.0
        prev_signed = safe_number(previous_signed_flow)
        if prev_signed not in (None, 0):
            return 1.0 if prev_signed > 0 else -1.0
    if open_price is not None and last_price is not None:
        if last_price > open_price + eps:
            return 1.0
        if last_price < open_price - eps:
            return -1.0
    return 0.0

def build_flow_rows(
    payload: Dict[str, Any],
    index_name: str,
    engine: sqlalchemy.Engine,
) -> Tuple[str, str, List[Dict[str, Any]]]:
    ts = snapshot_timestamp(payload)
    trade_date = trade_date_from_ts(ts)
    stock_rows = stock_rows_from_payload(payload, index_name)
    weights = calculate_weights(stock_rows)
    previous = get_previous_snapshot_map(engine, index_name, trade_date, ts)
    out = []
    for raw in stock_rows:
        symbol = row_symbol(raw)
        if not symbol:
            continue
        prev = previous.get(symbol) or {}
        open_price = row_number(raw, "open", "openPrice")
        high_price = row_number(raw, "dayHigh", "high", "highPrice")
        low_price = row_number(raw, "dayLow", "low", "lowPrice")
        last_price = row_number(raw, "lastPrice", "ltp", "closePrice")
        previous_close = row_number(raw, "previousClose", "prevClose")
        day_volume = row_number(raw, "totalTradedVolume", "volume", "totalTradedQty")
        day_value = row_number(raw, "totalTradedValue", "totalTradedValueInLakhs", "tradedValue")
        free_float_mcap = row_number(raw, "ffmc", "ffmcap", "freeFloatMarketCap", "marketCap")
        prev_volume = safe_number(prev.get("day_volume"))
        previous_snapshot_price = safe_number(prev.get("last_price"))
        previous_average_price = safe_number(prev.get("typical_price"))
        previous_multiplier = safe_number(prev.get("money_flow_multiplier"))
        previous_signed_flow = safe_number(prev.get("signed_flow"))
        minute_volume = 0.0 if day_volume is None or prev_volume is None else max(0.0, day_volume - prev_volume)
        typical_price = cash_average_price(high_price, low_price, open_price, last_price)
        mfm = money_flow_multiplier(typical_price, previous_average_price, open_price, last_price, previous_multiplier, previous_signed_flow)
        gross_flow = (minute_volume * typical_price) if typical_price is not None else 0.0
        signed_flow = gross_flow * mfm
        gross_flow_cr = gross_flow / 10000000.0
        signed_flow_cr = signed_flow / 10000000.0
        price_change = (last_price - previous_snapshot_price) if last_price is not None and previous_snapshot_price is not None else None
        if price_change is None and last_price is not None and previous_close is not None:
            price_change = last_price - previous_close
        price_change_pct = pct_change(last_price, previous_snapshot_price or previous_close)
        weight_pct, weight_source = weights.get(symbol, (0.0, "none"))
        weighted_signed_flow_cr = signed_flow_cr * (weight_pct / 100.0)
        flow_direction = "Inflow" if signed_flow_cr > 0 else ("Outflow" if signed_flow_cr < 0 else "Neutral")
        out.append({
            "index_symbol": index_name,
            "trade_date": trade_date,
            "timestamp": ts,
            "symbol": symbol,
            "company_name": row_text(raw, "companyName"),
            "industry": row_text(raw, "industry", "sector"),
            "series": row_text(raw, "series"),
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "last_price": last_price,
            "previous_close": previous_close,
            "previous_snapshot_price": previous_snapshot_price,
            "price_change": price_change,
            "price_change_pct": price_change_pct,
            "day_volume": day_volume,
            "previous_day_volume": prev_volume,
            "minute_volume": minute_volume,
            "day_value_raw": day_value,
            "typical_price": typical_price,
            "money_flow_multiplier": mfm,
            "gross_flow": gross_flow,
            "signed_flow": signed_flow,
            "gross_flow_cr": gross_flow_cr,
            "signed_flow_cr": signed_flow_cr,
            "flow_direction": flow_direction,
            "free_float_mcap": free_float_mcap,
            "index_weight_pct": weight_pct,
            "weighted_signed_flow_cr": weighted_signed_flow_cr,
            "raw_json": json_dumps(raw),
            "index_weight_source": weight_source,
        })
    return ts, trade_date, out


def build_summary(index_name: str, trade_date: str, ts: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    symbol_count = len(rows)
    advances = sum(1 for row in rows if (safe_number(row.get("price_change")) or 0) > 0)
    declines = sum(1 for row in rows if (safe_number(row.get("price_change")) or 0) < 0)
    flat = max(0, symbol_count - advances - declines)
    gross_flow_cr = sum(abs(safe_number(row.get("gross_flow_cr")) or 0) for row in rows)
    positive_flow_cr = sum(max(0.0, safe_number(row.get("signed_flow_cr")) or 0) for row in rows)
    negative_flow_cr = abs(sum(min(0.0, safe_number(row.get("signed_flow_cr")) or 0) for row in rows))
    net_flow_cr = positive_flow_cr - negative_flow_cr
    weighted_net_flow_cr = sum(safe_number(row.get("weighted_signed_flow_cr")) or 0 for row in rows)
    weighted_abs_flow_cr = sum(abs(safe_number(row.get("weighted_signed_flow_cr")) or 0) for row in rows)
    flow_base = positive_flow_cr + negative_flow_cr
    flow_score = (net_flow_cr / flow_base * 100.0) if flow_base else 0.0
    weighted_flow_score = (weighted_net_flow_cr / weighted_abs_flow_cr * 100.0) if weighted_abs_flow_cr else 0.0
    breadth_base = advances + declines
    breadth_score = ((advances - declines) / breadth_base * 100.0) if breadth_base else 0.0
    final_score = round((0.55 * flow_score) + (0.30 * weighted_flow_score) + (0.15 * breadth_score), 2)
    if final_score > 15:
        cash_direction = "Bullish Cash Inflow"
    elif final_score < -15:
        cash_direction = "Bearish Cash Outflow"
    else:
        cash_direction = "Neutral / Mixed Cash"
    top_in = max(rows, key=lambda row: safe_number(row.get("signed_flow_cr")) or 0, default={})
    top_out = min(rows, key=lambda row: safe_number(row.get("signed_flow_cr")) or 0, default={})
    top_rows = sorted(rows, key=lambda row: abs(safe_number(row.get("signed_flow_cr")) or 0), reverse=True)[:20]
    payload = {
        "index": index_name,
        "tradeDate": trade_date,
        "timestamp": ts,
        "cashDirection": cash_direction,
        "finalCashScore": final_score,
        "flowScore": round(flow_score, 2),
        "weightedFlowScore": round(weighted_flow_score, 2),
        "breadthScore": round(breadth_score, 2),
        "grossFlowCr": round(gross_flow_cr, 4),
        "positiveFlowCr": round(positive_flow_cr, 4),
        "negativeFlowCr": round(negative_flow_cr, 4),
        "netFlowCr": round(net_flow_cr, 4),
        "weightedNetFlowCr": round(weighted_net_flow_cr, 4),
        "advanceCount": advances,
        "declineCount": declines,
        "flatCount": flat,
        "topRows": [{
            "symbol": row.get("symbol"),
            "signedFlowCr": round(safe_number(row.get("signed_flow_cr")) or 0, 4),
            "grossFlowCr": round(safe_number(row.get("gross_flow_cr")) or 0, 4),
            "minuteVolume": safe_number(row.get("minute_volume")) or 0,
            "priceChange": safe_number(row.get("price_change")),
            "weightPct": safe_number(row.get("index_weight_pct")) or 0,
        } for row in top_rows],
        "note": "Live cash flow is a NIFTY 50 stock-basket proxy. Exact participant cash flow is EOD-only.",
    }
    return {
        "index_symbol": index_name,
        "trade_date": trade_date,
        "timestamp": ts,
        "symbol_count": symbol_count,
        "advance_count": advances,
        "decline_count": declines,
        "flat_count": flat,
        "gross_flow_cr": gross_flow_cr,
        "positive_flow_cr": positive_flow_cr,
        "negative_flow_cr": negative_flow_cr,
        "net_flow_cr": net_flow_cr,
        "weighted_net_flow_cr": weighted_net_flow_cr,
        "flow_score": flow_score,
        "weighted_flow_score": weighted_flow_score,
        "breadth_score": breadth_score,
        "final_cash_score": final_score,
        "cash_direction": cash_direction,
        "top_inflow_symbol": top_in.get("symbol", ""),
        "top_inflow_cr": safe_number(top_in.get("signed_flow_cr")) or 0,
        "top_outflow_symbol": top_out.get("symbol", ""),
        "top_outflow_cr": safe_number(top_out.get("signed_flow_cr")) or 0,
        "payload_json": json_dumps(payload),
    }

def upsert_constituents(conn: sqlalchemy.Connection, rows: List[Dict[str, Any]], trade_date: str, ts: str, index_name: str) -> None:
    stmt = text(f"""
        INSERT INTO {CONSTITUENT_TABLE}
            (index_symbol, symbol, company_name, industry, series, free_float_mcap,
             index_weight_pct, index_weight_source, last_seen_date, last_seen_timestamp, raw_json)
        VALUES
            (:index_symbol, :symbol, :company_name, :industry, :series, :free_float_mcap,
             :index_weight_pct, :index_weight_source, CAST(:last_seen_date AS date), :last_seen_timestamp,
             CAST(:raw_json AS jsonb))
        ON CONFLICT (index_symbol, symbol)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            industry = EXCLUDED.industry,
            series = EXCLUDED.series,
            free_float_mcap = EXCLUDED.free_float_mcap,
            index_weight_pct = EXCLUDED.index_weight_pct,
            index_weight_source = EXCLUDED.index_weight_source,
            last_seen_date = EXCLUDED.last_seen_date,
            last_seen_timestamp = EXCLUDED.last_seen_timestamp,
            raw_json = EXCLUDED.raw_json,
            updated_at = now()
    """)
    for row in rows:
        conn.execute(stmt, {
            "index_symbol": index_name,
            "symbol": row["symbol"],
            "company_name": row.get("company_name"),
            "industry": row.get("industry"),
            "series": row.get("series"),
            "free_float_mcap": row.get("free_float_mcap"),
            "index_weight_pct": row.get("index_weight_pct"),
            "index_weight_source": row.get("index_weight_source"),
            "last_seen_date": trade_date,
            "last_seen_timestamp": ts,
            "raw_json": row.get("raw_json"),
        })


def upsert_flow_rows(conn: sqlalchemy.Connection, rows: List[Dict[str, Any]]) -> int:
    stmt = text(f"""
        INSERT INTO {FLOW_TABLE}
            (index_symbol, trade_date, timestamp, symbol, company_name, industry, series,
             open_price, high_price, low_price, last_price, previous_close, previous_snapshot_price,
             price_change, price_change_pct, day_volume, previous_day_volume, minute_volume,
             day_value_raw, typical_price, money_flow_multiplier, gross_flow, signed_flow,
             gross_flow_cr, signed_flow_cr, flow_direction, free_float_mcap, index_weight_pct,
             weighted_signed_flow_cr, raw_json)
        VALUES
            (:index_symbol, CAST(:trade_date AS date), :timestamp, :symbol, :company_name, :industry, :series,
             :open_price, :high_price, :low_price, :last_price, :previous_close, :previous_snapshot_price,
             :price_change, :price_change_pct, :day_volume, :previous_day_volume, :minute_volume,
             :day_value_raw, :typical_price, :money_flow_multiplier, :gross_flow, :signed_flow,
             :gross_flow_cr, :signed_flow_cr, :flow_direction, :free_float_mcap, :index_weight_pct,
             :weighted_signed_flow_cr, CAST(:raw_json AS jsonb))
        ON CONFLICT (index_symbol, trade_date, timestamp, symbol)
        DO UPDATE SET
            last_price = EXCLUDED.last_price,
            previous_snapshot_price = EXCLUDED.previous_snapshot_price,
            price_change = EXCLUDED.price_change,
            price_change_pct = EXCLUDED.price_change_pct,
            day_volume = EXCLUDED.day_volume,
            previous_day_volume = EXCLUDED.previous_day_volume,
            minute_volume = EXCLUDED.minute_volume,
            gross_flow = EXCLUDED.gross_flow,
            signed_flow = EXCLUDED.signed_flow,
            gross_flow_cr = EXCLUDED.gross_flow_cr,
            signed_flow_cr = EXCLUDED.signed_flow_cr,
            flow_direction = EXCLUDED.flow_direction,
            index_weight_pct = EXCLUDED.index_weight_pct,
            weighted_signed_flow_cr = EXCLUDED.weighted_signed_flow_cr,
            raw_json = EXCLUDED.raw_json
    """)
    stored = 0
    for row in rows:
        conn.execute(stmt, row)
        stored += 1
    return stored


def upsert_summary(conn: sqlalchemy.Connection, summary: Dict[str, Any]) -> None:
    stmt = text(f"""
        INSERT INTO {SUMMARY_TABLE}
            (index_symbol, trade_date, timestamp, symbol_count, advance_count, decline_count, flat_count,
             gross_flow_cr, positive_flow_cr, negative_flow_cr, net_flow_cr, weighted_net_flow_cr,
             flow_score, weighted_flow_score, breadth_score, final_cash_score, cash_direction,
             top_inflow_symbol, top_inflow_cr, top_outflow_symbol, top_outflow_cr, payload_json)
        VALUES
            (:index_symbol, CAST(:trade_date AS date), :timestamp, :symbol_count, :advance_count, :decline_count, :flat_count,
             :gross_flow_cr, :positive_flow_cr, :negative_flow_cr, :net_flow_cr, :weighted_net_flow_cr,
             :flow_score, :weighted_flow_score, :breadth_score, :final_cash_score, :cash_direction,
             :top_inflow_symbol, :top_inflow_cr, :top_outflow_symbol, :top_outflow_cr, CAST(:payload_json AS jsonb))
        ON CONFLICT (index_symbol, trade_date, timestamp)
        DO UPDATE SET
            symbol_count = EXCLUDED.symbol_count,
            advance_count = EXCLUDED.advance_count,
            decline_count = EXCLUDED.decline_count,
            flat_count = EXCLUDED.flat_count,
            gross_flow_cr = EXCLUDED.gross_flow_cr,
            positive_flow_cr = EXCLUDED.positive_flow_cr,
            negative_flow_cr = EXCLUDED.negative_flow_cr,
            net_flow_cr = EXCLUDED.net_flow_cr,
            weighted_net_flow_cr = EXCLUDED.weighted_net_flow_cr,
            flow_score = EXCLUDED.flow_score,
            weighted_flow_score = EXCLUDED.weighted_flow_score,
            breadth_score = EXCLUDED.breadth_score,
            final_cash_score = EXCLUDED.final_cash_score,
            cash_direction = EXCLUDED.cash_direction,
            top_inflow_symbol = EXCLUDED.top_inflow_symbol,
            top_inflow_cr = EXCLUDED.top_inflow_cr,
            top_outflow_symbol = EXCLUDED.top_outflow_symbol,
            top_outflow_cr = EXCLUDED.top_outflow_cr,
            payload_json = EXCLUDED.payload_json
    """)
    conn.execute(stmt, summary)


def run_cycle(engine: sqlalchemy.Engine, index_name: str, force_upsert: bool = False) -> Optional[Dict[str, Any]]:
    payload = fetch_nifty50_snapshot(index_name)
    ts = snapshot_timestamp(payload)
    trade_date = trade_date_from_ts(ts)
    timestamp_position, latest_ts = timestamp_position_vs_latest(engine, index_name, trade_date, ts)
    if timestamp_position == "older":
        print(f"[SKIP] {index_name}: NSE timestamp {ts} is older than latest DB timestamp {latest_ts}. Waiting for next exchange update.")
        return None
    if timestamp_position == "same" and not force_upsert:
        print(f"[SKIP] {index_name}: NSE timestamp unchanged/already stored @ {ts}. Waiting for next exchange update.")
        return None
    if timestamp_position == "same" and force_upsert:
        print(f"[UPSERT] {index_name}: refreshing same-timestamp cash rows @ {ts}.")
    _ts, _trade_date, rows = build_flow_rows(payload, index_name, engine)
    if not rows:
        print(f"[WARN] {index_name}: no stock rows in NSE snapshot @ {ts}")
        return None
    summary = build_summary(index_name, trade_date, ts, rows)
    with engine.begin() as conn:
        upsert_constituents(conn, rows, trade_date, ts, index_name)
        stored = upsert_flow_rows(conn, rows)
        upsert_summary(conn, summary)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] {index_name} cash flow @ {ts}: "
        f"stored {stored} stocks | {summary['cash_direction']} | "
        f"score {summary['final_cash_score']:.2f} | net {summary['net_flow_cr']:.4f} Cr | "
        f"A/D/F {summary['advance_count']}/{summary['decline_count']}/{summary['flat_count']} | "
        f"top in {summary['top_inflow_symbol']} {summary['top_inflow_cr']:.4f} Cr | "
        f"top out {summary['top_outflow_symbol']} {summary['top_outflow_cr']:.4f} Cr"
    )
    return summary


def sleep_to_next_cycle(wait_seconds: int) -> None:
    delay = max(1.0, wait_seconds - (time.time() % wait_seconds))
    time.sleep(delay)


def parse_market_close(value: str) -> dtime:
    return datetime.strptime(value, "%H:%M").time()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch NIFTY 50 live cash money flow into PostgreSQL.")
    parser.add_argument("--index", default=DEFAULT_INDEX_NAME, help="NSE equity-stockIndices index name. Default: NIFTY 50")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help="PostgreSQL database to store cash flow tables.")
    parser.add_argument("--dbinfo", default=DBINFO_PATH, help="Path to dbinfo.txt containing <user> <password> <host> [port].")
    parser.add_argument("--wait", type=int, default=WAIT_SECONDS, help="Loop wait seconds. Default: 60")
    parser.add_argument("--once", action="store_true", help="Fetch/store one snapshot and exit.")
    parser.add_argument("--create-only", action="store_true", help="Create database/tables and exit without fetching NSE.")
    parser.add_argument("--refresh-cookies", action="store_true", help="Force autocookie refresh before first NSE call.")
    parser.add_argument("--market-close", default=MARKET_CLOSE, help="Stop loop at HH:MM local time. Default: 15:35")
    args = parser.parse_args()

    index_name = str(args.index or DEFAULT_INDEX_NAME).strip().upper()
    ensure_database(args.database, args.dbinfo)
    engine = make_engine(args.database, args.dbinfo)
    create_tables(engine)
    print(f"[READY] Tables are ready in database {args.database}: {CONSTITUENT_TABLE}, {FLOW_TABLE}, {SUMMARY_TABLE}")

    if args.create_only:
        return 0

    bootstrap_cookies(index_name, force_refresh=args.refresh_cookies)
    close_time = parse_market_close(args.market_close)

    while True:
        try:
            run_cycle(engine, index_name)
        except (requests.exceptions.RequestException, json.JSONDecodeError) as exc:
            print(f"[ERROR] NSE fetch failed: {type(exc).__name__}: {exc}")
            try:
                bootstrap_cookies(index_name, force_refresh=True)
            except Exception as cookie_exc:
                print(f"[WARN] Cookie refresh failed: {type(cookie_exc).__name__}: {cookie_exc}")
        except Exception as exc:
            print(f"[ERROR] Cash flow cycle failed: {type(exc).__name__}: {exc}")

        if args.once:
            break
        if datetime.now().time() >= close_time:
            print("Market close time reached. Exiting cash-flow collector.")
            break
        print(f"Sleeping until next {args.wait}-second cycle at {datetime.now()}")
        print("+" * 40)
        sleep_to_next_cycle(args.wait)

    try:
        engine.dispose()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())