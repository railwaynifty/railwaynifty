# -*- coding: utf-8 -*-
"""
NSE360 Upstox Analytics live worker.

Purpose
-------
Replace direct NSE website scraping (which is commonly blocked from cloud IPs)
with the read-only Upstox Analytics APIs while preserving the existing
PostgreSQL table names/column names consumed by the NSE360 backend/frontend.

Writes
------
options."NIFTY"     First N active option-expiry snapshots (default: 2).
futures."NIFTY"     Nearest NIFTY futures snapshot.
cash.*               NIFTY 50 cash-basket proxy tables used by 360 Money Flow.

Required environment variables
------------------------------
DATABASE_URL
UPSTOX_ANALYTICS_TOKEN

Optional environment variables
------------------------------
UPSTOX_UNDERLYING_KEY=NSE_INDEX|Nifty 50
UPSTOX_OPTION_EXPIRY_COUNT=2
UPSTOX_WAIT_SECONDS=60
UPSTOX_REQUEST_TIMEOUT=30
UPSTOX_RUN_OUTSIDE_MARKET=0
UPSTOX_DISABLE_CASH_MONEY_FLOW=0
OPTION_HISTORY_DAYS=1
OPTION_HISTORY_ATM_STEPS=10
OPTION_VACUUM_MINUTES=30
FUTURES_HISTORY_DAYS=5
CASH_HISTORY_DAYS=3
DB_STORAGE_WARN_MB=250
DB_STORAGE_PURGE_MB=300
DB_STORAGE_HARD_STOP_MB=340
DB_STORAGE_CHECK_MINUTES=5
NIFTY50_CONSTITUENTS_URL=https://niftyindices.com/IndexConstituent/ind_nifty50list.csv
APP_DATA_DIR=/app/data
SCHEMA_OPTIONS=options
SCHEMA_FUTURES=futures
SCHEMA_CASH=cash
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sys
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from cloud_db import make_schema_engine
import cash_money_flow as cash


# -------------------- CONFIG --------------------
IST = ZoneInfo("Asia/Kolkata")
PROJECT_DIR = Path(__file__).resolve().parent
APP_DATA_DIR = Path(os.getenv("APP_DATA_DIR", str(PROJECT_DIR / "data")))
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)

TOKEN = os.getenv("UPSTOX_ANALYTICS_TOKEN", "").strip()
UNDERLYING_KEY = os.getenv("UPSTOX_UNDERLYING_KEY", "NSE_INDEX|Nifty 50").strip()
OPTION_EXPIRY_COUNT = max(1, min(6, int(os.getenv("UPSTOX_OPTION_EXPIRY_COUNT", "2"))))
WAIT_SECONDS = max(15, int(os.getenv("UPSTOX_WAIT_SECONDS", "60")))
REQUEST_TIMEOUT = max(5, int(os.getenv("UPSTOX_REQUEST_TIMEOUT", "30")))
MARKET_START = dtime(9, 14)
MARKET_END = dtime(15, 50)
RUN_OUTSIDE_MARKET = os.getenv("UPSTOX_RUN_OUTSIDE_MARKET", "0").strip().lower() in {
    "1", "true", "yes", "y"
}
ENABLE_CASH = os.getenv("UPSTOX_DISABLE_CASH_MONEY_FLOW", "0").strip().lower() not in {
    "1", "true", "yes", "y"
}
OPTION_HISTORY_DAYS = max(1, int(os.getenv("OPTION_HISTORY_DAYS", "1")))
OPTION_HISTORY_ATM_STEPS = max(1, int(os.getenv("OPTION_HISTORY_ATM_STEPS", "10")))
OPTION_VACUUM_MINUTES = max(5, int(os.getenv("OPTION_VACUUM_MINUTES", "30")))
FUTURES_HISTORY_DAYS = max(1, int(os.getenv("FUTURES_HISTORY_DAYS", "5")))
CASH_HISTORY_DAYS = max(1, int(os.getenv("CASH_HISTORY_DAYS", "3")))
DB_STORAGE_WARN_MB = max(50, int(os.getenv("DB_STORAGE_WARN_MB", "250")))
DB_STORAGE_PURGE_MB = max(
    DB_STORAGE_WARN_MB + 10,
    int(os.getenv("DB_STORAGE_PURGE_MB", "300")),
)
DB_STORAGE_HARD_STOP_MB = max(
    DB_STORAGE_PURGE_MB + 10,
    int(os.getenv("DB_STORAGE_HARD_STOP_MB", "340")),
)
DB_STORAGE_CHECK_MINUTES = max(1, int(os.getenv("DB_STORAGE_CHECK_MINUTES", "5")))
# Kept only so old Railway variables do not cause confusion. The active safety
# thresholds are WARN/PURGE/HARD_STOP above.
LEGACY_DB_STORAGE_GUARD_MB = max(0, int(os.getenv("DB_STORAGE_GUARD_MB", "0")))
CONSTITUENTS_URL = os.getenv(
    "NIFTY50_CONSTITUENTS_URL",
    "https://niftyindices.com/IndexConstituent/ind_nifty50list.csv",
).strip()
FALLBACK_CONSTITUENTS = PROJECT_DIR / "nifty50_constituents_fallback.csv"

API_BASE = "https://api.upstox.com"
SYMBOL = "NIFTY"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "NSE360-Upstox-Worker/1.0",
    }
)

_OPTION_ENGINE: Optional[sqlalchemy.Engine] = None
_FUTURE_ENGINE: Optional[sqlalchemy.Engine] = None
_CASH_ENGINE: Optional[sqlalchemy.Engine] = None
_OPTION_BUYING_AI_MODULE = None
_FUTURE_CONTRACT_CACHE: Dict[str, Any] = {}
_OPTION_EXPIRY_CACHE: Dict[str, Any] = {}
_CONSTITUENT_CACHE: Dict[str, Any] = {}
_LAST_RETENTION_DATE: Optional[str] = None
_LAST_STORAGE_CHECK_MONOTONIC = 0.0
_LAST_OPTION_VACUUM_MONOTONIC = 0.0
_LAST_DATABASE_SIZE_MB = 0.0
_STORAGE_WRITES_PAUSED = False


# -------------------- GENERAL HELPERS --------------------
def now_ist() -> datetime:
    return datetime.now(IST)


def minute_bucket(value: Optional[datetime] = None) -> datetime:
    return (value or now_ist()).replace(second=0, microsecond=0)


def snapshot_timestamp(value: Optional[datetime] = None) -> str:
    return minute_bucket(value).strftime("%d-%b-%Y %H:%M:%S")


def safe_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value.lower() in {"", "-", "--", "none", "null", "nan", "xx"}:
                return None
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except Exception:
        return None


def pct_change(current: Any, previous: Any) -> Optional[float]:
    current_n = safe_number(current)
    previous_n = safe_number(previous)
    if current_n is None or previous_n in (None, 0):
        return None
    return ((current_n - previous_n) / abs(previous_n)) * 100.0


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=False)


def market_is_open(moment: Optional[datetime] = None) -> bool:
    moment = moment or now_ist()
    return moment.weekday() < 5 and MARKET_START <= moment.time() <= MARKET_END


def seconds_to_next_cycle(period: int = WAIT_SECONDS) -> float:
    return max(1.0, period - (time.time() % period))


def require_token() -> None:
    if not TOKEN:
        raise RuntimeError(
            "UPSTOX_ANALYTICS_TOKEN is missing. Add it to Railway > "
            "nse360-live-worker > Variables and redeploy."
        )
    if len(TOKEN) < 20:
        raise RuntimeError("UPSTOX_ANALYTICS_TOKEN looks incomplete; copy the full token.")


# -------------------- UPSTOX API --------------------
def upstox_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    require_token()
    url = f"{API_BASE}{path}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    response = SESSION.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
    if response.status_code in {401, 403}:
        body = response.text[:500]
        raise RuntimeError(
            f"Upstox authentication failed with HTTP {response.status_code}. "
            f"Confirm the full Analytics Token is saved in Railway. Response: {body}"
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"Upstox HTTP {response.status_code} for {path}: {response.text[:700]}"
        ) from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Upstox returned non-JSON for {path}: {response.text[:500]}") from exc
    if str(payload.get("status", "success")).lower() not in {"success", "ok"}:
        raise RuntimeError(f"Upstox API error for {path}: {payload}")
    return payload


def fetch_option_contracts() -> List[Dict[str, Any]]:
    payload = upstox_get(
        "/v2/option/contract",
        {"instrument_key": UNDERLYING_KEY},
    )
    rows = payload.get("data") or []
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected option-contract response: {type(rows).__name__}")
    return rows


def discover_upcoming_option_expiries(force: bool = False) -> List[str]:
    today = now_ist().date()
    cached_date = _OPTION_EXPIRY_CACHE.get("date")
    cached_expiries = _OPTION_EXPIRY_CACHE.get("expiries")
    if cached_date == today.isoformat() and cached_expiries and not force:
        return list(cached_expiries)[:OPTION_EXPIRY_COUNT]

    contracts = fetch_option_contracts()
    expiries: set[str] = set()
    for item in contracts:
        if not isinstance(item, dict):
            continue
        expiry_raw = item.get("expiry")
        if not expiry_raw:
            continue
        try:
            expiry_date = pd.to_datetime(expiry_raw, errors="raise").date()
        except Exception:
            continue
        if expiry_date >= today:
            expiries.add(expiry_date.isoformat())

    selected = sorted(expiries)[:OPTION_EXPIRY_COUNT]
    if len(selected) < OPTION_EXPIRY_COUNT:
        raise RuntimeError(
            f"Upstox returned only {len(selected)} active NIFTY expiries; "
            f"requested {OPTION_EXPIRY_COUNT}. Available={sorted(expiries)}"
        )
    _OPTION_EXPIRY_CACHE.update({"date": today.isoformat(), "expiries": sorted(expiries)})
    return selected


def fetch_option_chain(expiry_date: str) -> List[Dict[str, Any]]:
    payload = upstox_get(
        "/v2/option/chain",
        {"instrument_key": UNDERLYING_KEY, "expiry_date": expiry_date},
    )
    rows = payload.get("data") or []
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected option-chain response for {expiry_date}: {type(rows).__name__}")
    return rows


def search_nearest_future_contract(force: bool = False) -> Dict[str, Any]:
    today = now_ist().date()
    cached = _FUTURE_CONTRACT_CACHE.get("contract")
    cached_date = _FUTURE_CONTRACT_CACHE.get("date")
    if cached and cached_date == today.isoformat() and not force:
        return dict(cached)

    attempts = [
        {"segments": "FUT", "expiry": "current_month"},
        {"segments": "FO", "expiry": "current_month"},
    ]
    candidates: List[Dict[str, Any]] = []
    for attempt in attempts:
        payload = upstox_get(
            "/v2/instruments/search",
            {
                "query": "NIFTY",
                "exchanges": "NSE",
                "segments": attempt["segments"],
                "instrument_types": "FUT",
                "expiry": attempt["expiry"],
                "page_number": 1,
                "records": 30,
            },
        )
        data = payload.get("data") or []
        if isinstance(data, list):
            candidates.extend(data)
        if candidates:
            break

    normalized: List[Tuple[date, Dict[str, Any]]] = []
    for item in candidates:
        if str(item.get("instrument_type") or "").upper() != "FUT":
            continue
        underlying = str(item.get("underlying_symbol") or item.get("trading_symbol") or "").upper()
        if "NIFTY" not in underlying or "BANK" in underlying or "MID" in underlying or "FIN" in underlying:
            continue
        expiry_raw = item.get("expiry")
        try:
            expiry_dt = pd.to_datetime(expiry_raw, errors="raise").date()
        except Exception:
            continue
        if expiry_dt >= today:
            normalized.append((expiry_dt, item))

    if not normalized:
        raise RuntimeError("Upstox instrument search returned no active NIFTY futures contract.")
    normalized.sort(key=lambda pair: pair[0])
    contract = dict(normalized[0][1])
    _FUTURE_CONTRACT_CACHE.update({"date": today.isoformat(), "contract": contract})
    return contract


def fetch_full_quotes(instrument_keys: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    keys = [str(key).strip() for key in instrument_keys if str(key).strip()]
    if not keys:
        return {}
    if len(keys) > 500:
        raise ValueError("Upstox full-quote request supports at most 500 instrument keys.")
    payload = upstox_get(
        "/v2/market-quote/quotes",
        {"instrument_key": ",".join(keys)},
    )
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected full-quote response: {type(data).__name__}")

    by_key: Dict[str, Dict[str, Any]] = {}
    for response_key, quote in data.items():
        if not isinstance(quote, dict):
            continue
        token = str(quote.get("instrument_token") or quote.get("instrument_key") or "").strip()
        if token:
            by_key[token] = quote
        # Upstox response object keys often use SEGMENT:SYMBOL rather than the instrument key.
        by_key[str(response_key)] = quote
    return by_key


def find_quote(quotes: Dict[str, Dict[str, Any]], instrument_key: str) -> Dict[str, Any]:
    if instrument_key in quotes:
        return quotes[instrument_key]
    segment, _, token = instrument_key.partition("|")
    candidates = {
        f"{segment}:{token}",
        f"{segment}:{token.upper()}",
        f"{segment}|{token}",
    }
    for key in candidates:
        if key in quotes:
            return quotes[key]
    for quote in quotes.values():
        if str(quote.get("instrument_token") or "") == instrument_key:
            return quote
    return {}


# -------------------- DATAFRAME MAPPING --------------------
def build_options_dataframe(chains: Iterable[List[Dict[str, Any]]], ts: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    seen: set[Tuple[str, float, str]] = set()
    for chain in chains:
        for strike_item in chain:
            if not isinstance(strike_item, dict):
                continue
            expiry = str(strike_item.get("expiry") or "").strip()
            strike = safe_number(strike_item.get("strike_price"))
            spot = safe_number(strike_item.get("underlying_spot_price"))
            pcr = safe_number(strike_item.get("pcr"))
            if not expiry or strike is None:
                continue
            for side_key, side in (("call_options", "CE"), ("put_options", "PE")):
                option = strike_item.get(side_key) or {}
                if not isinstance(option, dict):
                    continue
                market = option.get("market_data") or {}
                greeks = option.get("option_greeks") or {}
                if not isinstance(market, dict):
                    market = {}
                if not isinstance(greeks, dict):
                    greeks = {}
                key = (expiry, float(strike), side)
                if key in seen:
                    continue
                seen.add(key)

                oi = safe_number(market.get("oi"))
                prev_oi = safe_number(market.get("prev_oi"))
                coi = (oi - prev_oi) if oi is not None and prev_oi is not None else None
                ltp = safe_number(market.get("ltp"))
                close_price = safe_number(market.get("close_price"))
                price_change = (ltp - close_price) if ltp is not None and close_price is not None else None
                volume = safe_number(market.get("volume"))
                instrument_key = str(option.get("instrument_key") or "").strip()

                rows.append(
                    {
                        "symbol": SYMBOL,
                        "identifier": instrument_key,
                        "instrumentKey": instrument_key,
                        "instrumentType": "Index Options",
                        "expiryDate": expiry,
                        "optionType": side,
                        "strikePrice": strike,
                        "lastPrice": ltp,
                        "closePrice": close_price,
                        "change": price_change,
                        "pChange": pct_change(ltp, close_price),
                        "tradedVolume": volume,
                        "tradedContracts": volume,
                        "volume": volume,
                        "openInterest": oi,
                        "prevOpenInterest": prev_oi,
                        "changeinOI": coi,
                        "pchangeinOI": pct_change(oi, prev_oi),
                        "impliedVolatility": safe_number(greeks.get("iv")),
                        "delta": safe_number(greeks.get("delta")),
                        "gamma": safe_number(greeks.get("gamma")),
                        "theta": safe_number(greeks.get("theta")),
                        "vega": safe_number(greeks.get("vega")),
                        "pop": safe_number(greeks.get("pop")),
                        "bidPrice": safe_number(market.get("bid_price")),
                        "bidQty": safe_number(market.get("bid_qty")),
                        "askPrice": safe_number(market.get("ask_price")),
                        "askQty": safe_number(market.get("ask_qty")),
                        "spotPrice": spot,
                        "underlyingValue": spot,
                        "pcr": pcr,
                        "dataSource": "UPSTOX_ANALYTICS",
                        "timestamp": ts,
                    }
                )
    return pd.DataFrame(rows)


def latest_and_first_future_oi(engine: sqlalchemy.Engine, trade_date: str) -> Tuple[Optional[float], Optional[float]]:
    if not table_exists(engine, SYMBOL):
        return None, None
    prefix = datetime.strptime(trade_date, "%Y-%m-%d").strftime("%d-%b-%Y") + "%"
    try:
        with engine.connect() as conn:
            first = conn.execute(
                text(
                    f'SELECT "openInterest" FROM "{SYMBOL}" '
                    'WHERE "timestamp" LIKE :prefix ORDER BY CAST("timestamp" AS TIMESTAMP) ASC LIMIT 1'
                ),
                {"prefix": prefix},
            ).scalar()
            latest = conn.execute(
                text(
                    f'SELECT "openInterest" FROM "{SYMBOL}" '
                    'WHERE "timestamp" LIKE :prefix ORDER BY CAST("timestamp" AS TIMESTAMP) DESC LIMIT 1'
                ),
                {"prefix": prefix},
            ).scalar()
        return safe_number(first), safe_number(latest)
    except Exception:
        return None, None


def build_futures_dataframe(
    contract: Dict[str, Any],
    future_quote: Dict[str, Any],
    spot_quote: Dict[str, Any],
    ts: str,
    engine: sqlalchemy.Engine,
) -> pd.DataFrame:
    if not future_quote:
        return pd.DataFrame()
    trade_date = now_ist().date().isoformat()
    first_oi, previous_oi = latest_and_first_future_oi(engine, trade_date)
    oi = safe_number(future_quote.get("oi"))
    intraday_coi = (oi - first_oi) if oi is not None and first_oi is not None else 0.0
    one_minute_coi = (oi - previous_oi) if oi is not None and previous_oi is not None else 0.0
    ohlc = future_quote.get("ohlc") or {}
    spot = safe_number(spot_quote.get("last_price"))
    if spot is None:
        spot = safe_number(future_quote.get("underlying_spot_price"))
    expiry = str(contract.get("expiry") or "").strip()
    last_price = safe_number(future_quote.get("last_price"))
    close_price = safe_number(ohlc.get("close"))
    volume = safe_number(future_quote.get("volume"))
    row = {
        "symbol": SYMBOL,
        "identifier": str(contract.get("instrument_key") or ""),
        "instrumentKey": str(contract.get("instrument_key") or ""),
        "instrumentType": "Index Futures",
        "expiryDate": expiry,
        "lastPrice": last_price,
        "openPrice": safe_number(ohlc.get("open")),
        "highPrice": safe_number(ohlc.get("high")),
        "lowPrice": safe_number(ohlc.get("low")),
        "closePrice": close_price,
        "change": safe_number(future_quote.get("net_change")),
        "pChange": pct_change(last_price, close_price),
        "vwap": safe_number(future_quote.get("average_price")),
        "tradedVolume": volume,
        "tradedContracts": volume,
        "volume": volume,
        "openInterest": oi,
        "changeinOI": intraday_coi,
        "pchangeinOI": pct_change(oi, first_oi),
        "oneMinuteChangeinOI": one_minute_coi,
        "oiDayHigh": safe_number(future_quote.get("oi_day_high")),
        "oiDayLow": safe_number(future_quote.get("oi_day_low")),
        "spotPrice": spot,
        "underlyingValue": spot,
        "dataSource": "UPSTOX_ANALYTICS",
        "timestamp": ts,
    }
    return pd.DataFrame([row])


# -------------------- DATABASE HELPERS --------------------
def option_engine() -> sqlalchemy.Engine:
    global _OPTION_ENGINE
    if _OPTION_ENGINE is None:
        _OPTION_ENGINE = make_schema_engine("idxoptionsdata_current")
    return _OPTION_ENGINE


def future_engine() -> sqlalchemy.Engine:
    global _FUTURE_ENGINE
    if _FUTURE_ENGINE is None:
        _FUTURE_ENGINE = make_schema_engine("idxfuturesdata_current")
    return _FUTURE_ENGINE


def cash_engine() -> sqlalchemy.Engine:
    global _CASH_ENGINE
    if _CASH_ENGINE is None:
        cash.ensure_database(cash.DEFAULT_DATABASE, cash.DBINFO_PATH)
        _CASH_ENGINE = cash.make_engine(cash.DEFAULT_DATABASE, cash.DBINFO_PATH)
        cash.create_tables(_CASH_ENGINE)
    return _CASH_ENGINE


def table_exists(engine: sqlalchemy.Engine, table_name: str) -> bool:
    try:
        return inspect(engine).has_table(table_name)
    except Exception:
        return False


def align_frame_to_existing_table(
    conn: sqlalchemy.Connection,
    frame: pd.DataFrame,
    table_name: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    inspector = inspect(conn)
    if not inspector.has_table(table_name):
        return frame
    existing = [column["name"] for column in inspector.get_columns(table_name)]
    out = frame.copy()
    for column in existing:
        if column not in out.columns:
            out[column] = None
    # Preserve the existing table contract. New adapter-only fields are omitted if an old table exists.
    return out[existing]


def _records_for_sql(frame: pd.DataFrame) -> List[Dict[str, Any]]:
    clean = frame.astype(object).where(pd.notna(frame), None)
    return clean.to_dict(orient="records")


def _ensure_unique_index(
    conn: sqlalchemy.Connection,
    table_name: str,
    index_name: str,
    key_columns: Sequence[str],
) -> None:
    existing_indexes = {item.get("name") for item in inspect(conn).get_indexes(table_name)}
    if index_name in existing_indexes:
        return

    quoted_columns = ", ".join(f'"{column}"' for column in key_columns)
    # Fresh cloud databases have no duplicates. On an existing database, remove
    # any exact duplicate natural keys once, before creating the unique index.
    equality = " AND ".join(f'a."{column}" IS NOT DISTINCT FROM b."{column}"' for column in key_columns)
    conn.execute(
        text(
            f'DELETE FROM "{table_name}" a USING "{table_name}" b '
            f'WHERE a.ctid < b.ctid AND {equality}'
        )
    )
    conn.execute(
        text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" '
            f'ON "{table_name}" ({quoted_columns})'
        )
    )


def upsert_frame(
    engine: sqlalchemy.Engine,
    frame: pd.DataFrame,
    table_name: str,
    key_columns: Sequence[str],
    index_name: str,
) -> int:
    if frame.empty:
        return 0
    frame = frame.drop_duplicates(subset=list(key_columns), keep="last").copy()
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table(table_name):
            frame.to_sql(table_name, con=conn, if_exists="append", index=False, method="multi", chunksize=1000)
        ready = align_frame_to_existing_table(conn, frame, table_name)
        _ensure_unique_index(conn, table_name, index_name, key_columns)
        table = Table(table_name, MetaData(), autoload_with=conn)
        records = _records_for_sql(ready)
        if records:
            stmt = pg_insert(table).values(records)
            update_values = {
                column.name: stmt.excluded[column.name]
                for column in table.columns
                if column.name not in key_columns
            }
            conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=[table.c[column] for column in key_columns],
                    set_=update_values,
                )
            )
    return len(frame)


def replace_option_snapshot(engine: sqlalchemy.Engine, frame: pd.DataFrame) -> int:
    return upsert_frame(
        engine,
        frame,
        SYMBOL,
        ("timestamp", "expiryDate", "strikePrice", "optionType"),
        "uq_nifty_minute_expiry_strike_side",
    )


def replace_future_snapshot(engine: sqlalchemy.Engine, frame: pd.DataFrame) -> int:
    return upsert_frame(
        engine,
        frame,
        SYMBOL,
        ("timestamp", "expiryDate"),
        "uq_nifty_future_minute_expiry",
    )


def database_size_mb(engine: sqlalchemy.Engine) -> float:
    with engine.connect() as conn:
        value = conn.execute(
            text("SELECT pg_database_size(current_database()) / 1024.0 / 1024.0")
        ).scalar()
    return float(value or 0.0)


def truncate_table(engine: sqlalchemy.Engine, table_name: str, reason: str) -> bool:
    if not table_exists(engine, table_name):
        return False
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
    print(f"[STORAGE] Truncated {table_name}: {reason}")
    return True


def table_date_counts(engine: sqlalchemy.Engine, table_name: str) -> Tuple[int, int]:
    """Return (all rows, today's rows) for a text/timestamp-based table."""
    if not table_exists(engine, table_name):
        return 0, 0
    today = now_ist().date()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f'SELECT COUNT(*) AS total, '
                f'COUNT(*) FILTER (WHERE CAST("timestamp" AS TIMESTAMP)::date = :today) AS today '
                f'FROM "{table_name}"'
            ),
            {"today": today},
        ).mappings().one()
    return int(row["total"] or 0), int(row["today"] or 0)


def daily_reset_option_history(engine: sqlalchemy.Engine) -> int:
    """
    Keep current-day option history only.

    When a new trading day starts and the table contains no rows for today,
    TRUNCATE is used instead of DELETE. TRUNCATE immediately releases the old
    relation pages and is important on Railway's 500 MB volume.
    """
    if not table_exists(engine, SYMBOL):
        return 0
    total_rows, today_rows = table_date_counts(engine, SYMBOL)
    if total_rows <= 0:
        return 0
    if today_rows == 0:
        truncate_table(engine, SYMBOL, "new trading day; previous option history removed")
        return total_rows

    with engine.begin() as conn:
        result = conn.execute(
            text(
                f'DELETE FROM "{SYMBOL}" '
                'WHERE CAST("timestamp" AS TIMESTAMP)::date < :today'
            ),
            {"today": now_ist().date()},
        )
    return int(result.rowcount or 0)


def delete_old_timestamp_rows(
    engine: sqlalchemy.Engine,
    table_name: str,
    retention_days: int,
) -> int:
    if not table_exists(engine, table_name):
        return 0
    cutoff = now_ist().date() - timedelta(days=max(0, retention_days - 1))
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f'DELETE FROM "{table_name}" '
                'WHERE CAST("timestamp" AS TIMESTAMP)::date < :cutoff'
            ),
            {"cutoff": cutoff},
        )
    return int(result.rowcount or 0)


def cleanup_cash_history(engine: sqlalchemy.Engine, retention_days: int) -> int:
    cutoff = now_ist().date() - timedelta(days=max(0, retention_days - 1))
    deleted = 0
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table_name in inspector.get_table_names():
            if not table_name.startswith("nifty50_"):
                continue
            columns = {column["name"] for column in inspector.get_columns(table_name)}
            try:
                if "trade_date" in columns:
                    result = conn.execute(
                        text(f'DELETE FROM "{table_name}" WHERE CAST("trade_date" AS date) < :cutoff'),
                        {"cutoff": cutoff},
                    )
                elif "timestamp" in columns:
                    result = conn.execute(
                        text(
                            f'DELETE FROM "{table_name}" '
                            'WHERE CAST("timestamp" AS TIMESTAMP)::date < :cutoff'
                        ),
                        {"cutoff": cutoff},
                    )
                else:
                    continue
                deleted += int(result.rowcount or 0)
            except Exception as exc:
                print(f"[RETENTION] cash table {table_name} warning: {type(exc).__name__}: {exc}")
    return deleted


def infer_option_step(frame: pd.DataFrame) -> float:
    strikes = sorted(
        {
            float(value)
            for value in frame.get("strikePrice", pd.Series(dtype=float)).dropna().tolist()
            if safe_number(value) is not None
        }
    )
    positive_diffs = [
        right - left
        for left, right in zip(strikes, strikes[1:])
        if right - left > 0
    ]
    if not positive_diffs:
        return 50.0
    return float(pd.Series(positive_diffs).median())


def compact_previous_option_snapshot(
    engine: sqlalchemy.Engine,
    current_timestamp: str,
    current_frame: pd.DataFrame,
) -> int:
    """
    Keep the newest snapshot as the complete two-expiry chain so all current
    dashboard OI-wall calculations remain accurate. Once a newer snapshot is
    stored, compact only the immediately previous snapshot to ATM +/- N strikes.

    This leaves current data complete while reducing historical growth from
    roughly 396 rows/minute to about 84 rows/minute for two expiries at +/-10.
    """
    if current_frame.empty or not table_exists(engine, SYMBOL):
        return 0
    spots = [safe_number(value) for value in current_frame.get("spotPrice", pd.Series(dtype=float)).tolist()]
    spots = [value for value in spots if value is not None and value > 0]
    if not spots:
        return 0
    spot = float(spots[0])
    step = infer_option_step(current_frame)
    atm = round(spot / step) * step
    low_strike = atm - (OPTION_HISTORY_ATM_STEPS * step)
    high_strike = atm + (OPTION_HISTORY_ATM_STEPS * step)

    with engine.begin() as conn:
        previous_ts = conn.execute(
            text(
                f'SELECT "timestamp" FROM "{SYMBOL}" '
                'WHERE CAST("timestamp" AS TIMESTAMP) < CAST(:current_ts AS TIMESTAMP) '
                'AND CAST("timestamp" AS TIMESTAMP)::date = CAST(:current_ts AS TIMESTAMP)::date '
                'ORDER BY CAST("timestamp" AS TIMESTAMP) DESC LIMIT 1'
            ),
            {"current_ts": current_timestamp},
        ).scalar()
        if not previous_ts:
            return 0
        result = conn.execute(
            text(
                f'DELETE FROM "{SYMBOL}" '
                'WHERE "timestamp" = :previous_ts '
                'AND (CAST("strikePrice" AS DOUBLE PRECISION) < :low_strike '
                'OR CAST("strikePrice" AS DOUBLE PRECISION) > :high_strike)'
            ),
            {
                "previous_ts": previous_ts,
                "low_strike": low_strike,
                "high_strike": high_strike,
            },
        )
    deleted = int(result.rowcount or 0)
    if deleted:
        print(
            f"[COMPACT] previous_ts={previous_ts} deleted={deleted} "
            f"kept_range={low_strike:.0f}-{high_strike:.0f} "
            f"ATM={atm:.0f} steps={OPTION_HISTORY_ATM_STEPS}"
        )
    return deleted


def vacuum_option_table_if_due(engine: sqlalchemy.Engine, force: bool = False) -> None:
    global _LAST_OPTION_VACUUM_MONOTONIC
    if not table_exists(engine, SYMBOL):
        return
    elapsed = time.monotonic() - _LAST_OPTION_VACUUM_MONOTONIC
    if not force and elapsed < OPTION_VACUUM_MINUTES * 60:
        return
    # VACUUM must run outside a transaction. It does not require extra rewrite
    # space and makes dead tuples reusable; VACUUM FULL is intentionally avoided.
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'VACUUM (ANALYZE) "{SYMBOL}"'))
        _LAST_OPTION_VACUUM_MONOTONIC = time.monotonic()
        print(f"[VACUUM] options.{SYMBOL} analyzed; dead space is reusable")
    except Exception as exc:
        print(f"[VACUUM] warning: {type(exc).__name__}: {exc}")


def emergency_option_reset(engine: sqlalchemy.Engine, size_before_mb: float) -> float:
    """
    Immediate pressure relief. DELETE does not reduce pg_database_size, whereas
    TRUNCATE releases the option relation files immediately. The current cycle
    then writes a fresh complete snapshot, so the dashboard continues working.
    """
    if table_exists(engine, SYMBOL):
        truncate_table(
            engine,
            SYMBOL,
            f"database {size_before_mb:.1f} MB reached purge threshold {DB_STORAGE_PURGE_MB} MB",
        )
    size_after = database_size_mb(engine)
    print(
        f"[STORAGE] emergency reset complete size_before={size_before_mb:.1f} MB "
        f"size_after={size_after:.1f} MB"
    )
    return size_after


def storage_guard_before_writes(force: bool = False) -> Tuple[bool, float]:
    global _LAST_STORAGE_CHECK_MONOTONIC, _LAST_DATABASE_SIZE_MB, _STORAGE_WRITES_PAUSED
    elapsed = time.monotonic() - _LAST_STORAGE_CHECK_MONOTONIC
    if not force and elapsed < DB_STORAGE_CHECK_MINUTES * 60:
        return (not _STORAGE_WRITES_PAUSED), _LAST_DATABASE_SIZE_MB

    size_mb = database_size_mb(option_engine())
    _LAST_STORAGE_CHECK_MONOTONIC = time.monotonic()
    _LAST_DATABASE_SIZE_MB = size_mb

    if size_mb >= DB_STORAGE_PURGE_MB:
        size_mb = emergency_option_reset(option_engine(), size_mb)
        _LAST_DATABASE_SIZE_MB = size_mb

    if size_mb >= DB_STORAGE_HARD_STOP_MB:
        _STORAGE_WRITES_PAUSED = True
        print(
            f"[STORAGE] CRITICAL database={size_mb:.1f} MB remains at/above hard-stop "
            f"{DB_STORAGE_HARD_STOP_MB} MB after option reset. All market-data writes "
            "are paused to protect PostgreSQL. Clear EOD/raw data or increase storage."
        )
        return False, size_mb

    if _STORAGE_WRITES_PAUSED:
        print(f"[STORAGE] RECOVERED database={size_mb:.1f} MB; writes resumed")
    _STORAGE_WRITES_PAUSED = False
    if size_mb >= DB_STORAGE_WARN_MB:
        print(
            f"[STORAGE] WARNING database={size_mb:.1f} MB warn={DB_STORAGE_WARN_MB} MB "
            f"purge={DB_STORAGE_PURGE_MB} MB hard_stop={DB_STORAGE_HARD_STOP_MB} MB"
        )
    else:
        print(
            f"[STORAGE] database={size_mb:.1f} MB warn={DB_STORAGE_WARN_MB} MB "
            f"purge={DB_STORAGE_PURGE_MB} MB hard_stop={DB_STORAGE_HARD_STOP_MB} MB"
        )
    return True, size_mb


def run_retention_cleanup(force: bool = False) -> Tuple[bool, float]:
    global _LAST_RETENTION_DATE
    today_key = now_ist().date().isoformat()
    daily_due = _LAST_RETENTION_DATE != today_key

    option_deleted = 0
    future_deleted = 0
    cash_deleted = 0
    if force or daily_due:
        option_deleted = daily_reset_option_history(option_engine())
        future_deleted = delete_old_timestamp_rows(future_engine(), SYMBOL, FUTURES_HISTORY_DAYS)
        if ENABLE_CASH:
            try:
                cash_deleted = cleanup_cash_history(cash_engine(), CASH_HISTORY_DAYS)
            except Exception as exc:
                print(f"[RETENTION] cash cleanup warning: {type(exc).__name__}: {exc}")
        _LAST_RETENTION_DATE = today_key

    allowed, size_mb = storage_guard_before_writes(force=force)
    print(
        f"[RETENTION] options_deleted={option_deleted} futures_deleted={future_deleted} "
        f"cash_deleted={cash_deleted} database_size={size_mb:.1f} MB writes_allowed={allowed}"
    )
    return allowed, size_mb


# -------------------- CASH MONEY FLOW --------------------
def load_nifty50_constituents(force: bool = False) -> pd.DataFrame:
    today_key = now_ist().date().isoformat()
    cached = _CONSTITUENT_CACHE.get("frame")
    if cached is not None and _CONSTITUENT_CACHE.get("date") == today_key and not force:
        return cached.copy()

    frame = pd.DataFrame()
    if CONSTITUENTS_URL:
        try:
            response = SESSION.get(
                CONSTITUENTS_URL,
                headers={"User-Agent": "Mozilla/5.0 NSE360/1.0"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            from io import StringIO

            frame = pd.read_csv(StringIO(response.text))
            print(f"[CASH] Downloaded {len(frame)} NIFTY 50 constituents from Nifty Indices.")
        except Exception as exc:
            print(f"[CASH] Constituent download failed: {type(exc).__name__}: {exc}")

    if frame.empty and FALLBACK_CONSTITUENTS.exists():
        frame = pd.read_csv(FALLBACK_CONSTITUENTS)
        print(f"[CASH] Using bundled constituent fallback with {len(frame)} rows.")

    required = {"Company Name", "Industry", "Symbol", "ISIN Code"}
    if frame.empty or not required.issubset(set(frame.columns)):
        raise RuntimeError(
            "NIFTY 50 constituent list unavailable or has unexpected columns. "
            f"Required: {sorted(required)}"
        )
    frame = frame.dropna(subset=["Symbol", "ISIN Code"]).copy()
    frame["Symbol"] = frame["Symbol"].astype(str).str.strip().str.upper()
    frame["ISIN Code"] = frame["ISIN Code"].astype(str).str.strip()
    frame["instrument_key"] = "NSE_EQ|" + frame["ISIN Code"]
    frame = frame.drop_duplicates(subset=["instrument_key"]).reset_index(drop=True)
    _CONSTITUENT_CACHE.update({"date": today_key, "frame": frame.copy()})
    return frame


def build_cash_rows(
    constituents: pd.DataFrame,
    quotes: Dict[str, Dict[str, Any]],
    ts: str,
    trade_date: str,
    engine: sqlalchemy.Engine,
) -> List[Dict[str, Any]]:
    previous = cash.get_previous_snapshot_map(engine, cash.DEFAULT_INDEX_NAME, trade_date, ts)
    count = max(1, len(constituents))
    equal_weight = 100.0 / count
    rows: List[Dict[str, Any]] = []

    for _, constituent in constituents.iterrows():
        instrument_key = str(constituent["instrument_key"])
        symbol = str(constituent["Symbol"])
        quote = find_quote(quotes, instrument_key)
        if not quote:
            continue
        previous_row = previous.get(symbol) or {}
        ohlc = quote.get("ohlc") or {}
        open_price = safe_number(ohlc.get("open"))
        high_price = safe_number(ohlc.get("high"))
        low_price = safe_number(ohlc.get("low"))
        last_price = safe_number(quote.get("last_price"))
        previous_close = safe_number(ohlc.get("close"))
        day_volume = safe_number(quote.get("volume"))
        previous_day_volume = safe_number(previous_row.get("day_volume"))
        previous_snapshot_price = safe_number(previous_row.get("last_price"))
        previous_average_price = safe_number(previous_row.get("typical_price"))
        previous_multiplier = safe_number(previous_row.get("money_flow_multiplier"))
        previous_signed_flow = safe_number(previous_row.get("signed_flow"))

        minute_volume = (
            0.0
            if day_volume is None or previous_day_volume is None
            else max(0.0, day_volume - previous_day_volume)
        )
        average_price = cash.cash_average_price(high_price, low_price, open_price, last_price)
        multiplier = cash.money_flow_multiplier(
            average_price,
            previous_average_price,
            open_price,
            last_price,
            previous_multiplier,
            previous_signed_flow,
        )
        gross_flow = minute_volume * average_price if average_price is not None else 0.0
        signed_flow = gross_flow * multiplier
        gross_flow_cr = gross_flow / 10_000_000.0
        signed_flow_cr = signed_flow / 10_000_000.0
        price_change = (
            last_price - previous_snapshot_price
            if last_price is not None and previous_snapshot_price is not None
            else (
                last_price - previous_close
                if last_price is not None and previous_close is not None
                else None
            )
        )
        weighted_signed_flow_cr = signed_flow_cr * (equal_weight / 100.0)
        rows.append(
            {
                "index_symbol": cash.DEFAULT_INDEX_NAME,
                "trade_date": trade_date,
                "timestamp": ts,
                "symbol": symbol,
                "company_name": str(constituent.get("Company Name") or ""),
                "industry": str(constituent.get("Industry") or ""),
                "series": str(constituent.get("Series") or "EQ"),
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "last_price": last_price,
                "previous_close": previous_close,
                "previous_snapshot_price": previous_snapshot_price,
                "price_change": price_change,
                "price_change_pct": pct_change(last_price, previous_snapshot_price or previous_close),
                "day_volume": day_volume,
                "previous_day_volume": previous_day_volume,
                "minute_volume": minute_volume,
                "day_value_raw": (
                    day_volume * safe_number(quote.get("average_price"))
                    if day_volume is not None and safe_number(quote.get("average_price")) is not None
                    else None
                ),
                "typical_price": average_price,
                "money_flow_multiplier": multiplier,
                "gross_flow": gross_flow,
                "signed_flow": signed_flow,
                "gross_flow_cr": gross_flow_cr,
                "signed_flow_cr": signed_flow_cr,
                "flow_direction": "Inflow" if signed_flow_cr > 0 else ("Outflow" if signed_flow_cr < 0 else "Neutral"),
                "free_float_mcap": None,
                "index_weight_pct": equal_weight,
                "weighted_signed_flow_cr": weighted_signed_flow_cr,
                "raw_json": json_dumps({"instrument_key": instrument_key, "quote": quote}),
                "index_weight_source": "equal_upstox",
            }
        )
    return rows


def run_cash_cycle(ts: str, trade_date: str) -> Optional[Dict[str, Any]]:
    if not ENABLE_CASH:
        return None
    engine = cash_engine()
    constituents = load_nifty50_constituents()
    quotes = fetch_full_quotes(constituents["instrument_key"].tolist())
    rows = build_cash_rows(constituents, quotes, ts, trade_date, engine)
    if not rows:
        raise RuntimeError("Upstox returned no usable NIFTY 50 constituent quotes.")
    summary = cash.build_summary(cash.DEFAULT_INDEX_NAME, trade_date, ts, rows)
    with engine.begin() as conn:
        cash.upsert_constituents(conn, rows, trade_date, ts, cash.DEFAULT_INDEX_NAME)
        stored = cash.upsert_flow_rows(conn, rows)
        cash.upsert_summary(conn, summary)
    print(
        f"[CASH] stored={stored} direction={summary['cash_direction']} "
        f"score={summary['final_cash_score']:.2f} net={summary['net_flow_cr']:.4f} Cr"
    )
    return summary


# -------------------- OPTIONAL SIGNAL CACHE --------------------
def refresh_option_buying_ai_cache(trade_date: str, expiry: Optional[str]) -> None:
    global _OPTION_BUYING_AI_MODULE
    if os.getenv("UPSTOX_DISABLE_OPTION_AI_CACHE", "0").strip().lower() in {"1", "true", "yes"}:
        return
    try:
        dashboard_path = PROJECT_DIR / "dashboard.py"
        if _OPTION_BUYING_AI_MODULE is None:
            spec = importlib.util.spec_from_file_location("nse360_dashboard_option_ai", dashboard_path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Could not load {dashboard_path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            _OPTION_BUYING_AI_MODULE = module
        payload = _OPTION_BUYING_AI_MODULE.build_option_buying_ai_payload(
            SYMBOL,
            trade_date=trade_date,
            expiry=expiry,
            band=20,
        )
        if payload.get("error"):
            print(f"[AI] Cache skipped: {payload.get('error')}")
        else:
            result = payload.get("optionBuyingResultWrite") or {}
            print(
                f"[AI] cache rows={result.get('storedCandidates', 0)} "
                f"payloadCached={result.get('payloadCached')}"
            )
    except Exception as exc:
        print(f"[AI] Cache refresh warning: {type(exc).__name__}: {exc}")


# -------------------- CYCLE --------------------
def run_cycle() -> Dict[str, Any]:
    cycle_time = minute_bucket(now_ist())
    ts = snapshot_timestamp(cycle_time)
    trade_date = cycle_time.date().isoformat()
    print(f"[RUN] Upstox NIFTY cycle @ {ts}")

    writes_allowed, _ = run_retention_cleanup()
    if not writes_allowed:
        return {
            "timestamp": ts,
            "trade_date": trade_date,
            "option_rows": 0,
            "future_rows": 0,
            "expiries": [],
            "cash": None,
            "storage_paused": True,
        }

    selected_expiries = discover_upcoming_option_expiries()
    print(f"[OPTIONS] selected expiries={selected_expiries}")

    chains: List[List[Dict[str, Any]]] = []
    actual_expiries: List[str] = []
    for expiry_date in selected_expiries:
        chain = fetch_option_chain(expiry_date)
        chains.append(chain)
        chain_expiries = sorted({str(item.get("expiry")) for item in chain if item.get("expiry")})
        actual_expiries.extend(chain_expiries or [expiry_date])
        print(f"[OPTIONS] expiry={expiry_date} strikes={len(chain)} response_expiries={chain_expiries}")

    options_df = build_options_dataframe(chains, ts)
    if options_df.empty:
        raise RuntimeError("Upstox option chain returned no CE/PE rows.")
    option_count = replace_option_snapshot(option_engine(), options_df)
    compacted = compact_previous_option_snapshot(option_engine(), ts, options_df)
    vacuum_option_table_if_due(option_engine())
    print(
        f"[OPTIONS] stored={option_count} contracts compacted={compacted} "
        f"expiries={sorted(options_df['expiryDate'].astype(str).unique().tolist())}"
    )

    contract = search_nearest_future_contract()
    quote_keys = [str(contract.get("instrument_key") or ""), UNDERLYING_KEY]
    quotes = fetch_full_quotes(quote_keys)
    future_quote = find_quote(quotes, quote_keys[0])
    spot_quote = find_quote(quotes, UNDERLYING_KEY)
    futures_df = build_futures_dataframe(contract, future_quote, spot_quote, ts, future_engine())
    future_count = replace_future_snapshot(future_engine(), futures_df)
    print(
        f"[FUTURES] stored={future_count} contract={contract.get('trading_symbol')} "
        f"expiry={contract.get('expiry')}"
    )

    cash_summary = None
    try:
        cash_summary = run_cash_cycle(ts, trade_date)
    except Exception as exc:
        print(f"[CASH] Warning: {type(exc).__name__}: {exc}")

    nearest_expiry = sorted(set(actual_expiries))[0] if actual_expiries else None
    refresh_option_buying_ai_cache(trade_date, nearest_expiry)
    return {
        "timestamp": ts,
        "trade_date": trade_date,
        "option_rows": option_count,
        "future_rows": future_count,
        "expiries": sorted(set(actual_expiries)),
        "cash": cash_summary,
    }


def startup_token_test() -> None:
    print("[UPSTOX] Validating Analytics Token and active NIFTY option expiries...")
    selected = discover_upcoming_option_expiries(force=True)
    chain = fetch_option_chain(selected[0])
    expiries = sorted({str(item.get("expiry")) for item in chain if item.get("expiry")})
    print(
        f"[UPSTOX] Token valid. selected_expiries={selected} "
        f"first_chain_strikes={len(chain)} response_expiries={expiries}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="NSE360 Upstox Analytics live worker")
    parser.add_argument("--once", action="store_true", help="Run one collection cycle and exit")
    parser.add_argument("--test-token", action="store_true", help="Validate token/API access and exit")
    args = parser.parse_args()

    require_token()
    # Initialize schemas before the first request so configuration errors are visible immediately.
    option_engine()
    future_engine()
    if ENABLE_CASH:
        cash_engine()
    run_retention_cleanup(force=True)

    print(
        f"[READY] NSE360 Upstox worker started. source=UPSTOX_ANALYTICS "
        f"underlying={UNDERLYING_KEY} expiry_count={OPTION_EXPIRY_COUNT} "
        f"option_history_days={OPTION_HISTORY_DAYS} history_atm_steps={OPTION_HISTORY_ATM_STEPS} "
        f"futures_history_days={FUTURES_HISTORY_DAYS} cash_history_days={CASH_HISTORY_DAYS} "
        f"storage_warn={DB_STORAGE_WARN_MB} storage_purge={DB_STORAGE_PURGE_MB} "
        f"storage_hard_stop={DB_STORAGE_HARD_STOP_MB} "
        f"market={MARKET_START.strftime('%H:%M')}-{MARKET_END.strftime('%H:%M')} IST"
    )
    startup_token_test()
    if args.test_token:
        return 0

    while True:
        moment = now_ist()
        if RUN_OUTSIDE_MARKET or market_is_open(moment) or args.once:
            try:
                result = run_cycle()
                if result.get("storage_paused"):
                    print(f"[DONE] {result['timestamp']} storage_paused=True; no writes performed")
                else:
                    print(
                        f"[DONE] {result['timestamp']} options={result['option_rows']} "
                        f"futures={result['future_rows']}"
                    )
            except Exception as exc:
                print(f"[ERROR] Cycle failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        else:
            print(
                f"[WAIT] Outside market window @ {moment.strftime('%d-%b-%Y %H:%M:%S')} IST; "
                "worker remains online."
            )

        if args.once:
            break
        time.sleep(seconds_to_next_cycle())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
