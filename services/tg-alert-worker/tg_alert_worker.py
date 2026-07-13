# -*- coding: utf-8 -*-
"""NSE360 Railway Telegram alert worker.

Reads the Upstox-normalised option snapshots already stored in
``options.\"NIFTY\"`` and sends Telegram alerts using the same core rules as
Harpal's local COI/Volume script:

* current + next expiry
* signed COI / volume threshold
* minimum volume and absolute COI filters
* ATM +/- N strike filter
* repeat cooldown and material-change re-alert
* Telegram 429 retry handling

All secrets and operational settings are environment variables. Alert state is
stored in PostgreSQL, so redeploying the Railway service does not resend the
same snapshot immediately.
"""
from __future__ import annotations

import argparse
import html
import math
import os
import time
from datetime import date, datetime, time as dtime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from psycopg2 import sql
import requests

IST = ZoneInfo("Asia/Kolkata")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SCHEMA_OPTIONS = os.getenv("SCHEMA_OPTIONS", "options").strip() or "options"
SYMBOL = os.getenv("TG_ALERT_SYMBOL", "NIFTY").strip().upper() or "NIFTY"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

THRESHOLD = float(os.getenv("TG_ALERT_THRESHOLD", "0.25"))
POLL_SECONDS = max(5, int(os.getenv("TG_ALERT_POLL_SECONDS", "60")))
REPEAT_MINUTES = max(0, int(os.getenv("TG_ALERT_REPEAT_MINUTES", "15")))
REALERT_DELTA = max(0.0, float(os.getenv("TG_ALERT_REALERT_DELTA", "0.05")))
MIN_VOLUME = max(0.0, float(os.getenv("TG_ALERT_MIN_VOLUME", "1000")))
MIN_ABS_COI = max(0.0, float(os.getenv("TG_ALERT_MIN_ABS_COI", "100")))
ATM_STEPS = int(os.getenv("TG_ALERT_ATM_STEPS", "10"))
INCLUDE_NEGATIVE = os.getenv("TG_ALERT_INCLUDE_NEGATIVE", "0").strip().lower() in {
    "1", "true", "yes", "y"
}
MARKET_START = dtime.fromisoformat(os.getenv("TG_ALERT_MARKET_START", "09:14"))
MARKET_END = dtime.fromisoformat(os.getenv("TG_ALERT_MARKET_END", "15:50"))
RUN_OUTSIDE_MARKET = os.getenv("TG_ALERT_RUN_OUTSIDE_MARKET", "0").strip().lower() in {
    "1", "true", "yes", "y"
}
SEND_STARTUP_MESSAGE = os.getenv("TG_ALERT_SEND_STARTUP_MESSAGE", "0").strip().lower() in {
    "1", "true", "yes", "y"
}
TELEGRAM_DELAY = max(0.1, float(os.getenv("TG_ALERT_TELEGRAM_DELAY", "1.0")))
TELEGRAM_MAX_RETRIES = max(0, int(os.getenv("TG_ALERT_TELEGRAM_MAX_RETRIES", "8")))

STATE_TABLE = "tg_coi_volume_alert_state"
_LAST_TELEGRAM_REQUEST_AT = 0.0


def now_ist() -> datetime:
    return datetime.now(IST)


def market_is_open(moment: Optional[datetime] = None) -> bool:
    moment = moment or now_ist()
    return moment.weekday() < 5 and MARKET_START <= moment.time() <= MARKET_END


def require_config() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID is missing")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(DATABASE_URL, connect_timeout=15)


def ensure_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS public.{} (
                    symbol text NOT NULL,
                    expiry date NOT NULL,
                    strike numeric NOT NULL,
                    side text NOT NULL,
                    last_snapshot text,
                    ratio numeric,
                    sent_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (symbol, expiry, strike, side)
                )
                """
            ).format(sql.Identifier(STATE_TABLE))
        )
    conn.commit()


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
    except (TypeError, ValueError):
        return None


def first_number(row: Dict[str, Any], keys: Sequence[str]) -> Optional[float]:
    for key in keys:
        value = safe_number(row.get(key))
        if value is not None:
            return value
    return None


def row_volume(row: Dict[str, Any]) -> Optional[float]:
    # Upstox adapter writes tradedVolume, tradedContracts and volume.
    # noOfTrades is retained as a fallback for older local/NSE-style tables.
    return first_number(row, ("tradedVolume", "volume", "tradedContracts", "noOfTrades"))


def parse_expiry(value: Any) -> Optional[date]:
    text_value = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text_value, pattern).date()
        except ValueError:
            continue
    return None


def parse_snapshot(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        result = value
    else:
        text_value = str(value or "").strip()
        result = None
        for pattern in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S"):
            try:
                result = datetime.strptime(text_value, pattern)
                break
            except ValueError:
                continue
        if result is None:
            return None
    return result.replace(tzinfo=IST) if result.tzinfo is None else result.astimezone(IST)


def roc(current: Any, previous: Any) -> Optional[float]:
    current_n = safe_number(current)
    previous_n = safe_number(previous)
    if current_n is None or previous_n in (None, 0):
        return None
    return ((current_n - previous_n) / abs(previous_n)) * 100.0


def timestamp_order_expression() -> sql.Composed:
    return sql.SQL(
        """
        CASE
          WHEN {ts} ~ '^\\d{{2}}-[A-Za-z]{{3}}-\\d{{4}} '
            THEN to_timestamp({ts}, 'DD-Mon-YYYY HH24:MI:SS')
          WHEN {ts} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}} '
            THEN to_timestamp({ts}, 'YYYY-MM-DD HH24:MI:SS')
          WHEN {ts} ~ '^\\d{{2}}-\\d{{2}}-\\d{{4}} '
            THEN to_timestamp({ts}, 'DD-MM-YYYY HH24:MI:SS')
          ELSE NULL
        END
        """
    ).format(ts=sql.Identifier("timestamp"))


def fetch_latest_two_snapshots(conn) -> List[Dict[str, Any]]:
    schema_id = sql.Identifier(SCHEMA_OPTIONS)
    table_id = sql.Identifier(SYMBOL)
    order_expr = timestamp_order_expression()

    latest_query = sql.SQL(
        """
        SELECT {ts}
        FROM {schema}.{table}
        WHERE UPPER(COALESCE({option_type}, '')) IN ('CE', 'PE')
        GROUP BY {ts}
        ORDER BY {order_expr} DESC NULLS LAST
        LIMIT 2
        """
    ).format(
        ts=sql.Identifier("timestamp"),
        schema=schema_id,
        table=table_id,
        option_type=sql.Identifier("optionType"),
        order_expr=order_expr,
    )

    with conn.cursor() as cur:
        try:
            cur.execute(latest_query)
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            return []
        timestamps = [row[0] for row in cur.fetchall() if row and row[0]]

    if not timestamps:
        return []

    rows_query = sql.SQL(
        """
        SELECT *
        FROM {schema}.{table}
        WHERE UPPER(COALESCE({option_type}, '')) IN ('CE', 'PE')
          AND {ts} = ANY(%s)
        """
    ).format(
        schema=schema_id,
        table=table_id,
        option_type=sql.Identifier("optionType"),
        ts=sql.Identifier("timestamp"),
    )
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(rows_query, (timestamps,))
        output = [dict(row) for row in cur.fetchall()]

    for row in output:
        row["snapshot_ts"] = parse_snapshot(row.get("timestamp"))
    return output


def select_expiries(rows: Iterable[Dict[str, Any]]) -> Tuple[List[date], Optional[datetime], Optional[datetime]]:
    timestamps = sorted(
        {row.get("snapshot_ts") for row in rows if row.get("snapshot_ts") is not None},
        reverse=True,
    )
    if not timestamps:
        return [], None, None
    latest_ts = timestamps[0]
    previous_ts = timestamps[1] if len(timestamps) > 1 else None
    expiries = sorted(
        {
            expiry
            for row in rows
            if row.get("snapshot_ts") == latest_ts
            for expiry in [parse_expiry(row.get("expiryDate"))]
            if expiry is not None and expiry >= latest_ts.date()
        }
    )[:2]
    return expiries, latest_ts, previous_ts


def build_qualified(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    expiries, latest_ts, previous_ts = select_expiries(rows)
    meta = {"latest": latest_ts, "previous": previous_ts, "expiries": expiries}
    if not expiries or latest_ts is None:
        return [], meta

    previous: Dict[Tuple[date, int, str], Dict[str, Any]] = {}
    if previous_ts is not None:
        for row in rows:
            if row.get("snapshot_ts") != previous_ts:
                continue
            expiry = parse_expiry(row.get("expiryDate"))
            strike = int(first_number(row, ("strikePrice",)) or 0)
            side = str(row.get("optionType") or "").upper()
            if expiry and strike and side in {"CE", "PE"}:
                previous[(expiry, strike, side)] = row

    output: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("snapshot_ts") != latest_ts:
            continue
        expiry = parse_expiry(row.get("expiryDate"))
        if expiry not in expiries:
            continue
        strike = int(first_number(row, ("strikePrice",)) or 0)
        side = str(row.get("optionType") or "").upper()
        coi = first_number(row, ("changeinOI",))
        volume = row_volume(row)
        if side not in {"CE", "PE"} or not strike or coi is None or volume in (None, 0):
            continue
        if volume < MIN_VOLUME or abs(coi) < MIN_ABS_COI:
            continue

        spot = first_number(row, ("spotPrice", "underlyingValue"))
        step = 50
        atm = round(spot / step) * step if spot is not None else None
        if ATM_STEPS >= 0 and atm is not None and abs(strike - atm) > ATM_STEPS * step:
            continue

        ratio = coi / volume
        qualifies = abs(ratio) >= THRESHOLD if INCLUDE_NEGATIVE else ratio >= THRESHOLD
        if not qualifies:
            continue

        old = previous.get((expiry, strike, side), {})
        ltp = first_number(row, ("lastPrice",))
        iv = first_number(row, ("impliedVolatility", "iv"))
        oi = first_number(row, ("openInterest",))
        old_volume = row_volume(old) if old else None
        output.append(
            {
                "symbol": SYMBOL,
                "timestamp": latest_ts,
                "previousTimestamp": previous_ts,
                "expiry": expiry,
                "expiryRank": expiries.index(expiry) + 1,
                "strike": strike,
                "side": side,
                "spot": spot,
                "ltp": ltp,
                "iv": iv,
                "oi": oi,
                "coi": coi,
                "volume": volume,
                "coiVol": ratio,
                "premiumRoc": roc(ltp, old.get("lastPrice")),
                "ivRoc": roc(iv, old.get("impliedVolatility") or old.get("iv")),
                "volumeRoc": roc(volume, old_volume),
                "oiRoc": roc(oi, old.get("openInterest")),
                "coiRoc": roc(coi, old.get("changeinOI")),
                "coiOiPct": (coi / oi * 100.0) if oi not in (None, 0) else None,
            }
        )
    output.sort(key=lambda item: abs(item["coiVol"]), reverse=True)
    return output, meta


def load_state(conn, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT last_snapshot, ratio, sent_at
                FROM public.{}
                WHERE symbol=%s AND expiry=%s AND strike=%s AND side=%s
                """
            ).format(sql.Identifier(STATE_TABLE)),
            (item["symbol"], item["expiry"], item["strike"], item["side"]),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def should_alert(item: Dict[str, Any], old: Optional[Dict[str, Any]]) -> bool:
    if not old:
        return True
    snapshot_text = item["timestamp"].strftime("%d-%b-%Y %H:%M:%S")
    if str(old.get("last_snapshot") or "") == snapshot_text:
        return False
    old_ratio = safe_number(old.get("ratio"))
    sent_at = old.get("sent_at")
    if isinstance(sent_at, datetime):
        sent_at = sent_at if sent_at.tzinfo else sent_at.replace(tzinfo=IST)
        elapsed_minutes = (now_ist() - sent_at.astimezone(IST)).total_seconds() / 60.0
    else:
        elapsed_minutes = None
    materially_changed = old_ratio is None or abs(item["coiVol"] - old_ratio) >= REALERT_DELTA
    cooldown_elapsed = elapsed_minutes is None or elapsed_minutes >= REPEAT_MINUTES
    return materially_changed or cooldown_elapsed


def save_state(conn, item: Dict[str, Any]) -> None:
    snapshot_text = item["timestamp"].strftime("%d-%b-%Y %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO public.{} 
                    (symbol, expiry, strike, side, last_snapshot, ratio, sent_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (symbol, expiry, strike, side)
                DO UPDATE SET
                    last_snapshot=EXCLUDED.last_snapshot,
                    ratio=EXCLUDED.ratio,
                    sent_at=now(),
                    updated_at=now()
                """
            ).format(sql.Identifier(STATE_TABLE)),
            (
                item["symbol"], item["expiry"], item["strike"], item["side"],
                snapshot_text, item["coiVol"],
            ),
        )
    conn.commit()


def fmt_num(value: Any, decimals: int = 0, signed: bool = False) -> str:
    number = safe_number(value)
    if number is None:
        return "NA"
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:,.{decimals}f}"


def fmt_pct(value: Any) -> str:
    return f"{fmt_num(value, 2, signed=True)}%"


def classify(item: Dict[str, Any]) -> str:
    side = item["side"]
    premium_roc = item.get("premiumRoc")
    iv_roc = item.get("ivRoc")
    if side == "CE":
        if premium_roc is not None and premium_roc > 0 and (iv_roc is None or iv_roc >= 0):
            return "CE long build-up / bullish pressure"
        if premium_roc is not None and premium_roc <= 0:
            return "CE writing / resistance watch"
        return "CE fresh OI concentration"
    if premium_roc is not None and premium_roc > 0 and (iv_roc is None or iv_roc >= 0):
        return "PE long build-up / bearish pressure"
    if premium_roc is not None and premium_roc <= 0:
        return "PE writing / support watch"
    return "PE fresh OI concentration"


def format_alert(item: Dict[str, Any]) -> str:
    rank = "CURRENT" if item["expiryRank"] == 1 else "NEXT"
    return (
        f'<b>{fmt_num(item["strike"])} {item["side"]} - HIGH COI/VOL ALERT</b>\n\n'
        f'<b>Time:</b> {item["timestamp"].strftime("%d-%b-%Y %H:%M:%S")} IST\n'
        f'<b>Expiry:</b> {item["expiry"].strftime("%d-%m-%Y")} ({rank})\n'
        f'<b>Spot / Strike:</b> {fmt_num(item["spot"], 2)} / {fmt_num(item["strike"])}\n'
        f'<b>LTP / IV:</b> {fmt_num(item["ltp"], 2)} / {fmt_num(item["iv"], 2)}\n\n'
        f'<b>COI/VOL:</b> {fmt_num(item["coiVol"], 4, signed=True)} (trigger {THRESHOLD:.2f})\n'
        f'<b>COI / Volume:</b> {fmt_num(item["coi"], 0, signed=True)} / {fmt_num(item["volume"])}\n'
        f'<b>OI:</b> {fmt_num(item["oi"])} | <b>COI/OI:</b> {fmt_pct(item["coiOiPct"])}\n\n'
        f'<b>Premium ROC:</b> {fmt_pct(item["premiumRoc"])}\n'
        f'<b>IV ROC:</b> {fmt_pct(item["ivRoc"])}\n'
        f'<b>Volume ROC:</b> {fmt_pct(item["volumeRoc"])}\n'
        f'<b>OI ROC / COI ROC:</b> {fmt_pct(item["oiRoc"])} / {fmt_pct(item["coiRoc"])}\n\n'
        f'<b>Read:</b> {html.escape(classify(item))}\n'
        f'<i>COI/VOL uses Upstox change in OI divided by cumulative option volume; no lot multiplier.</i>'
    )


def telegram_send(message: str, dry_run: bool = False) -> bool:
    global _LAST_TELEGRAM_REQUEST_AT
    if dry_run:
        print("\n----- TELEGRAM DRY RUN -----")
        print(message)
        print("----------------------------")
        return True
    require_config()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(TELEGRAM_MAX_RETRIES + 1):
        elapsed = time.monotonic() - _LAST_TELEGRAM_REQUEST_AT
        if elapsed < TELEGRAM_DELAY:
            time.sleep(TELEGRAM_DELAY - elapsed)
        try:
            response = requests.post(url, json=payload, timeout=25)
            _LAST_TELEGRAM_REQUEST_AT = time.monotonic()
        except requests.RequestException as exc:
            if attempt >= TELEGRAM_MAX_RETRIES:
                raise RuntimeError(f"Telegram network error: {exc}") from exc
            wait_seconds = min(60, 2 ** attempt)
            print(f"[TELEGRAM] Network error: {exc}; retrying in {wait_seconds}s")
            time.sleep(wait_seconds)
            continue
        if response.ok:
            return True
        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code == 429 and attempt < TELEGRAM_MAX_RETRIES:
            retry_after = safe_number((body.get("parameters") or {}).get("retry_after")) or 30
            wait_seconds = int(retry_after) + 1
            print(f"[TELEGRAM] Rate limited; retrying in {wait_seconds}s")
            time.sleep(wait_seconds)
            continue
        if 500 <= response.status_code < 600 and attempt < TELEGRAM_MAX_RETRIES:
            wait_seconds = min(60, 2 ** attempt)
            time.sleep(wait_seconds)
            continue
        raise RuntimeError(f"Telegram error {response.status_code}: {response.text[:700]}")
    raise RuntimeError("Telegram send failed after retry loop")


def run_scan(dry_run: bool = False) -> None:
    with get_conn() as conn:
        ensure_state_table(conn)
        rows = fetch_latest_two_snapshots(conn)
        qualified, meta = build_qualified(rows)
        sent = 0
        for item in qualified:
            old = load_state(conn, item)
            if not should_alert(item, old):
                continue
            telegram_send(format_alert(item), dry_run=dry_run)
            if not dry_run:
                save_state(conn, item)
            sent += 1
    expiry_text = ", ".join(expiry.strftime("%d-%m-%Y") for expiry in meta["expiries"]) or "none"
    print(
        f'[{now_ist().strftime("%Y-%m-%d %H:%M:%S")}] '
        f'latest={meta["latest"]} previous={meta["previous"]} '
        f'expiries={expiry_text} qualified={len(qualified)} sent={sent}',
        flush=True,
    )


def reset_state() -> None:
    with get_conn() as conn:
        ensure_state_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM public.{} WHERE symbol=%s").format(sql.Identifier(STATE_TABLE)),
                (SYMBOL,),
            )
        conn.commit()
    print(f"State reset for {SYMBOL}")


def main() -> int:
    parser = argparse.ArgumentParser(description="NSE360 Upstox COI/Volume Telegram alert worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test-telegram", action="store_true")
    parser.add_argument("--reset-state", action="store_true")
    args = parser.parse_args()

    require_config()
    with get_conn() as conn:
        ensure_state_table(conn)

    if args.reset_state:
        reset_state()
        return 0
    if args.test_telegram:
        telegram_send(
            "<b>NSE360 Telegram Test</b>\nUpstox COI/Volume alert worker is connected.",
            dry_run=args.dry_run,
        )
        print("Telegram test completed")
        return 0

    print(
        f"[READY] Telegram alert worker started | symbol={SYMBOL} | "
        f"threshold={THRESHOLD} | min_volume={MIN_VOLUME:g} | "
        f"min_abs_coi={MIN_ABS_COI:g} | ATM+/-{ATM_STEPS} | poll={POLL_SECONDS}s",
        flush=True,
    )
    if SEND_STARTUP_MESSAGE:
        telegram_send(
            "<b>NSE360 Telegram Alert Worker</b>\nConnected to Railway PostgreSQL and Telegram."
        )

    while True:
        try:
            if RUN_OUTSIDE_MARKET or market_is_open():
                run_scan(dry_run=args.dry_run)
            else:
                print(
                    f'[{now_ist().strftime("%Y-%m-%d %H:%M:%S")}] '
                    f'outside market window {MARKET_START.strftime("%H:%M")}-'
                    f'{MARKET_END.strftime("%H:%M")} IST; waiting',
                    flush=True,
                )
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            print(
                f'[{now_ist().strftime("%Y-%m-%d %H:%M:%S")}] '
                f'ERROR {type(exc).__name__}: {exc}',
                flush=True,
            )
        if args.once:
            return 0
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
