#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Best-effort NSE cookie/session warm-up for cloud workers.

Important: NSE does not always return a Set-Cookie header to data-centre IPs.
This helper therefore logs the warm-up result and returns an empty dict instead
of terminating the Railway worker. The live worker will continue retrying.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict

import requests

BASE_DIR = os.getenv("APP_DATA_DIR", str(Path(__file__).resolve().parent / "data"))
os.makedirs(BASE_DIR, exist_ok=True)
COOKIE_PATH = os.path.join(BASE_DIR, "cookies.txt")

HTML_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "accept-encoding": "gzip, deflate",
    "accept-language": "en-US,en;q=0.9,hi;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "connection": "keep-alive",
}

WARMUP_URLS = (
    "https://www.nseindia.com/",
    "https://www.nseindia.com/option-chain",
    "https://www.nseindia.com/market-data/equity-derivatives-watch",
)


def _write_cookie_file(cookies: Dict[str, str]) -> None:
    cookie_text = ";".join(f"{key}={value}" for key, value in cookies.items())
    temp_path = COOKIE_PATH + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        handle.write(cookie_text)
    os.replace(temp_path, COOKIE_PATH)


def getCookies(strict: bool = False) -> Dict[str, str]:
    """Warm an NSE session and persist cookies when NSE supplies them.

    By default this function never terminates the cloud worker merely because
    NSE returned no cookies. Pass strict=True only for an interactive test.
    """
    last_error: Exception | None = None

    for attempt in range(1, 6):
        session = requests.Session()
        try:
            for position, url in enumerate(WARMUP_URLS):
                request_headers = dict(HTML_HEADERS)
                if position:
                    request_headers["referer"] = WARMUP_URLS[position - 1]

                response = session.get(
                    url,
                    headers=request_headers,
                    timeout=25,
                    allow_redirects=True,
                )
                print(
                    f"[COOKIES] warm-up {position + 1}/{len(WARMUP_URLS)} "
                    f"status={response.status_code} bytes={len(response.content)} "
                    f"url={response.url}",
                    flush=True,
                )

                if response.status_code in {401, 403, 429}:
                    raise requests.HTTPError(
                        f"NSE warm-up blocked with HTTP {response.status_code}",
                        response=response,
                    )
                response.raise_for_status()
                time.sleep(0.8)

            cookies = session.cookies.get_dict()
            if cookies:
                _write_cookie_file(cookies)
                print(
                    f"[COOKIES] Saved {len(cookies)} NSE cookies to {COOKIE_PATH}.",
                    flush=True,
                )
                return cookies

            last_error = OSError("NSE warm-up succeeded but returned no session cookies")
            print(
                f"[COOKIES] Attempt {attempt}/5 returned no cookies; "
                "the worker will retry without exiting.",
                flush=True,
            )

        except (requests.RequestException, OSError) as exc:
            last_error = exc
            print(
                f"[COOKIES] Attempt {attempt}/5 failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

        if attempt < 5:
            time.sleep(min(15, 3 * attempt))

    if strict and last_error is not None:
        raise last_error

    print(
        "[COOKIES] WARNING: NSE cookies are unavailable. "
        "Returning control to the worker so it can retry later.",
        flush=True,
    )
    return {}


def main() -> int:
    cookies = getCookies(strict=False)
    return 0 if cookies else 1


if __name__ == "__main__":
    raise SystemExit(main())
