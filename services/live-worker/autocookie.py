#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 23:26:45 2020

@author:
"""

import os
import requests
import time
from pathlib import Path
###############################################################################
BASE_DIR = os.getenv('APP_DATA_DIR', str(Path(__file__).resolve().parent / 'data'))
os.makedirs(BASE_DIR, exist_ok=True)
os.chdir(BASE_DIR)
###############################################################################
headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8'
            }
###############################################################################
def getCookies():
    """Refresh NSE session cookies into the worker data directory."""
    attempts = 0
    while attempts < 5:
        attempts += 1
        try:
            session = requests.Session()
            session.get('https://www.nseindia.com/', headers=headers, timeout=20)
            session.get(
                'https://www.nseindia.com/market-data/equity-derivatives-watch',
                headers=headers,
                timeout=20,
            )
            cookie_text = ';'.join(f"{cookie.name}={cookie.value}" for cookie in session.cookies)
            if not cookie_text:
                raise OSError('NSE returned no session cookies')
            cookie_path = os.path.join(BASE_DIR, 'cookies.txt')
            temp_path = cookie_path + '.tmp'
            with open(temp_path, 'w', encoding='utf-8') as handle:
                handle.write(cookie_text)
            os.replace(temp_path, cookie_path)
            print('Fresh cookies have been saved in cookies.txt.')
            return
        except (requests.RequestException, OSError) as exc:
            print(f'Cookie refresh attempt {attempts}/5 failed: {type(exc).__name__}: {exc}')
            if attempts >= 5:
                raise
            time.sleep(min(15, 3 * attempts))
###############################################################################
def main():
    getCookies()
###############################################################################
if __name__ == '__main__':
    main()
###############################################################################