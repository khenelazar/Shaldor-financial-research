"""
Shaldor HTTP Helper
───────────────────
Shared HTTP functions using Python requests library.
Replaces curl subprocess calls for cloud compatibility (Streamlit Cloud, etc).

Used by sec_scraper.py and maya_scraper.py via monkey-patching in the orchestrator.
"""

import json
import time
import random
import os
import requests as _requests
from typing import Optional, Any

# ─── SEC EDGAR Configuration ────────────────────────────────────────────────

SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "CompanyResearch research@example.com"
)

SEC_HEADERS = {
    "Accept": "application/json",
    "User-Agent": SEC_USER_AGENT,
}

MAYA_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Rate limiting
_last_request_time = 0.0
REQUEST_DELAY = 0.12

# Retry config
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_JITTER = 0.3


def _rate_limit():
    """Enforce rate limiting between requests."""
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.monotonic()


def _retry_delay(attempt: int) -> float:
    """Calculate retry delay with exponential backoff + jitter."""
    base = RETRY_BASE_DELAY * (2 ** attempt)
    jitter = base * RETRY_JITTER * (2 * random.random() - 1)
    return max(0.1, base + jitter)


# ─── SEC Fetch ───────────────────────────────────────────────────────────────

def sec_fetch(url: str) -> Optional[dict]:
    """
    Fetch JSON from SEC EDGAR URL using requests with retry + backoff.
    Drop-in replacement for sec_scraper.fetch().
    """
    import sys
    last_error = ""
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limit()
            print(f"  [HTTP] GET {url[:80]}...", file=sys.stderr, flush=True)
            resp = _requests.get(url, headers=SEC_HEADERS, timeout=20)
            print(f"  [HTTP] Status: {resp.status_code}", file=sys.stderr, flush=True)

            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = f"HTTP {resp.status_code}"
                if attempt < MAX_RETRIES - 1:
                    delay = _retry_delay(attempt)
                    if resp.status_code == 429:
                        delay = max(delay, 2.0)
                    time.sleep(delay)
                    continue
                print(f"  ⚠ {last_error} for {url} after {MAX_RETRIES} tries", file=sys.stderr)
                return None

            if 400 <= resp.status_code < 500:
                print(f"  ⚠ HTTP {resp.status_code} for {url}", file=sys.stderr)
                return None

            if not resp.text.strip():
                print(f"  ⚠ Empty response from {url}", file=sys.stderr)
                return None

            return resp.json()

        except _requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            print(f"  ⚠ Timeout for {url} after {MAX_RETRIES} tries", file=sys.stderr)
            return None
        except _requests.exceptions.ConnectionError:
            last_error = "connection error"
            if attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            print(f"  ⚠ Connection error for {url} after {MAX_RETRIES} tries", file=sys.stderr)
            return None
        except json.JSONDecodeError:
            print(f"  ⚠ Invalid JSON from {url}", file=sys.stderr)
            return None

    return None


def sec_download_file(url: str, dest_path: str):
    """Download a file from SEC EDGAR. Returns (success, error)."""
    last_error = ""
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limit()
            resp = _requests.get(
                url,
                headers={"User-Agent": SEC_USER_AGENT},
                timeout=60,
                stream=True,
            )
            if resp.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True, None

            last_error = f"HTTP {resp.status_code}"
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            return False, last_error

        except (_requests.exceptions.Timeout, _requests.exceptions.ConnectionError) as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(attempt))
                continue
            return False, f"{last_error} after {MAX_RETRIES} attempts"

    return False, last_error


# ─── Maya Fetch ──────────────────────────────────────────────────────────────

def maya_fetch(url: str) -> dict:
    """
    Fetch JSON from Maya TASE URL using requests.
    Drop-in replacement for maya_scraper.fetch().
    Raises ConnectionError or ValueError on failure (same contract as original).
    """
    try:
        resp = _requests.get(url, headers=MAYA_HEADERS, timeout=15)
    except _requests.exceptions.RequestException as e:
        raise ConnectionError(f"HTTP request failed for {url}: {e}")

    if resp.status_code != 200:
        raise ConnectionError(
            f"HTTP request failed for {url} (status {resp.status_code})"
        )

    try:
        return resp.json()
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON from {url}: {resp.text[:200]}")


def maya_download_file(url: str, dest_path: str):
    """Download a file from Maya. Returns (success, error_message)."""
    try:
        resp = _requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=60,
            stream=True,
        )
        if resp.status_code == 200:
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True, None
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)


# ─── Monkey-patch function ───────────────────────────────────────────────────

_patched = False

def patch_scrapers():
    """
    Replace curl-based fetch/download functions in sec_scraper and maya_scraper
    with requests-based versions. Call this before using the scrapers.
    Only patches once — safe to call multiple times.
    """
    global _patched
    if _patched:
        return

    import sys
    import sec_scraper
    import maya_scraper

    sec_scraper.fetch = sec_fetch
    sec_scraper.download_file = sec_download_file
    sec_scraper._rate_limit = _rate_limit

    # Reset ticker cache if it was loaded empty by curl before patch
    if sec_scraper._ticker_cache is not None and len(sec_scraper._ticker_cache) == 0:
        sec_scraper._ticker_cache = None

    maya_scraper.fetch = maya_fetch
    maya_scraper.download_file = maya_download_file

    _patched = True
    print("[PATCH] Scrapers patched to use requests instead of curl", file=sys.stderr, flush=True)
