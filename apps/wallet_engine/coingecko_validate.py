from __future__ import annotations

import csv
import logging
import time
from typing import Iterable, List, Set

import requests

from apps.shared.config import (
    ASSET_PLATFORM_ID_BASE,
    COINGECKO_API_KEY,
    COINGECKO_BASE_URL,
    REQUEST_SLEEP_SEC,
    WALLET_ACTIVITY_CSV_PATH,
)
from apps.wallet_engine.contract_cache import (
    is_allowlisted,
    is_ignored,
    is_known_spam,
    mark_contract_spam_force,
)

try:
    from apps.shared.logging_utils import get_logger, setup_logging
except Exception:  # pragma: no cover - fallback when shared logging isn't available

    def setup_logging(level: str = "INFO") -> None:
        logging.basicConfig(
            level=getattr(logging, level.upper(), logging.INFO),
            format="%(asctime)s | %(levelname)-5s | %(message)s",
        )

    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)


log = get_logger("wallet_engine.coingecko_validate")


def _normalize_address(address: str) -> str:
    return (address or "").strip().lower()


def _iter_contract_addresses(rows: Iterable[dict]) -> Set[str]:
    addresses: Set[str] = set()
    for row in rows:
        addr = _normalize_address(row.get("contractAddress") or "")
        if addr:
            addresses.add(addr)
    return addresses


def _read_base_activity() -> List[dict]:
    with open(WALLET_ACTIVITY_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _coingecko_headers() -> dict:
    if COINGECKO_API_KEY:
        return {
            # "x-cg-pro-api-key": COINGECKO_API_KEY,
            "x-cg-demo-api-key": COINGECKO_API_KEY,
        }
    return {}


def _is_token_on_coingecko(
    session: requests.Session, address: str
) -> tuple[bool | None, str]:
    url = f"{COINGECKO_BASE_URL}/coins/{ASSET_PLATFORM_ID_BASE}/contract/{address}"
    headers = _coingecko_headers()
    max_retries = 6
    last_reason = "unknown"

    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=headers, timeout=20)
            if resp.status_code == 404:
                return False, "http_404"
            if resp.status_code in (429, 500, 502, 503, 504):
                last_reason = f"http_{resp.status_code}"
                time.sleep(REQUEST_SLEEP_SEC * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                err = (data.get("error") or "").strip().lower()
                if err == "coin not found":
                    return False, "coin_not_found"
                if data.get("id"):
                    return True, "found"
                return False, "missing_id"
            return False, "unexpected_payload"
        except requests.RequestException:
            last_reason = "request_error"
            time.sleep(REQUEST_SLEEP_SEC * (attempt + 1))

    return None, last_reason


def validate_addresses(addresses: Iterable[str]) -> set[str]:
    setup_logging("INFO")
    normalized = {_normalize_address(addr) for addr in addresses if _normalize_address(addr)}
    if not normalized:
        log.info("No contract addresses to validate.")
        return set()

    ordered = sorted(normalized)
    total = len(ordered)
    log.info("Validating %d contracts via Coingecko", total)

    session = requests.Session()
    min_interval_s = 2.1  # 30 requests/minute guardrail
    last_call = 0.0
    spam_found: set[str] = set()

    for i, addr in enumerate(ordered, start=1):
        if is_ignored(addr):
            log.info("[%d/%d] skipped %s reason=ignored", i, total, addr)
            continue
        if is_allowlisted(addr):
            log.info("[%d/%d] valid %s reason=allowlist", i, total, addr)
            continue
        if is_known_spam(addr):
            spam_found.add(addr)
            log.info("[%d/%d] bad %s reason=already_spam", i, total, addr)
            continue

        now = time.monotonic()
        delta = now - last_call
        if delta < min_interval_s:
            time.sleep(min_interval_s - delta)
        last_call = time.monotonic()

        log.info("[%d/%d] checking %s", i, total, addr)
        result, reason = _is_token_on_coingecko(session, addr)
        if result is None:
            log.warning(
                "[%d/%d] bad %s reason=%s (not saved)",
                i,
                total,
                addr,
                reason,
            )
            continue
        if result:
            log.info("[%d/%d] valid %s reason=%s", i, total, addr, reason)
            continue

        mark_contract_spam_force(addr)
        spam_found.add(addr)
        log.info("[%d/%d] bad %s reason=%s", i, total, addr, reason)

    return spam_found


def run() -> None:
    setup_logging("INFO")
    rows = _read_base_activity()
    addresses = _iter_contract_addresses(rows)
    if not addresses:
        log.info("No contractAddress values found in %s", WALLET_ACTIVITY_CSV_PATH)
        return
    validate_addresses(addresses)


if __name__ == "__main__":
    run()
