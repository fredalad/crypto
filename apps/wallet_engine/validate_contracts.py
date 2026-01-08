from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

import requests

from apps.wallet_engine.coingecko_validate import _is_token_on_coingecko
from apps.wallet_engine.contract_cache import CACHE_PATH, mark_contract_spam_force

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


log = get_logger("wallet_engine.validate_contracts")


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _normalize_address(address: str) -> str:
    return (address or "").strip().lower()


def _load_valid_addresses() -> List[str]:
    data = _load_json(CACHE_PATH)
    if not data:
        return []
    addresses = {_normalize_address(addr) for addr in data.keys()}
    return sorted([addr for addr in addresses if addr])


def run() -> None:
    setup_logging("INFO")
    if not os.path.exists(CACHE_PATH):
        log.info("Missing contracts file: %s", CACHE_PATH)
        return

    addresses = _load_valid_addresses()
    if not addresses:
        log.info("No valid contract addresses found.")
        return

    log.info("Valid contracts total=%d", len(addresses))
    session = requests.Session()
    min_interval_s = 2.1  # 30 requests/minute guardrail
    last_call = 0.0
    total = len(addresses)

    for i, addr in enumerate(addresses, start=1):
        now = time.monotonic()
        delta = now - last_call
        if delta < min_interval_s:
            time.sleep(min_interval_s - delta)
        last_call = time.monotonic()

        log.info("[%d/%d] checking %s", i, total, addr)
        result, reason = _is_token_on_coingecko(session, addr)
        if result is None:
            log.warning("[%d/%d] error checking %s reason=%s", i, total, addr, reason)
            continue
        if result:
            log.info("[%d/%d] valid %s reason=%s", i, total, addr, reason)
            continue

        mark_contract_spam_force(addr)
        log.info("[%d/%d] bad %s reason=%s", i, total, addr, reason)


if __name__ == "__main__":
    run()
