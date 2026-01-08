from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

from apps.wallet_engine.contract_cache import CACHE_PATH, CONTRACTS_SPAM_PATH

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


log = get_logger("wallet_engine.dedupe_contracts")


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _save_json_atomic(path: str, data: Dict[str, Any]) -> None:
    tmp_path = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _normalize_address(address: str) -> str:
    return (address or "").strip().lower()


def run() -> None:
    setup_logging("INFO")
    valid = _load_json(CACHE_PATH)
    spam = _load_json(CONTRACTS_SPAM_PATH)

    valid_keys = {_normalize_address(k): k for k in valid.keys() if _normalize_address(k)}
    spam_keys = {_normalize_address(k) for k in spam.keys() if _normalize_address(k)}

    overlaps = [orig for norm, orig in valid_keys.items() if norm in spam_keys]
    if not overlaps:
        log.info("No overlaps found between valid and spam.")
        return

    for key in overlaps:
        valid.pop(key, None)

    _save_json_atomic(CACHE_PATH, valid)
    log.info("Removed %d overlapping contracts from contracts.json.", len(overlaps))


if __name__ == "__main__":
    run()
