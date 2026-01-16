from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from apps.shared.config import (
    CHAIN_ID_BASE,
    ETHERSCAN_API_KEY,
    ETHERSCAN_API_URL,
    require_api_key,
)
from apps.shared.etherscan_v2 import EtherscanV2

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


NAME_SELECTOR = "0x06fdde03"
SYMBOL_SELECTOR = "0x95d89b41"
DECIMALS_SELECTOR = "0x313ce567"

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
CACHE_PATH = os.path.join(CACHE_DIR, "contracts.json")
CONTRACTS_SPAM_PATH = os.path.join(CACHE_DIR, "contracts_spam.json")
LEGACY_DIR = os.path.join(CACHE_DIR, "contracts")

log = get_logger("wallet_engine.contract_cache")

ALWAYS_VALID_ADDRESSES = {
    "0x4e65fe4dba92790696d040ac24aa414708f5c0ab",
    "0x59dca05b6c26dbd64b5381374aaac5cd05644c28",
    "0xbdb9300b7cde636d9cd4aff00f6f009ffbbc8ee6",
    "0x24e6e0795b3c7c71d965fcc4f371803d1c1dca1e",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "0x827922686190790b37229fd06084350e74485b72",
    "0x1fba65de0a9cbd2d1df82d141897042d36bb6c86",
}
ALWAYS_IGNORE_ADDRESSES = {
    "0x6bc33580f41f7c40fd2dbadc03cc9cb986b2b80e",
}

_CLIENT: Optional[EtherscanV2] = None
_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
_SPAM_REGISTRY: Optional[Dict[str, Dict[str, Any]]] = None


def _ensure_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        setup_logging("INFO")


def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _normalize_address(address: str) -> str:
    return (address or "").strip().lower()


def is_allowlisted(address: str) -> bool:
    return _normalize_address(address) in ALWAYS_VALID_ADDRESSES


def is_ignored(address: str) -> bool:
    return _normalize_address(address) in ALWAYS_IGNORE_ADDRESSES


def _build_client() -> EtherscanV2:
    require_api_key()
    return EtherscanV2(
        api_key=ETHERSCAN_API_KEY,
        chainid=CHAIN_ID_BASE,
        base_url=ETHERSCAN_API_URL,
    )


def _get_client() -> EtherscanV2:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = _build_client()
    return _CLIENT


def _save_json_atomic(path: str, data: Dict[str, Any]) -> None:
    _ensure_cache_dir()
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _load_registry(path: str) -> Dict[str, Dict[str, Any]]:
    data = _load_json(path) if os.path.exists(path) else {}
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            addr = _normalize_address(key)
            if not addr:
                continue
            out[addr] = value if isinstance(value, dict) else {}
    return out


def _get_spam_registry() -> Dict[str, Dict[str, Any]]:
    global _SPAM_REGISTRY
    if _SPAM_REGISTRY is None:
        _SPAM_REGISTRY = _load_registry(CONTRACTS_SPAM_PATH)
    return _SPAM_REGISTRY


def _save_registry(path: str, data: Dict[str, Dict[str, Any]]) -> None:
    _save_json_atomic(path, data)


def _load_legacy_cache() -> Dict[str, Dict[str, Any]]:
    legacy: Dict[str, Dict[str, Any]] = {}
    if not os.path.isdir(LEGACY_DIR):
        return legacy
    for name in os.listdir(LEGACY_DIR):
        if not name.endswith(".json"):
            continue
        path = os.path.join(LEGACY_DIR, name)
        data = _load_json(path)
        address = _normalize_address(data.get("address") or name[:-5])
        if not address:
            continue
        legacy[address] = {
            "address": address,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "decimals": data.get("decimals"),
        }
    return legacy


def _load_cache() -> Dict[str, Dict[str, Any]]:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    _ensure_cache_dir()
    cache = _load_json(CACHE_PATH) if os.path.exists(CACHE_PATH) else {}

    legacy = _load_legacy_cache()
    if legacy:
        updated = False
        for address, data in legacy.items():
            if address not in cache:
                cache[address] = data
                updated = True
        if updated:
            _save_json_atomic(CACHE_PATH, cache)

    _CACHE = cache
    return _CACHE


def _get_cached_meta(address: str) -> Dict[str, Any]:
    cache = _load_cache()
    cached = cache.get(_normalize_address(address))
    if isinstance(cached, dict):
        return cached
    return _empty_meta(address)


def _remove_allowlisted_from_spam() -> int:
    spam = _get_spam_registry()
    if not spam:
        return 0
    removed = 0
    for addr in list(spam.keys()):
        if is_allowlisted(addr):
            spam.pop(addr, None)
            removed += 1
    if removed:
        _save_registry(CONTRACTS_SPAM_PATH, spam)
    return removed


def _remove_ignored_from_spam() -> int:
    spam = _get_spam_registry()
    if not spam:
        return 0
    removed = 0
    for addr in list(spam.keys()):
        if is_ignored(addr):
            spam.pop(addr, None)
            removed += 1
    if removed:
        _save_registry(CONTRACTS_SPAM_PATH, spam)
    return removed


def _remove_ignored_from_cache() -> int:
    cache = _load_cache()
    if not cache:
        return 0
    removed = 0
    for addr in list(cache.keys()):
        if is_ignored(addr):
            cache.pop(addr, None)
            removed += 1
    if removed:
        _save_json_atomic(CACHE_PATH, cache)
    return removed


def _empty_meta(address: str) -> Dict[str, Any]:
    return {
        "address": _normalize_address(address),
        "name": None,
        "symbol": None,
        "decimals": None,
    }


def _strip_hex_prefix(data: str) -> str:
    if not data:
        return ""
    return data[2:] if data.startswith("0x") else data


def _decode_uint256(data_hex: Optional[str]) -> Optional[int]:
    if not data_hex or data_hex == "0x":
        return None
    raw = _strip_hex_prefix(data_hex)
    if len(raw) < 64:
        return None
    try:
        data = bytes.fromhex(raw[:64])
    except ValueError:
        return None
    return int.from_bytes(data, "big")


def _decode_abi_string(data_hex: Optional[str]) -> Optional[str]:
    if not data_hex or data_hex == "0x":
        return None
    raw = _strip_hex_prefix(data_hex)
    try:
        data = bytes.fromhex(raw)
    except ValueError:
        return None
    if len(data) < 32:
        return None
    offset = int.from_bytes(data[:32], "big")
    if 0 <= offset + 32 <= len(data):
        length = int.from_bytes(data[offset : offset + 32], "big")
        start = offset + 32
        end = start + length
        if 0 <= length and end <= len(data):
            try:
                value = data[start:end].decode("utf-8")
            except UnicodeDecodeError:
                value = data[start:end].decode("utf-8", "ignore")
            value = value.strip("\x00")
            if value:
                return value
    value = data[:32].rstrip(b"\x00")
    if not value:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.decode("utf-8", "ignore") or None


def _safe_eth_call(address: str, data_hex: str) -> Optional[str]:
    try:
        return _get_client().eth_call(address, data_hex)
    except Exception:
        return None


def mark_contract_valid(address: str, meta: Optional[Dict[str, Any]] = None) -> None:
    addr = _normalize_address(address)
    if not addr:
        return
    if is_ignored(addr):
        _remove_ignored_from_cache()
        return
    if is_allowlisted(addr):
        _remove_allowlisted_from_spam()
    spam = _get_spam_registry()
    if addr in spam:
        return
    cache = _load_cache()
    if addr in cache:
        return
    record = meta if isinstance(meta, dict) else _get_cached_meta(addr)
    cache[addr] = record
    _save_json_atomic(CACHE_PATH, cache)


def mark_contract_spam(address: str, meta: Optional[Dict[str, Any]] = None) -> None:
    addr = _normalize_address(address)
    if not addr:
        return
    if is_ignored(addr):
        _remove_ignored_from_spam()
        _remove_ignored_from_cache()
        return
    if is_allowlisted(addr):
        _remove_allowlisted_from_spam()
        return
    spam = _get_spam_registry()
    if addr in spam:
        return
    cache = _load_cache()
    record = meta if isinstance(meta, dict) else cache.get(addr) or _empty_meta(addr)
    if addr in cache:
        cache.pop(addr, None)
        _save_json_atomic(CACHE_PATH, cache)
    spam[addr] = record
    _save_registry(CONTRACTS_SPAM_PATH, spam)


def mark_contract_spam_force(
    address: str, meta: Optional[Dict[str, Any]] = None
) -> None:
    addr = _normalize_address(address)
    if not addr:
        return
    if is_ignored(addr):
        _remove_ignored_from_spam()
        _remove_ignored_from_cache()
        return
    if is_allowlisted(addr):
        _remove_allowlisted_from_spam()
        return
    spam = _get_spam_registry()
    if addr in spam:
        return
    cache = _load_cache()
    record = meta if isinstance(meta, dict) else cache.get(addr) or _empty_meta(addr)
    if addr in cache:
        cache.pop(addr, None)
        _save_json_atomic(CACHE_PATH, cache)
    spam[addr] = record
    _save_registry(CONTRACTS_SPAM_PATH, spam)


def is_known_spam(address: str) -> bool:
    addr = _normalize_address(address)
    if not addr:
        return False
    if is_allowlisted(addr):
        return False
    if is_ignored(addr):
        return False
    return addr in _get_spam_registry()


def is_known_valid(address: str) -> bool:
    addr = _normalize_address(address)
    if not addr:
        return False
    if is_allowlisted(addr):
        return True
    if is_ignored(addr):
        return False
    if addr in _get_spam_registry():
        return False
    return addr in _load_cache()


def get_spam_contracts() -> set[str]:
    _remove_allowlisted_from_spam()
    _remove_ignored_from_spam()
    return set(_get_spam_registry().keys())


def get_valid_contracts() -> set[str]:
    spam = _get_spam_registry()
    return {addr for addr in _load_cache().keys() if addr not in spam}


def get_contract_metadata(address: str) -> Dict[str, Any]:
    _ensure_logging()
    addr = _normalize_address(address)
    if not addr:
        raise ValueError("Missing contract address")
    if is_ignored(addr):
        return _empty_meta(addr)
    if addr in _get_spam_registry() and not is_allowlisted(addr):
        return _empty_meta(addr)
    cache = _load_cache()
    cached = cache.get(addr)
    if isinstance(cached, dict):
        if (
            cached.get("name") is not None
            or cached.get("symbol") is not None
            or cached.get("decimals") is not None
        ):
            return cached

    name = _decode_abi_string(_safe_eth_call(addr, NAME_SELECTOR))
    symbol = _decode_abi_string(_safe_eth_call(addr, SYMBOL_SELECTOR))
    decimals = _decode_uint256(_safe_eth_call(addr, DECIMALS_SELECTOR))

    record = {
        "address": addr,
        "name": name,
        "symbol": symbol,
        "decimals": decimals,
    }
    cache[addr] = record
    _save_json_atomic(CACHE_PATH, cache)
    log.info("Fetched contract metadata for %s", addr)
    return record


def is_erc20(address: str) -> bool:
    meta = get_contract_metadata(address)
    return meta.get("decimals") is not None


def dedupe_valid_against_spam() -> int:
    cache = _load_cache()
    _remove_allowlisted_from_spam()
    _remove_ignored_from_spam()
    _remove_ignored_from_cache()
    spam = _get_spam_registry()
    overlaps = [addr for addr in cache.keys() if addr in spam]
    if not overlaps:
        return 0
    for addr in overlaps:
        cache.pop(addr, None)
    _save_json_atomic(CACHE_PATH, cache)
    return len(overlaps)
