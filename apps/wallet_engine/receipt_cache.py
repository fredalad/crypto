from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional

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


CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
TX_RECEIPTS_PATH = os.path.join(CACHE_DIR, "tx_receipts.jsonl")
TX_RECEIPTS_SPAM_PATH = os.path.join(CACHE_DIR, "tx_receipts_spam.jsonl")
TX_INDEX_PATH = os.path.join(CACHE_DIR, "tx_index.json")
TX_INDEX_SPAM_PATH = os.path.join(CACHE_DIR, "tx_index_spam.json")

log = get_logger("wallet_engine.receipt_cache")

_CLIENT: Optional[EtherscanV2] = None
_TX_INDEX: Optional[Dict[str, Dict[str, Any]]] = None
_SPAM_INDEX: Optional[Dict[str, Dict[str, Any]]] = None
_LINE_COUNT: Optional[int] = None
_SPAM_LINE_COUNT: Optional[int] = None


def _ensure_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        setup_logging("INFO")


def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _normalize_hash(tx_hash: str) -> str:
    return (tx_hash or "").strip().lower()


def _short_hash(tx_hash: str) -> str:
    h = (tx_hash or "").strip()
    if len(h) <= 12:
        return h or "<missing>"
    return f"{h[:8]}...{h[-4:]}"


def _minimize_logs(logs: Any) -> List[Dict[str, Any]]:
    if not isinstance(logs, list):
        return []
    out: List[Dict[str, Any]] = []
    for log_row in logs:
        if not isinstance(log_row, dict):
            continue
        out.append(
            {
                "address": log_row.get("address", ""),
                "topics": log_row.get("topics") or [],
                "data": log_row.get("data", ""),
                "logIndex": log_row.get("logIndex"),
            }
        )
    return out


def _minimize_receipt(receipt: Dict[str, Any]) -> Dict[str, Any]:
    status = receipt.get("status") or receipt.get("txreceipt_status")
    return {
        "status": status,
        "logs": _minimize_logs(receipt.get("logs") or []),
    }


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


def _append_jsonl(path: str, row: Dict[str, Any]) -> None:
    _ensure_cache_dir()
    line = json.dumps(row, ensure_ascii=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json_atomic(path: str, data: Any) -> None:
    _ensure_cache_dir()
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _rebuild_index_from_jsonl(jsonl_path: str) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(jsonl_path):
        return index
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            tx_hash = _normalize_hash(str(row.get("tx_hash") or ""))
            if not tx_hash:
                receipt = row.get("receipt") if isinstance(row, dict) else None
                if isinstance(receipt, dict):
                    tx_hash = _normalize_hash(str(receipt.get("transactionHash") or ""))
            if not tx_hash:
                continue
            index[tx_hash] = {
                "line": i,
                "block_timestamp": row.get("block_timestamp") or row.get("timestamp"),
            }
    return index


def _compute_line_count(index: Dict[str, Dict[str, Any]]) -> int:
    max_line = -1
    for value in index.values():
        line = value.get("line")
        if isinstance(line, int) and line > max_line:
            max_line = line
    return max_line + 1 if max_line >= 0 else len(index)


def load_tx_index() -> Dict[str, Dict[str, Any]]:
    global _TX_INDEX, _LINE_COUNT
    if _TX_INDEX is not None:
        return _TX_INDEX
    _ensure_cache_dir()

    index: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(TX_INDEX_PATH):
        data = _load_json(TX_INDEX_PATH)
        if isinstance(data, dict):
            for k, v in data.items():
                index[_normalize_hash(k)] = v if isinstance(v, dict) else {}
        elif isinstance(data, list):
            for i, h in enumerate(data):
                index[_normalize_hash(str(h))] = {"line": i}
    elif os.path.exists(TX_RECEIPTS_PATH):
        index = _rebuild_index_from_jsonl(TX_RECEIPTS_PATH)

    _TX_INDEX = index
    _LINE_COUNT = _compute_line_count(index)
    return _TX_INDEX


def load_spam_index() -> Dict[str, Dict[str, Any]]:
    global _SPAM_INDEX, _SPAM_LINE_COUNT
    if _SPAM_INDEX is not None:
        return _SPAM_INDEX
    _ensure_cache_dir()

    index: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(TX_INDEX_SPAM_PATH):
        data = _load_json(TX_INDEX_SPAM_PATH)
        if isinstance(data, dict):
            for k, v in data.items():
                index[_normalize_hash(k)] = v if isinstance(v, dict) else {}
        elif isinstance(data, list):
            for i, h in enumerate(data):
                index[_normalize_hash(str(h))] = {"line": i}
    elif os.path.exists(TX_RECEIPTS_SPAM_PATH):
        index = _rebuild_index_from_jsonl(TX_RECEIPTS_SPAM_PATH)

    _SPAM_INDEX = index
    _SPAM_LINE_COUNT = _compute_line_count(index)
    return _SPAM_INDEX


def _save_tx_index(index: Dict[str, Dict[str, Any]]) -> None:
    _save_json_atomic(TX_INDEX_PATH, index)


def has_tx(tx_hash: str) -> bool:
    key = _normalize_hash(tx_hash)
    if not key:
        return False
    if key in load_tx_index():
        return True
    return key in load_spam_index()


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_and_cache_tx(
    tx_hash: str, block_number: Any, timestamp: Any
) -> Dict[str, Any]:
    index = load_tx_index()
    key = _normalize_hash(tx_hash)
    if not key:
        raise ValueError("Missing tx_hash")

    cached = index.get(key)
    if cached:
        data = get_tx_data(tx_hash)
        if data is not None:
            return data
        return {"tx_hash": key, "block_timestamp": cached.get("block_timestamp")}

    if key in load_spam_index():
        return {"tx_hash": key, "spam": True}

    client = _get_client()
    receipt = client.get_transaction_receipt(tx_hash)
    minimal = _minimize_receipt(receipt)
    record = {
        "tx_hash": key,
        "block_timestamp": _parse_int(timestamp),
        "status": minimal.get("status"),
        "logs": minimal.get("logs"),
    }

    _append_jsonl(TX_RECEIPTS_PATH, record)

    global _LINE_COUNT
    if _LINE_COUNT is None:
        _LINE_COUNT = _compute_line_count(index)
    index[key] = {
        "line": _LINE_COUNT,
        "block_timestamp": record["block_timestamp"],
    }
    _LINE_COUNT += 1
    _save_tx_index(index)
    return record


def get_tx_data(tx_hash: str) -> Optional[Dict[str, Any]]:
    index = load_tx_index()
    key = _normalize_hash(tx_hash)
    if key in load_spam_index():
        return None
    entry = index.get(key)
    if not entry or not os.path.exists(TX_RECEIPTS_PATH):
        return None

    target_line = entry.get("line")
    with open(TX_RECEIPTS_PATH, "r", encoding="utf-8") as f:
        if isinstance(target_line, int):
            for i, line in enumerate(f):
                if i != target_line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    return None
                if _normalize_hash(str(row.get("tx_hash") or "")) == key:
                    return row
                return None
        else:
            for line in f:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if _normalize_hash(str(row.get("tx_hash") or "")) == key:
                    return row
    return None


def _iter_wallet_txs(wallet_txs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return list(wallet_txs)


def process_wallet_transactions(wallet_txs: Iterable[Dict[str, Any]]) -> None:
    _ensure_logging()
    _ensure_cache_dir()

    tx_list = _iter_wallet_txs(wallet_txs)
    total = len(tx_list)

    for i, tx in enumerate(tx_list, start=1):
        tx_hash = tx.get("hash") or tx.get("transactionHash") or ""
        block_number = tx.get("blockNumber")
        timestamp = tx.get("timeStamp")

        if has_tx(tx_hash):
            status = "cached"
        else:
            fetch_and_cache_tx(tx_hash, block_number, timestamp)
            status = "fetched"

        log.info("[%d/%d] %s %s", i, total, status, _short_hash(tx_hash))
