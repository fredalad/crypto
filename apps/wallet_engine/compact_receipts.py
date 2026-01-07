from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

from apps.wallet_engine.receipt_cache import TX_INDEX_PATH, TX_RECEIPTS_PATH

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


log = get_logger("wallet_engine.compact_receipts")


def _normalize_hash(tx_hash: str) -> str:
    return (tx_hash or "").strip().lower()


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


def _extract_tx_hash(row: Dict[str, Any]) -> str:
    tx_hash = _normalize_hash(str(row.get("tx_hash") or ""))
    if tx_hash:
        return tx_hash
    receipt = row.get("receipt") if isinstance(row, dict) else None
    if isinstance(receipt, dict):
        return _normalize_hash(str(receipt.get("transactionHash") or ""))
    return ""


def compact() -> None:
    setup_logging("INFO")
    if not os.path.exists(TX_RECEIPTS_PATH):
        log.info("No receipts file at %s", TX_RECEIPTS_PATH)
        return

    out_path = f"{TX_RECEIPTS_PATH}.tmp"
    index: Dict[str, Dict[str, Any]] = {}
    kept = 0

    with open(TX_RECEIPTS_PATH, "r", encoding="utf-8") as src, open(
        out_path, "w", encoding="utf-8"
    ) as dst:
        for line in src:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            tx_hash = _extract_tx_hash(row)
            if not tx_hash:
                continue

            receipt = row.get("receipt")
            if isinstance(receipt, dict):
                minimized = _minimize_receipt(receipt)
                record = {
                    "tx_hash": tx_hash,
                    "block_timestamp": row.get("timestamp") or row.get("block_timestamp"),
                    "status": minimized.get("status"),
                    "logs": minimized.get("logs"),
                }
            else:
                record = {
                    "tx_hash": tx_hash,
                    "block_timestamp": row.get("block_timestamp") or row.get("timestamp"),
                    "status": row.get("status"),
                    "logs": row.get("logs") or [],
                }

            dst.write(json.dumps(record, ensure_ascii=True) + "\n")
            index[tx_hash] = {
                "line": kept,
                "block_timestamp": record.get("block_timestamp"),
            }
            kept += 1

        dst.flush()
        os.fsync(dst.fileno())

    os.replace(out_path, TX_RECEIPTS_PATH)
    with open(TX_INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())

    log.info("Compacted receipts: %d", kept)


if __name__ == "__main__":
    compact()
