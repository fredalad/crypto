from __future__ import annotations

import csv
import logging
import os
from typing import Any, Dict, List, Tuple

from apps.shared.config import WALLET_ACTIVITY_CSV_PATH, WALLET_ACTIVITY_SPAM_CSV_PATH
from apps.wallet_engine.contract_cache import (
    get_spam_contracts,
    is_known_valid,
    mark_contract_spam,
    mark_contract_valid,
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


log = get_logger("wallet_engine.split_airdrops")


def _split_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    clean_rows: List[Dict[str, Any]] = []
    spam_rows: List[Dict[str, Any]] = []
    for row in rows:
        function_name = (row.get("functionName") or "").strip().lower()
        if (
            "airdrop" in function_name
            or "dispersetoken" in function_name
            or "batchtransfer" in function_name
        ):
            spam_rows.append(row)
        else:
            clean_rows.append(row)
    return clean_rows, spam_rows


def _row_contract_address(row: Dict[str, Any]) -> str:
    tx_type = row.get("tx_type")
    if tx_type in ("token", "nft"):
        return (row.get("contractAddress") or "").strip()
    if tx_type == "native":
        to_addr = (row.get("to") or "").strip()
        input_data = (row.get("input") or "").strip()
        if to_addr and input_data and input_data != "0x":
            return to_addr
    return ""


def _filter_preexisting_spam(
    rows: List[Dict[str, Any]], preexisting_spam: set[str]
) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for row in rows:
        addr = _row_contract_address(row)
        if addr and addr.lower() in preexisting_spam and not is_known_valid(addr):
            continue
        kept.append(row)
    return kept


def _mark_contracts(rows: List[Dict[str, Any]], *, is_spam: bool) -> None:
    seen = set()
    for row in rows:
        addr = _row_contract_address(row)
        if not addr:
            continue
        addr_l = addr.lower()
        if addr_l in seen:
            continue
        seen.add(addr_l)
        if is_spam:
            mark_contract_spam(addr)
        else:
            mark_contract_valid(addr)


def _write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run() -> None:
    setup_logging("INFO")
    if not os.path.exists(WALLET_ACTIVITY_CSV_PATH):
        log.info("Missing CSV: %s", WALLET_ACTIVITY_CSV_PATH)
        return

    with open(WALLET_ACTIVITY_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    preexisting_spam = get_spam_contracts()
    clean_rows, spam_rows = _split_rows(rows)
    _mark_contracts(clean_rows, is_spam=False)
    _mark_contracts(spam_rows, is_spam=True)
    clean_rows = _filter_preexisting_spam(clean_rows, preexisting_spam)
    spam_rows = _filter_preexisting_spam(spam_rows, preexisting_spam)
    _write_csv(WALLET_ACTIVITY_CSV_PATH, clean_rows, fieldnames)
    _write_csv(WALLET_ACTIVITY_SPAM_CSV_PATH, spam_rows, fieldnames)

    log.info("Clean rows: %d", len(clean_rows))
    log.info("Spam rows: %d", len(spam_rows))
    log.info("Wrote CSV: %s", WALLET_ACTIVITY_CSV_PATH)
    log.info("Wrote spam CSV: %s", WALLET_ACTIVITY_SPAM_CSV_PATH)


if __name__ == "__main__":
    run()
