from __future__ import annotations

import csv
import logging
import os
from typing import Any, Dict, List, Tuple

from apps.shared.config import WALLET_ACTIVITY_CSV_PATH, WALLET_ACTIVITY_SPAM_CSV_PATH
from apps.wallet_engine.contract_cache import (
    dedupe_valid_against_spam,
    get_spam_contracts,
    is_allowlisted,
    is_ignored,
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
        addr = _row_contract_address(row)
        if addr and is_ignored(addr):
            continue
        if row.get("tx_type") == "nft":
            clean_rows.append(row)
            continue
        function_name = (row.get("functionName") or "").strip().lower()
        if addr and is_allowlisted(addr):
            clean_rows.append(row)
            continue
        if _is_nonspam_function(function_name):
            clean_rows.append(row)
            continue
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


def _move_preexisting_spam(
    clean_rows: List[Dict[str, Any]],
    spam_rows: List[Dict[str, Any]],
    preexisting_spam: set[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    kept: List[Dict[str, Any]] = []
    moved: List[Dict[str, Any]] = []
    for row in clean_rows:
        if row.get("tx_type") == "nft":
            kept.append(row)
            continue
        addr = _row_contract_address(row)
        if addr and addr.lower() in preexisting_spam and not is_known_valid(addr):
            moved.append(row)
        else:
            kept.append(row)
    if moved:
        spam_rows = spam_rows + moved
    return kept, spam_rows


def _row_tx_hash(row: Dict[str, Any]) -> str:
    return (row.get("hash") or "").strip().lower()


def _read_csv_rows(path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, reader.fieldnames or []


def _merge_csv_hashes(
    base_path: str,
    spam_path: str,
    fieldnames: List[str],
) -> int:
    base_rows, base_fields = _read_csv_rows(base_path)
    spam_rows, spam_fields = _read_csv_rows(spam_path)
    if not base_rows or not spam_rows:
        return 0
    base_hashes = {_row_tx_hash(row) for row in base_rows if _row_tx_hash(row)}
    if not base_hashes:
        return 0
    kept_spam: List[Dict[str, Any]] = []
    moved: List[Dict[str, Any]] = []
    out_fields = fieldnames or base_fields or spam_fields
    base_sig = {
        tuple((row.get(field) or "") for field in out_fields) for row in base_rows
    }
    for row in spam_rows:
        tx_hash = _row_tx_hash(row)
        if tx_hash and tx_hash in base_hashes:
            signature = tuple((row.get(field) or "") for field in out_fields)
            if signature not in base_sig:
                moved.append(row)
                base_sig.add(signature)
        else:
            kept_spam.append(row)
    if not moved:
        return 0
    base_rows = base_rows + moved
    _write_csv(base_path, base_rows, out_fields)
    _write_csv(spam_path, kept_spam, out_fields)
    return len(moved)


def _is_nonspam_function(function_name: str) -> bool:
    lowered = (function_name or "").strip().lower()
    return any(
        keyword in lowered
        for keyword in (
            "approve",
            "withdraw",
            "getreward",
            "supply",
            "deposit",
            "repay",
            "claim",
        )
    )


def _move_approve_rows(
    base_path: str,
    spam_path: str,
    fieldnames: List[str],
) -> int:
    base_rows, base_fields = _read_csv_rows(base_path)
    spam_rows, spam_fields = _read_csv_rows(spam_path)
    if not spam_rows:
        return 0
    kept_spam: List[Dict[str, Any]] = []
    moved: List[Dict[str, Any]] = []
    for row in spam_rows:
        function_name = row.get("functionName") or ""
        if _is_nonspam_function(function_name):
            moved.append(row)
        else:
            kept_spam.append(row)
    if not moved:
        return 0
    out_fields = fieldnames or base_fields or spam_fields
    base_rows = base_rows + moved
    _write_csv(base_path, base_rows, out_fields)
    _write_csv(spam_path, kept_spam, out_fields)
    return len(moved)


def _mark_contracts(rows: List[Dict[str, Any]], *, is_spam: bool) -> None:
    seen = set()
    for row in rows:
        if is_spam and row.get("tx_type") == "nft":
            continue
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
    base_rows, base_fields = _read_csv_rows(WALLET_ACTIVITY_CSV_PATH)
    spam_rows_existing, spam_fields = _read_csv_rows(WALLET_ACTIVITY_SPAM_CSV_PATH)
    if not base_rows and not spam_rows_existing:
        log.info("Missing CSVs: %s, %s", WALLET_ACTIVITY_CSV_PATH, WALLET_ACTIVITY_SPAM_CSV_PATH)
        return
    fieldnames = base_fields or spam_fields
    rows = base_rows + spam_rows_existing

    preexisting_spam = get_spam_contracts()
    clean_rows, spam_rows = _split_rows(rows)
    _mark_contracts(clean_rows, is_spam=False)
    _mark_contracts(spam_rows, is_spam=True)
    removed = dedupe_valid_against_spam()
    if removed:
        log.info("Removed %d overlapping contracts from contracts.json", removed)
    clean_rows, spam_rows = _move_preexisting_spam(
        clean_rows, spam_rows, preexisting_spam
    )
    _write_csv(WALLET_ACTIVITY_CSV_PATH, clean_rows, fieldnames)
    _write_csv(WALLET_ACTIVITY_SPAM_CSV_PATH, spam_rows, fieldnames)
    moved = _merge_csv_hashes(
        WALLET_ACTIVITY_CSV_PATH,
        WALLET_ACTIVITY_SPAM_CSV_PATH,
        fieldnames,
    )
    if moved:
        log.info("Merged %d spam rows into base by hash", moved)
    moved = _move_approve_rows(
        WALLET_ACTIVITY_CSV_PATH,
        WALLET_ACTIVITY_SPAM_CSV_PATH,
        fieldnames,
    )
    if moved:
        log.info("Moved %d approve rows into base", moved)

    log.info("Clean rows: %d", len(clean_rows))
    log.info("Spam rows: %d", len(spam_rows))
    log.info("Wrote CSV: %s", WALLET_ACTIVITY_CSV_PATH)
    log.info("Wrote spam CSV: %s", WALLET_ACTIVITY_SPAM_CSV_PATH)


if __name__ == "__main__":
    run()
