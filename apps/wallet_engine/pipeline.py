from __future__ import annotations

import csv
import os
from typing import Any, Dict, List

from apps.shared.config import (
    BASE_WALLET_ADDRESS,
    CHAIN_ID_BASE,
    ETHERSCAN_API_KEY,
    ETHERSCAN_API_URL,
    REQUEST_SLEEP_SEC,
    WALLET_ACTIVITY_CONTRACTS_CSV_PATH,
    WALLET_ACTIVITY_CSV_PATH,
    WALLET_ACTIVITY_NO_CONTRACT_CSV_PATH,
    WALLET_ACTIVITY_SPAM_CSV_PATH,
    require_api_key,
)
from apps.shared.csv_utils import write_csv_rows
from apps.shared.etherscan_v2 import EtherscanV2
from apps.shared.logging_utils import get_logger, setup_logging
from apps.wallet_engine.coingecko_validate import validate_addresses
from apps.wallet_engine.contract_cache import (
    dedupe_valid_against_spam,
    get_spam_contracts,
    get_contract_metadata,
    is_allowlisted,
    is_ignored,
    is_known_valid,
    mark_contract_spam,
    mark_contract_valid,
)
from apps.wallet_engine.receipt_cache import process_wallet_transactions

log = get_logger("wallet_engine")


def _build_client() -> EtherscanV2:
    require_api_key()
    return EtherscanV2(
        api_key=ETHERSCAN_API_KEY,
        chainid=CHAIN_ID_BASE,
        base_url=ETHERSCAN_API_URL,
    )


def _resolve_wallet_address(address: str | None) -> str:
    resolved = (address or BASE_WALLET_ADDRESS or "").strip()
    if not resolved:
        raise RuntimeError("Missing wallet address. Set BASE_WALLET_ADDRESS in .env.")
    return resolved


def _fieldnames(rows: List[Dict[str, str]]) -> List[str]:
    preferred = [
        "hash",
        "from",
        "to",
        "value",
        "blockNumber",
        "timeStamp",
        "nonce",
        "gas",
        "gasPrice",
        "gasUsed",
        "isError",
        "txreceipt_status",
        "input",
    ]
    seen = set()
    out: List[str] = []
    for key in preferred:
        if any(key in row for row in rows):
            out.append(key)
            seen.add(key)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                out.append(key)
                seen.add(key)
    return out


def _split_airdrops(
    rows: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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


def _filter_ignored(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for row in rows:
        addr = _row_contract_address(row)
        if addr and is_ignored(addr):
            continue
        kept.append(row)
    return kept


def _move_preexisting_spam(
    clean_rows: List[Dict[str, Any]],
    spam_rows: List[Dict[str, Any]],
    preexisting_spam: set[str],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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


def _read_csv_rows(path: str) -> tuple[List[Dict[str, Any]], List[str]]:
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
    write_csv_rows(base_path, base_rows, out_fields)
    write_csv_rows(spam_path, kept_spam, out_fields)
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


def _move_nonspam_rows(
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
    write_csv_rows(base_path, base_rows, out_fields)
    write_csv_rows(spam_path, kept_spam, out_fields)
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


def _collect_contract_addresses(rows: List[Dict[str, Any]]) -> List[str]:
    addresses = set()
    for row in rows:
        tx_type = row.get("tx_type")
        if tx_type in ("token", "nft"):
            addr = (row.get("contractAddress") or "").strip()
            if addr and not is_ignored(addr):
                addresses.add(addr)
            continue
        if tx_type == "native":
            to_addr = (row.get("to") or "").strip()
            input_data = (row.get("input") or "").strip()
            if to_addr and input_data and input_data != "0x":
                if not is_ignored(to_addr):
                    addresses.add(to_addr)
    return sorted(addresses)


def _collect_token_contract_addresses(rows: List[Dict[str, Any]]) -> List[str]:
    addresses = set()
    for row in rows:
        if row.get("tx_type") != "token":
            continue
        addr = (row.get("contractAddress") or "").strip()
        if addr and not is_ignored(addr):
            addresses.add(addr)
    return sorted(addresses)


def _cache_contract_metadata(rows: List[Dict[str, Any]]) -> None:
    addresses = _collect_contract_addresses(rows)
    if not addresses:
        return
    log.info("Caching contract metadata for %d addresses", len(addresses))
    total = len(addresses)
    for i, address in enumerate(addresses, start=1):
        remaining = total - i
        log.info("[%d/%d] contracts remaining=%d", i, total, remaining)
        get_contract_metadata(address)


def fetch_wallet_transactions(
    address: str,
    *,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
) -> List[Dict[str, str]]:
    client = _build_client()
    native_txs = client.fetch_all_account_txs(
        address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        sleep_s=REQUEST_SLEEP_SEC,
    )
    token_txs = client.fetch_all_token_transfers(
        address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        sleep_s=REQUEST_SLEEP_SEC,
    )
    nft_txs = client.fetch_all_nft_transfers(
        address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        sleep_s=REQUEST_SLEEP_SEC,
    )

    combined: List[Dict[str, str]] = []
    for row in native_txs:
        row["tx_type"] = "native"
        combined.append(row)
    for row in token_txs:
        row["tx_type"] = "token"
        combined.append(row)
    for row in nft_txs:
        row["tx_type"] = "nft"
        combined.append(row)

    def _sort_key(r: Dict[str, str]) -> int:
        try:
            return int(r.get("timeStamp") or 0)
        except (TypeError, ValueError):
            return 0

    combined.sort(key=_sort_key)
    if sort == "desc":
        combined.reverse()
    return combined


def run_export(
    *,
    address: str | None = None,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
    log_level: str = "INFO",
) -> None:
    setup_logging(log_level)
    addr = _resolve_wallet_address(address)
    log.info("Fetching wallet transactions for %s", addr)
    rows = fetch_wallet_transactions(
        addr,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
    )
    rows = _filter_ignored(rows)
    clean_rows, spam_rows = _split_airdrops(rows)
    _mark_contracts(clean_rows, is_spam=False)
    _mark_contracts(spam_rows, is_spam=True)
    removed = dedupe_valid_against_spam()
    if removed:
        log.info("Removed %d overlapping contracts from contracts.json", removed)
    validate_addresses(_collect_token_contract_addresses(clean_rows))
    removed = dedupe_valid_against_spam()
    if removed:
        log.info("Removed %d overlapping contracts from contracts.json", removed)
    preexisting_spam = get_spam_contracts()
    clean_rows, spam_rows = _move_preexisting_spam(
        clean_rows, spam_rows, preexisting_spam
    )
    process_wallet_transactions(clean_rows)
    _cache_contract_metadata(clean_rows)
    log.info("Fetched %d transactions", len(rows))

    fieldnames = _fieldnames(rows)
    write_csv_rows(WALLET_ACTIVITY_CSV_PATH, clean_rows, fieldnames)
    write_csv_rows(WALLET_ACTIVITY_SPAM_CSV_PATH, spam_rows, fieldnames)
    moved = _merge_csv_hashes(
        WALLET_ACTIVITY_CSV_PATH,
        WALLET_ACTIVITY_SPAM_CSV_PATH,
        fieldnames,
    )
    if moved:
        log.info("Merged %d spam rows into base by hash", moved)
    moved = _move_nonspam_rows(
        WALLET_ACTIVITY_CSV_PATH,
        WALLET_ACTIVITY_SPAM_CSV_PATH,
        fieldnames,
    )
    if moved:
        log.info("Moved %d non-spam rows into base", moved)
    log.info("Wrote spam CSV: %s", WALLET_ACTIVITY_SPAM_CSV_PATH)
    log.info("Wrote CSV: %s", WALLET_ACTIVITY_CSV_PATH)


def enrich() -> None:
    raise NotImplementedError("Wallet enrichment pipeline not implemented yet.")


def split_base_activity_by_contract_address(
    *,
    log_level: str = "INFO",
) -> None:
    setup_logging(log_level)
    if not os.path.exists(WALLET_ACTIVITY_CSV_PATH):
        log.info("Missing CSV: %s", WALLET_ACTIVITY_CSV_PATH)
        return
    rows, fieldnames = _read_csv_rows(WALLET_ACTIVITY_CSV_PATH)
    if not rows:
        write_csv_rows(WALLET_ACTIVITY_CONTRACTS_CSV_PATH, [], fieldnames)
        write_csv_rows(WALLET_ACTIVITY_NO_CONTRACT_CSV_PATH, [], fieldnames)
        log.info("No rows found in %s", WALLET_ACTIVITY_CSV_PATH)
        return

    with_contract: List[Dict[str, Any]] = []
    without_contract: List[Dict[str, Any]] = []
    for row in rows:
        contract_addr = (row.get("contractAddress") or "").strip()
        if contract_addr:
            with_contract.append(row)
        else:
            without_contract.append(row)

    write_csv_rows(WALLET_ACTIVITY_CONTRACTS_CSV_PATH, with_contract, fieldnames)
    write_csv_rows(WALLET_ACTIVITY_NO_CONTRACT_CSV_PATH, without_contract, fieldnames)
    log.info("Contract rows: %d", len(with_contract))
    log.info("No-contract rows: %d", len(without_contract))
    log.info("Wrote CSV: %s", WALLET_ACTIVITY_CONTRACTS_CSV_PATH)
    log.info("Wrote CSV: %s", WALLET_ACTIVITY_NO_CONTRACT_CSV_PATH)
