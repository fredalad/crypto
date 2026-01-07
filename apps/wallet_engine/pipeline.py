from __future__ import annotations

from typing import Any, Dict, List

from apps.shared.config import (
    BASE_WALLET_ADDRESS,
    CHAIN_ID_BASE,
    ETHERSCAN_API_KEY,
    ETHERSCAN_API_URL,
    REQUEST_SLEEP_SEC,
    WALLET_ACTIVITY_CSV_PATH,
    WALLET_ACTIVITY_SPAM_CSV_PATH,
    require_api_key,
)
from apps.shared.csv_utils import write_csv_rows
from apps.shared.etherscan_v2 import EtherscanV2
from apps.shared.logging_utils import get_logger, setup_logging
from apps.wallet_engine.contract_cache import (
    get_spam_contracts,
    get_contract_metadata,
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


def _collect_contract_addresses(rows: List[Dict[str, Any]]) -> List[str]:
    addresses = set()
    for row in rows:
        tx_type = row.get("tx_type")
        if tx_type in ("token", "nft"):
            addr = (row.get("contractAddress") or "").strip()
            if addr:
                addresses.add(addr)
            continue
        if tx_type == "native":
            to_addr = (row.get("to") or "").strip()
            input_data = (row.get("input") or "").strip()
            if to_addr and input_data and input_data != "0x":
                addresses.add(to_addr)
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
    preexisting_spam = get_spam_contracts()
    clean_rows, spam_rows = _split_airdrops(rows)
    _mark_contracts(clean_rows, is_spam=False)
    _mark_contracts(spam_rows, is_spam=True)
    clean_rows = _filter_preexisting_spam(clean_rows, preexisting_spam)
    spam_rows = _filter_preexisting_spam(spam_rows, preexisting_spam)
    process_wallet_transactions(clean_rows)
    _cache_contract_metadata(clean_rows)
    log.info("Fetched %d transactions", len(rows))

    fieldnames = _fieldnames(rows)
    write_csv_rows(WALLET_ACTIVITY_CSV_PATH, clean_rows, fieldnames)
    write_csv_rows(WALLET_ACTIVITY_SPAM_CSV_PATH, spam_rows, fieldnames)
    log.info("Wrote spam CSV: %s", WALLET_ACTIVITY_SPAM_CSV_PATH)
    log.info("Wrote CSV: %s", WALLET_ACTIVITY_CSV_PATH)


def enrich() -> None:
    raise NotImplementedError("Wallet enrichment pipeline not implemented yet.")
