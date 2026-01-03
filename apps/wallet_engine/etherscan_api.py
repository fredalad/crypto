import time
from typing import List, Dict, Any

import requests

from .config import (
    ETHERSCAN_API_KEY,
    ETHERSCAN_API_URL,
    CHAIN_ID_BASE,
    require_api_key,
)


def etherscan_get(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generic helper for Etherscan V2 GET requests that return
    list-like data in 'result'.
    """
    require_api_key()

    base_params = {
        "apikey": ETHERSCAN_API_KEY,
        "chainid": CHAIN_ID_BASE,
    }
    all_params = {**base_params, **params}

    resp = requests.get(ETHERSCAN_API_URL, params=all_params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status")
    result = data.get("result")

    if status == "0":
        # Treat "No transactions found" as empty list
        if isinstance(result, str) and "No transactions" in result:
            return []
        raise RuntimeError(f"Etherscan API error: {data}")

    if status != "1":
        raise RuntimeError(f"Unexpected Etherscan response: {data}")

    if not isinstance(result, list):
        raise RuntimeError(f"Expected list in result, got: {type(result)}")

    return result


def fetch_base_native_txs(
    address: str,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
    page: int = 1,
    offset: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Fetch 'normal' (native) transactions on the Base network for this address.
    """
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "page": page,
        "offset": offset,
        "sort": sort,
    }
    return etherscan_get(params)


def fetch_all_base_native_txs(address: str) -> List[Dict[str, Any]]:
    all_txs: List[Dict[str, Any]] = []
    page = 1
    offset = 1000

    while True:
        print(f"[base native] Fetching page {page}...")
        txs = fetch_base_native_txs(address, page=page, offset=offset)

        if not txs:
            break

        all_txs.extend(txs)
        if len(txs) < offset:
            break

        page += 1
        time.sleep(0.2)

    print(f"[base native] Total fetched: {len(all_txs)}")
    return all_txs


def fetch_base_token_transfers(
    address: str,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
    page: int = 1,
    offset: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Fetch ERC-20 token transfers on the Base network for this address.
    """
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "page": page,
        "offset": offset,
        "sort": sort,
    }
    return etherscan_get(params)


def fetch_all_base_token_transfers(address: str) -> List[Dict[str, Any]]:
    all_txs: List[Dict[str, Any]] = []
    page = 1
    offset = 1000

    while True:
        print(f"[base tokens] Fetching page {page}...")
        txs = fetch_base_token_transfers(address, page=page, offset=offset)

        if not txs:
            break

        all_txs.extend(txs)
        if len(txs) < offset:
            break

        page += 1
        time.sleep(0.2)

    print(f"[base tokens] Total fetched: {len(all_txs)}")
    return all_txs


def fetch_tx_logs(tx_hash: str) -> List[Dict[str, Any]]:
    """
    Fetch logs for a specific transaction hash (Base chain) via proxy/getTransactionReceipt.
    Returns raw log entries (topics/data/address).
    """
    require_api_key()

    params = {
        "apikey": ETHERSCAN_API_KEY,
        "chainid": CHAIN_ID_BASE,
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": tx_hash,
    }
    resp = requests.get(ETHERSCAN_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Proxy style response
    if "result" not in data or data.get("result") in (None, "null"):
        raise RuntimeError(f"Etherscan proxy error for {tx_hash}: {data}")

    result = data["result"]
    logs = result.get("logs", []) or []

    # Normalize logs to a lightweight shape
    norm_logs: List[Dict[str, Any]] = []
    for log in logs:
        norm_logs.append(
            {
                "address": log.get("address", ""),
                "topics": log.get("topics", []),
                "data": log.get("data", ""),
                # event name not decoded here; topic[0] is used in classify via EVENT_SIGS
                "event": log.get("event", ""),
            }
        )

    return norm_logs


def fetch_base_nft_transfers(
    address: str,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
    page: int = 1,
    offset: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Fetch ERC-721 transfers on the Base network for this address.
    Needed to see Aerodrome v3 position NFTs (liquidity positions).
    """
    params = {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "page": page,
        "offset": offset,
        "sort": sort,
    }
    return etherscan_get(params)


def fetch_all_base_nft_transfers(address: str) -> List[Dict[str, Any]]:
    all_txs: List[Dict[str, Any]] = []
    page = 1
    offset = 1000

    while True:
        print(f"[base nfts] Fetching page {page}...")
        txs = fetch_base_nft_transfers(address, page=page, offset=offset)

        if not txs:
            break

        all_txs.extend(txs)
        if len(txs) < offset:
            break

        page += 1
        time.sleep(0.2)

    print(f"[base nfts] Total fetched: {len(all_txs)}")
    return all_txs
