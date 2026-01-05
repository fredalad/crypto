import time
from typing import Any, Dict, List, Optional

from ..shared.config import (
    CHAIN_ID_BASE,
    ETHERSCAN_API_KEY,
    ETHERSCAN_V2_URL,
    require_api_key,
)
from ..shared.etherscan_v2 import EtherscanV2


def _fetch_all_pages(client: EtherscanV2, label: str, fetch_page, *, offset: int, sleep_s: float) -> List[Dict[str, Any]]:
    """
    Thin wrapper around client._fetch_all_pages to keep call-site simple.
    """
    return client._fetch_all_pages(label=label, fetch_page=fetch_page, offset=offset, sleep_s=sleep_s)


def _fetch_all_internal_txs(
    client: EtherscanV2,
    address: str,
    *,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
    offset: int = 1000,
    sleep_s: float = 0.2,
) -> List[Dict[str, Any]]:
    return _fetch_all_pages(
        client,
        label="base internal",
        fetch_page=lambda page, offset: client._get_list_result(
            {
                "module": "account",
                "action": "txlistinternal",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": sort,
            }
        ),
        offset=offset,
        sleep_s=sleep_s,
    )


def fetch_wallet_activity(
    address: str,
    *,
    start_block: int = 0,
    end_block: int = 9_999_999_999,
    sort: str = "asc",
    offset: int = 1000,
    sleep_s: float = 0.2,
    receipt_sleep_s: float = 0.1,
    client: Optional[EtherscanV2] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches all activity for a wallet on Base via Etherscan v2.
    Outputs one object per tx with raw responses preserved.
    """
    require_api_key()
    client = client or EtherscanV2(
        ETHERSCAN_API_KEY,
        CHAIN_ID_BASE,
        base_url=ETHERSCAN_V2_URL,
        timeout=(10.0, 30.0),
    )

    normal_txs = _fetch_all_pages(
        client,
        label="base normal",
        fetch_page=lambda page, offset: client.get_account_txs(
            address,
            start_block=start_block,
            end_block=end_block,
            sort=sort,
            page=page,
            offset=offset,
        ),
        offset=offset,
        sleep_s=sleep_s,
    )

    internal_txs = _fetch_all_internal_txs(
        client,
        address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        offset=offset,
        sleep_s=sleep_s,
    )

    erc20_transfers = _fetch_all_pages(
        client,
        label="base tokens",
        fetch_page=lambda page, offset: client.get_token_transfers(
            address,
            start_block=start_block,
            end_block=end_block,
            sort=sort,
            page=page,
            offset=offset,
        ),
        offset=offset,
        sleep_s=sleep_s,
    )

    # Index internals and erc20 by tx hash for quick merge
    internals_by_hash: Dict[str, List[Dict[str, Any]]] = {}
    for itx in internal_txs:
        h = itx.get("hash") or itx.get("transactionHash") or ""
        internals_by_hash.setdefault(h, []).append(itx)

    erc20_by_hash: Dict[str, List[Dict[str, Any]]] = {}
    for t in erc20_transfers:
        h = t.get("hash") or ""
        erc20_by_hash.setdefault(h, []).append(t)

    # Preserve order from normal_txs (already sorted by Etherscan)
    results: List[Dict[str, Any]] = []
    for tx in normal_txs:
        h = tx.get("hash") or ""
        block_number = tx.get("blockNumber", "")
        timestamp = tx.get("timeStamp", "")

        receipt = client.get_transaction_receipt(h)
        time.sleep(receipt_sleep_s)

        results.append(
            {
                "tx_hash": h,
                "block_number": block_number,
                "timestamp": timestamp,
                "normal_tx": tx,
                "internal_txs": internals_by_hash.get(h, []),
                "erc20_transfers": erc20_by_hash.get(h, []),
                "receipt": receipt,
            }
        )

    return results
