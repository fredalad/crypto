import os
from pathlib import Path
from typing import Set

from config import BASE_WALLET_ADDRESS
from etherscan_api import (
    fetch_all_base_native_txs,
    fetch_all_base_token_transfers,
)


OUTPUT_PATH = Path("csv/base_tx_ids.txt")


def extract_hash(tx: dict) -> str:
    """
    Return a transaction hash with defensive fallbacks.
    """
    return (tx.get("hash") or tx.get("transactionHash") or "").strip()


def collect_all_tx_ids(address: str) -> Set[str]:
    """
    Fetch native + token transfers and return a deduplicated set of hash|type lines.
    """
    native_txs = fetch_all_base_native_txs(address)
    token_txs = fetch_all_base_token_transfers(address)

    entries: Set[str] = set()

    for tx in native_txs + token_txs:
        h = extract_hash(tx)
        if h:
            # txreceipt_status only present on native list; tokentx includes tokenSymbol
            tx_type = "native" if tx in native_txs else "token_transfer"
            entries.add(f"{h.lower()}|{tx_type}")

    return entries


def main() -> None:
    address = (BASE_WALLET_ADDRESS or "").strip()
    if not address:
        raise RuntimeError(
            "Set BASE_WALLET_ADDRESS in .env to your Base wallet address (e.g. 0xabc...)"
        )

    print(f"Fetching all Base tx ids for address: {address}")
    tx_ids = collect_all_tx_ids(address)
    print(f"Total unique tx ids: {len(tx_ids)}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text("\n".join(sorted(tx_ids)), encoding="utf-8")
    print(f"Wrote tx ids to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
