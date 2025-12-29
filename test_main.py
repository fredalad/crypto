import os
import requests
from eth_abi import decode
from dotenv import load_dotenv

load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
assert ETHERSCAN_API_KEY, "Missing ETHERSCAN_API_KEY"
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
SWAP_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V2_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V3_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def identify_tx(tx_hash: str) -> dict:
    """
    Identifies whether a tx is a swap by inspecting its logs.
    Etherscan-only. Deterministic. No heuristics.
    """

    # --------------------------------------------------
    # 1. Fetch transaction receipt
    # --------------------------------------------------
    params = {
        "chainid": 8453,
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": tx_hash,
        "apikey": ETHERSCAN_API_KEY,
    }

    r = requests.get(ETHERSCAN_API_URL, params=params)
    r.raise_for_status()

    receipt = r.json().get("result")
    if not receipt:
        return {
            "tx_hash": tx_hash,
            "type": "unknown",
            "reason": "receipt_not_found",
        }

    # --------------------------------------------------
    # 2. Scan logs for Swap event
    # --------------------------------------------------
    swap_logs = []

    for log in receipt.get("logs", []):
        topics = log.get("topics", [])
        if not topics:
            continue

        if any(topic.lower() in (SWAP_V2_TOPIC0, SWAP_V3_TOPIC0) for topic in topics):
            swap_logs.append(log)

    if not swap_logs:
        return {
            "tx_hash": tx_hash,
            "type": "not_swap",
            "reason": "no_swap_event",
        }

    # --------------------------------------------------
    # 3. Decode swaps (multi-hop supported)
    # --------------------------------------------------
    decoded_swaps = []

    for log in swap_logs:
        amount0_in, amount1_in, amount0_out, amount1_out = decode(
            ["uint256", "uint256", "uint256", "uint256"], bytes.fromhex(log["data"][2:])
        )

        decoded_swaps.append(
            {
                "pair_address": log["address"],
                "amount0_in": amount0_in,
                "amount1_in": amount1_in,
                "amount0_out": amount0_out,
                "amount1_out": amount1_out,
                "block_number": int(log["blockNumber"], 16),
            }
        )

    return {
        "tx_hash": tx_hash,
        "type": "swap",
        "swap_hops": len(decoded_swaps),
        "swaps": decoded_swaps,
    }


if __name__ == "__main__":
    tx = "0x15660d60d7493ad78f1bec695dce9232e970bfd7a45f856649c8ffc1b3a49c09"
    result = identify_tx(tx)

    print(result)
