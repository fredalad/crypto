from typing import Any, Dict, List, Optional

from eth_utils import to_checksum_address

SWAP_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


def _decode_uint256(hex_str: str) -> int:
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    return int(hex_str, 16)


def _decode_amounts(data_hex: str) -> Optional[Dict[str, int]]:
    # data: amount0In, amount1In, amount0Out, amount1Out each 32 bytes
    if not data_hex.startswith("0x"):
        return None
    hex_body = data_hex[2:]
    if len(hex_body) < 64 * 4:
        return None
    try:
        amount0_in = _decode_uint256(hex_body[0:64])
        amount1_in = _decode_uint256(hex_body[64:128])
        amount0_out = _decode_uint256(hex_body[128:192])
        amount1_out = _decode_uint256(hex_body[192:256])
    except Exception:
        return None
    return {
        "amount0_in": amount0_in,
        "amount1_in": amount1_in,
        "amount0_out": amount0_out,
        "amount1_out": amount1_out,
    }


def detect_swaps_from_logs(
    normalized_logs: List[Dict[str, Any]],
    pools_metadata: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    swaps: List[Dict[str, Any]] = []
    pools_set = set(pools_metadata.keys())

    for log in normalized_logs:
        addr = log.get("address") or ""
        if addr not in pools_set:
            continue

        topics = log.get("topics") or []
        topic0 = (topics[0] or "").lower() if topics else ""
        if topic0 != SWAP_TOPIC0:
            continue

        amounts = _decode_amounts(log.get("data", ""))
        if not amounts:
            continue

        meta = pools_metadata.get(addr, {})
        token0 = meta.get("token0", "")
        token1 = meta.get("token1", "")

        token_in = ""
        amount_in = 0
        token_out = ""
        amount_out = 0

        if amounts["amount0_in"] > 0:
            token_in = token0
            amount_in = amounts["amount0_in"]
        elif amounts["amount1_in"] > 0:
            token_in = token1
            amount_in = amounts["amount1_in"]

        if amounts["amount0_out"] > 0:
            token_out = token0
            amount_out = amounts["amount0_out"]
        elif amounts["amount1_out"] > 0:
            token_out = token1
            amount_out = amounts["amount1_out"]

        swaps.append(
            {
                "action": "swap",
                "pool": to_checksum_address(addr),
                "token_in": token_in,
                "amount_in": amount_in,
                "token_out": token_out,
                "amount_out": amount_out,
                "tx_hash": log.get("tx_hash", ""),
                "log_index": log.get("log_index", ""),
            }
        )

    return swaps
