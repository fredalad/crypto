from typing import Any, Dict, List

from eth_utils import to_checksum_address

MINT_TOPIC0 = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"


def _decode_uint256(hex_str: str) -> int:
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    return int(hex_str or "0", 16)


def _decode_mint_amounts(data_hex: str) -> Dict[str, int]:
    # data: amount0, amount1 (32 bytes each)
    if not data_hex.startswith("0x"):
        return {"amount0": 0, "amount1": 0}
    hex_body = data_hex[2:]
    if len(hex_body) < 64 * 2:
        return {"amount0": 0, "amount1": 0}
    amount0 = _decode_uint256(hex_body[0:64])
    amount1 = _decode_uint256(hex_body[64:128])
    return {"amount0": amount0, "amount1": amount1}


def _transfers_by_tx_hash(transfers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_hash: Dict[str, List[Dict[str, Any]]] = {}
    for t in transfers or []:
        h = t.get("hash") or ""
        by_hash.setdefault(h, []).append(t)
    return by_hash


def detect_lp_adds(
    normalized_logs: List[Dict[str, Any]],
    pools_metadata: Dict[str, Dict[str, Any]],
    wallet_address: str,
    erc20_transfers: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Detect LP adds based on Mint events + token transfers into pool + LP token mint to wallet.
    """
    wallet = wallet_address.lower()
    pools_set = set(pools_metadata.keys())
    transfers_by_hash = _transfers_by_tx_hash(erc20_transfers)

    actions: List[Dict[str, Any]] = []

    for log in normalized_logs:
        addr = log.get("address") or ""
        if addr not in pools_set:
            continue

        topic0 = (log.get("topic0") or "").lower()
        if topic0 != MINT_TOPIC0:
            continue

        tx_hash = log.get("tx_hash", "")
        data_hex = log.get("data", "")
        amounts = _decode_mint_amounts(data_hex)
        token0 = pools_metadata[addr].get("token0", "")
        token1 = pools_metadata[addr].get("token1", "")

        # Validate transfers: wallet -> pool for token0 and token1
        tx_transfers = transfers_by_hash.get(tx_hash, [])
        t0_in = next(
            (
                t
                for t in tx_transfers
                if (t.get("token_contract") or "").lower() == token0.lower()
                and (t.get("from") or "").lower() == wallet
                and (t.get("to") or "").lower() == addr
            ),
            None,
        )
        t1_in = next(
            (
                t
                for t in tx_transfers
                if (t.get("token_contract") or "").lower() == token1.lower()
                and (t.get("from") or "").lower() == wallet
                and (t.get("to") or "").lower() == addr
            ),
            None,
        )

        # LP tokens minted to wallet: pool token transfer to wallet
        lp_out = next(
            (
                t
                for t in tx_transfers
                if (t.get("token_contract") or "").lower() == addr
                and (t.get("to") or "").lower() == wallet
            ),
            None,
        )

        if not (t0_in and t1_in and lp_out):
            continue

        actions.append(
            {
                "action": "lp_add",
                "pool": to_checksum_address(addr),
                "token0_amount": amounts.get("amount0", 0),
                "token1_amount": amounts.get("amount1", 0),
                "lp_tokens_received": lp_out.get("token_amount", 0),
                "tx_hash": tx_hash,
                "log_index": log.get("log_index", ""),
            }
        )

    return actions
