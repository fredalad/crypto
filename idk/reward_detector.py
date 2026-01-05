from typing import Any, Dict, List


def _group_transfers_by_tx(transfers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for t in transfers or []:
        h = t.get("hash") or ""
        grouped.setdefault(h, []).append(t)
    return grouped


def detect_reward_claims(
    erc20_transfers: List[Dict[str, Any]],
    pools_metadata: Dict[str, Dict[str, Any]],
    gauges: List[str],
    wallet_address: str,
) -> List[Dict[str, Any]]:
    """
    Detect reward claims:
    - token transfer from gauge or pool to wallet
    - token is NOT an LP token (pool address)
    - no LP token transfers in the same tx (avoids LP burns/stakes)
    """
    wallet = wallet_address.lower()
    pools_set = set(pools_metadata.keys())
    gauge_set = {g.lower() for g in gauges}

    grouped = _group_transfers_by_tx(erc20_transfers)
    actions: List[Dict[str, Any]] = []

    for tx_hash, tx_transfers in grouped.items():
        # If any LP token transfer exists in this tx, skip reward detection for it.
        has_lp_transfer = any(
            (t.get("token_contract") or "").lower() in pools_set for t in tx_transfers
        )
        if has_lp_transfer:
            continue

        for t in tx_transfers:
            token_contract = (t.get("token_contract") or "").lower()
            from_addr = (t.get("from") or "").lower()
            to_addr = (t.get("to") or "").lower()
            amount = t.get("token_amount", 0)

            if to_addr != wallet:
                continue

            if from_addr not in pools_set and from_addr not in gauge_set:
                continue

            # Exclude LP tokens themselves
            if token_contract in pools_set:
                continue

            actions.append(
                {
                    "action": "reward_claim",
                    "token": token_contract,
                    "amount": amount,
                    "source_contract": from_addr,
                    "tx_hash": tx_hash,
                }
            )

    return actions
