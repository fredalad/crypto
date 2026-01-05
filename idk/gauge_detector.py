from typing import Any, Dict, List

def _transfers_by_tx_hash(transfers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_hash: Dict[str, List[Dict[str, Any]]] = {}
    for t in transfers or []:
        h = t.get("hash") or ""
        by_hash.setdefault(h, []).append(t)
    return by_hash


def detect_gauge_interactions(
    erc20_transfers: List[Dict[str, Any]],
    gauges: List[str],
    wallet_address: str,
) -> List[Dict[str, Any]]:
    """
    Detect lp_stake / lp_unstake based on LP token transfers to/from known gauges.
    """
    wallet = wallet_address.lower()
    gauge_set = {g.lower() for g in gauges}
    actions: List[Dict[str, Any]] = []

    for t in erc20_transfers or []:
        gauge = (t.get("to") or "").lower()
        from_addr = (t.get("from") or "").lower()
        token_addr = (t.get("token_contract") or "").lower()
        amount = t.get("token_amount", 0)
        tx_hash = t.get("hash", "")

        # lp_stake: wallet -> gauge
        if from_addr == wallet and gauge in gauge_set:
            actions.append(
                {
                    "action": "lp_stake",
                    "lp_token": token_addr,
                    "amount": amount,
                    "gauge": gauge,
                    "tx_hash": tx_hash,
                }
            )
            continue

        # lp_unstake: gauge -> wallet
        to_addr = gauge
        from_addr2 = (t.get("from") or "").lower()
        if from_addr2 in gauge_set and (t.get("to") or "").lower() == wallet:
            actions.append(
                {
                    "action": "lp_unstake",
                    "lp_token": token_addr,
                    "amount": amount,
                    "gauge": from_addr2,
                    "tx_hash": tx_hash,
                }
            )

    return actions
