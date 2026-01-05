from typing import Any, Dict, List


def _group_transfers_by_tx(transfers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for t in transfers or []:
        h = t.get("hash") or ""
        grouped.setdefault(h, []).append(t)
    return grouped


def detect_veaero_actions(
    erc20_transfers: List[Dict[str, Any]],
    vote_logs: List[Dict[str, Any]],
    escrow_contracts: List[str],
    vote_contracts: List[str],
    aero_token: str,
    wallet_address: str,
) -> List[Dict[str, Any]]:
    """
    Detect veAERO interactions:
    - AERO -> escrow: lock_create / lock_increase
    - AERO <- escrow: lock_withdraw
    - Vote events from vote contracts: vote
    """
    wallet = wallet_address.lower()
    aero = aero_token.lower()
    escrow_set = {c.lower() for c in escrow_contracts}
    vote_set = {c.lower() for c in vote_contracts}

    grouped = _group_transfers_by_tx(erc20_transfers)
    actions: List[Dict[str, Any]] = []

    for tx_hash, tx_transfers in grouped.items():
        for t in tx_transfers:
            token = (t.get("token_contract") or "").lower()
            from_addr = (t.get("from") or "").lower()
            to_addr = (t.get("to") or "").lower()
            amt = t.get("token_amount", 0)

            if token != aero:
                continue

            if from_addr == wallet and to_addr in escrow_set:
                actions.append(
                    {
                        "action": "lock_create",
                        "amount": amt,
                        "contract": to_addr,
                        "tx_hash": tx_hash,
                    }
                )
            elif from_addr in escrow_set and to_addr == wallet:
                actions.append(
                    {
                        "action": "lock_withdraw",
                        "amount": amt,
                        "contract": from_addr,
                        "tx_hash": tx_hash,
                    }
                )

    # Votes based on logs
    for log in vote_logs or []:
        addr = (log.get("address") or "").lower()
        if addr in vote_set:
            actions.append(
                {
                    "action": "vote",
                    "amount": 0,
                    "contract": addr,
                    "tx_hash": log.get("tx_hash", ""),
                }
            )

    return actions
