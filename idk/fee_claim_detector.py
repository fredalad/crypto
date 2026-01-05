from typing import Any, Dict, List


def _group_transfers_by_tx(transfers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]] = {}
    for t in transfers or []:
        h = t.get("hash") or ""
        grouped.setdefault(h, []).append(t)
    return grouped


def detect_fee_claims(
    erc20_transfers: List[Dict[str, Any]],
    normalized_logs: List[Dict[str, Any]],
    pools_metadata: Dict[str, Dict[str, Any]],
    wallet_address: str,
) -> List[Dict[str, Any]]:
    """
    Detect LP fee claims:
    - Tokens transferred from pool to wallet
    - No Mint or Burn in same tx
    - No LP token movement in same tx
    """
    wallet = wallet_address.lower()
    pools_set = set(pools_metadata.keys())

    transfers_by_tx = _group_transfers_by_tx(erc20_transfers)
    logs_by_tx: Dict[str, List[Dict[str, Any]]] = {}
    for log in normalized_logs or []:
        h = log.get("tx_hash") or ""
        logs_by_tx.setdefault(h, []).append(log)

    actions: List[Dict[str, Any]] = []

    for tx_hash, tx_transfers in transfers_by_tx.items():
        # Skip if Mint or Burn logs present
        tx_logs = logs_by_tx.get(tx_hash, [])
        has_mint_or_burn = any(
            (l.get("topic0") or "").lower()
            in {
                "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9",  # Mint
                "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496",  # Burn
            }
            for l in tx_logs
        )
        if has_mint_or_burn:
            continue

        # Skip if any LP token movement in tx
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

            if from_addr not in pools_set:
                continue

            actions.append(
                {
                    "action": "fee_claim",
                    "token": token_contract,
                    "amount": amount,
                    "pool": from_addr,
                    "tx_hash": tx_hash,
                }
            )

    return actions
