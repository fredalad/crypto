from typing import Any, Dict, List


def build_timestamp_index(normalized_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a tx_hash -> timestamp mapping from normalized logs.
    """
    ts_map: Dict[str, Any] = {}
    for log in normalized_logs or []:
        h = log.get("tx_hash") or ""
        if not h or h in ts_map:
            continue
        ts_map[h] = log.get("timestamp", "")
    return ts_map


def _base_record(tx_hash: str, timestamp: Any, action: str, pool: str = "") -> Dict[str, Any]:
    return {
        "tx_hash": tx_hash,
        "timestamp": timestamp,
        "protocol": "aerodrome",
        "action": action,
        "token_in": "",
        "amount_in": 0,
        "token_out": "",
        "amount_out": 0,
        "pool": pool,
        "notes": "",
        "usd_value_at_time": None,
        "usd_value_now": None,
    }


def normalize_actions(
    actions: List[Dict[str, Any]],
    *,
    timestamp_index: Dict[str, Any],
    pool_metadata: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Normalize heterogeneous action dicts into the canonical schema.
    """
    normalized: List[Dict[str, Any]] = []

    for a in actions or []:
        tx_hash = a.get("tx_hash", "")
        ts = timestamp_index.get(tx_hash, "")
        action_type = a.get("action", "").lower()
        pool_addr = (a.get("pool") or "").lower()
        base = _base_record(tx_hash, ts, action_type, pool_addr)

        if action_type == "swap":
            base.update(
                {
                    "token_in": a.get("token_in", ""),
                    "amount_in": a.get("amount_in", 0),
                    "token_out": a.get("token_out", ""),
                    "amount_out": a.get("amount_out", 0),
                }
            )
        elif action_type == "lp_add":
            base["notes"] = "lp_add"
            base["token_in"] = pool_metadata.get(pool_addr, {}).get("token0", "")
            base["amount_in"] = a.get("token0_amount", 0)
            base["token_out"] = pool_metadata.get(pool_addr, {}).get("token1", "")
            base["amount_out"] = a.get("token1_amount", 0)
        elif action_type == "lp_remove":
            base["notes"] = "lp_remove"
            base["token_in"] = pool_metadata.get(pool_addr, {}).get("token0", "")
            base["amount_in"] = a.get("token0_amount", 0)
            base["token_out"] = pool_metadata.get(pool_addr, {}).get("token1", "")
            base["amount_out"] = a.get("token1_amount", 0)
        elif action_type in {"lp_stake", "lp_unstake"}:
            base["token_in"] = a.get("lp_token", "")
            base["amount_in"] = a.get("amount", 0)
            base["pool"] = a.get("gauge", pool_addr)
        elif action_type in {"reward_claim", "fee_claim"}:
            base["token_in"] = a.get("token", "")
            base["amount_in"] = a.get("amount", 0)
            base["pool"] = a.get("pool", a.get("source_contract", pool_addr))
        elif action_type in {"lock_create", "lock_increase", "lock_withdraw", "vote"}:
            base["token_in"] = ""
            base["amount_in"] = a.get("amount", 0)
            base["pool"] = a.get("contract", pool_addr)

        normalized.append(base)

    return normalized
