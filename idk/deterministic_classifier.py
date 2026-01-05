from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

from .registry import load_aerodrome_registry

# Topic0 -> action names (log-driven)
EVENT_SIGS = {
    # LP
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9": "LP_MINT",
    "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496": "LP_BURN",
    # Gauge
    "0xe1fffcc4923d02f16c1d26a8e22a87b9345c16a3c2e2d3b6a6d9d5f4f4f3c5b5": "GAUGE_DEPOSIT",
    "0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364": "GAUGE_WITHDRAW",
    "0x3d0c3c6f56c44c8c6e6d16fcbcb0bfbcae9f6c9e4b2a2d7bdebb7c7e7e7e7e7": "GAUGE_CLAIM",
    # Locks
    "0x4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f": "LOCK_REBASE",
    "0xaaaabbbbccccddddeeeeffff1111222233334444555566667777888899990000": "LOCK_CLAIM",
    # Claims (Aero bribes/fees/NFT claims)
    "0x9aa05b3d70a9e3e2f004f039648839560576334fb45c81f91b6db03ad9e2efc9": "CLAIM_LOCK_REWARDS",
    "0x1f89f96333d3133000ee447473151fa9606543368f02271c9d95ae14f13bcc67": "CLAIM_LOCK_REWARDS",
    "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": "CLAIM_LOCK_REWARDS",
    "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0": "CLAIM_LOCK_REWARDS",
    "0xf8e1a15aba9398e019f0b49df1a4fde98ee17ae345cb5f6b5e2c27f5033e8ce7": "CLAIM_LOCK_REWARDS",
    "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01": "CLAIM_LOCK_REWARDS",
}

# Swap topic signatures (same for v2/v3)
SWAP_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V3_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

def _log_topic0(log: Dict[str, Any]) -> str:
    topics = log.get("topics") or []
    return topics[0].lower() if topics else ""


def _is_swap(topics: List[str]) -> bool:
    lowered = [(t or "").lower() for t in topics]
    return SWAP_TOPIC0 in lowered or SWAP_V3_TOPIC0 in lowered


def _recognized_address(log_address: str, allowed: Set[str]) -> bool:
    return (log_address or "").lower() in allowed


def classify_logs(
    logs: List[Dict[str, Any]],
    registry: Dict[str, Set[str]],
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    allowed_addresses = registry["all"]
    pools = registry["pools"]
    gauges = registry["gauges"]
    lock_contracts = registry["lock_contracts"]
    lock_vote = registry["lock_vote"]
    vote_hints = registry["vote_hints"]

    for idx, log in enumerate(logs or []):
        addr = (log.get("address") or "").lower()
        if addr not in allowed_addresses:
            # Skip logs from unknown contracts to avoid inference.
            continue

        topics = log.get("topics") or []
        t0 = _log_topic0(log)

        # Swaps must come from pools we know.
        if _is_swap(topics) and addr in pools:
            actions.append(
                {
                    "tx_hash": log.get("tx_hash") or log.get("transactionHash") or log.get("hash") or "",
                    "action": "SWAP",
                    "log_index": log.get("logIndex", idx),
                    "log_address": addr,
                    "topics": topics,
                }
            )
            continue

        if t0 in EVENT_SIGS:
            action_name = EVENT_SIGS[t0]
            if action_name in {"LP_MINT", "LP_BURN"} and addr not in pools:
                continue
            if action_name.startswith("GAUGE_") and addr not in gauges:
                continue
            if action_name.startswith("LOCK_") and addr not in lock_contracts:
                continue
            if action_name.startswith("CLAIM_LOCK") and (
                addr not in lock_contracts and addr not in gauges
            ):
                continue

            actions.append(
                {
                    "tx_hash": log.get("tx_hash") or log.get("transactionHash") or log.get("hash") or "",
                    "action": action_name,
                    "log_index": log.get("logIndex", idx),
                    "log_address": addr,
                    "topics": topics,
                }
            )
            continue

        ev = (log.get("event") or "").lower()
        if ev == "vote" and addr in vote_hints:
            actions.append(
                {
                    "tx_hash": log.get("tx_hash") or log.get("transactionHash") or log.get("hash") or "",
                    "action": "VOTE",
                    "log_index": log.get("logIndex", idx),
                    "log_address": addr,
                    "topics": topics,
                }
            )

    return actions


def aggregate_assets(tx_rows: List[Dict[str, Any]]) -> Tuple[str, str]:
    """Build per-tx summaries of assets moving in/out (symbol:amount)."""
    in_assets = defaultdict(float)
    out_assets = defaultdict(float)

    for r in tx_rows:
        tx_type = r.get("tx_type")
        direction = r.get("direction")

        if tx_type == "base_token_transfer":
            amt = r.get("token_amount") or 0
            sym = r.get("token_symbol") or "UNKNOWN"
            if direction == "in":
                in_assets[sym] += float(amt)
            elif direction == "out":
                out_assets[sym] += float(amt)
        elif tx_type == "base_native":
            amt = r.get("native_amount_eth") or 0
            sym = "ETH"
            if direction == "in":
                in_assets[sym] += float(amt)
            elif direction == "out":
                out_assets[sym] += float(amt)

    def fmt(d):
        return (
            " | ".join(f"{sym}:{amt:.6g}" for sym, amt in sorted(d.items()))
            if d
            else ""
        )

    return fmt(in_assets), fmt(out_assets)


def normalize_receipt_logs(
    rows: List[Dict[str, Any]],
    logs_by_hash: Dict[str, List[Dict[str, Any]]],
    pool_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten receipt logs into a single normalized list.
    Each entry includes: tx_hash, log_index, address (lower), topic0, topics, data, block_number, timestamp.
    """
    tx_meta: Dict[str, Tuple[str, str]] = {}
    for r in rows:
        h = r.get("hash") or ""
        if not h:
            continue
        if h not in tx_meta:
            tx_meta[h] = (r.get("blockNumber", ""), r.get("timeStamp", ""))

    pool_metadata = pool_metadata or {}
    normalized: List[Dict[str, Any]] = []
    for h, logs in logs_by_hash.items():
        block_num, ts = tx_meta.get(h, ("", ""))
        for idx, log in enumerate(logs or []):
            topics = log.get("topics") or []
            topic0 = topics[0].lower() if topics else ""
            addr = (log.get("address") or "").lower()
            pool_meta = pool_metadata.get(addr, {})
            normalized.append(
                {
                    "tx_hash": h,
                    "log_index": log.get("logIndex", idx),
                    "address": addr,
                    "topic0": topic0,
                    "topics": topics,
                    "data": log.get("data", ""),
                    "block_number": block_num,
                    "timestamp": ts,
                    "pool_token0": pool_meta.get("token0", ""),
                    "pool_token1": pool_meta.get("token1", ""),
                    "pool_type": pool_meta.get("pool_type", ""),
                }
            )
    return normalized


def classify_transactions(
    rows: List[Dict[str, Any]],
    logs_by_hash: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    normalized_logs: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Deterministic classification:
    - Uses only known Aerodrome contract addresses and explicit log topics.
    - Multiple actions per transaction are preserved (semicolon-joined in activity_type).
    """
    logs_by_hash = logs_by_hash or {}
    registry = load_aerodrome_registry()

    if normalized_logs is None:
        normalized_logs = normalize_receipt_logs(
            rows, logs_by_hash, pool_metadata=registry.get("pool_metadata", {})
        )

    logs_by_tx: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for log in normalized_logs:
        logs_by_tx[log.get("tx_hash", "")].append(log)

    # Group rows by tx hash
    by_hash: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_hash[r.get("hash", "")].append(r)

    aerodrome_txs: Set[str] = set()
    for tx_hash, tx_rows in by_hash.items():
        tx_logs = logs_by_tx.get(tx_hash, [])
        actions = classify_logs(tx_logs, registry)
        ordered = sorted(actions, key=lambda x: x["log_index"])
        ordered_actions = [a["action"] for a in ordered]
        activity = ";".join(ordered_actions) if ordered_actions else "UNCLASSIFIED"

        # Flag Aerodrome-related tx if any log address is a known pool
        if any((log.get("address") or "").lower() in registry["pools"] for log in tx_logs):
            aerodrome_txs.add(tx_hash)

        # Assign to rows
        for r in tx_rows:
            r["activity_type"] = activity
            r["actions"] = ordered_actions
            r["aerodrome_related"] = tx_hash in aerodrome_txs
            r["actions_detail"] = ordered

        # Aggregate asset movement summaries per tx
        in_summary, out_summary = aggregate_assets(tx_rows)
        for r in tx_rows:
            r["token_in_assets"] = in_summary
            r["token_out_assets"] = out_summary
