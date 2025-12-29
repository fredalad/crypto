from collections import defaultdict
from typing import List, Dict, Any, DefaultDict, Optional

# Topic0 -> action names (log-driven)
EVENT_SIGS = {
    # LP
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9": "lp_mint",
    "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496": "lp_burn",
    # Gauge
    "0xe1fffcc4923d02f16c1d26a8e22a87b9345c16a3c2e2d3b6a6d9d5f4f4f3c5b5": "gauge_deposit",
    "0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364": "gauge_withdraw",
    "0x3d0c3c6f56c44c8c6e6d16fcbcb0bfbcae9f6c9e4b2a2d7bdebb7c7e7e7e7e7": "gauge_claim",
    # Locks
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": "erc20_approve",
    "0x4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f": "lock_rebase",
    "0xaaaabbbbccccddddeeeeffff1111222233334444555566667777888899990000": "lock_claim",
    # Claims (Aero bribes/fees/NFT claims)
    "0x9aa05b3d70a9e3e2f004f039648839560576334fb45c81f91b6db03ad9e2efc9": "claim_lock_rewards",
    "0x1f89f96333d3133000ee447473151fa9606543368f02271c9d95ae14f13bcc67": "claim_lock_rewards",
    "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": "claim_lock_rewards",
    "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0": "claim_lock_rewards",
    "0xf8e1a15aba9398e019f0b49df1a4fde98ee17ae345cb5f6b5e2c27f5033e8ce7": "claim_lock_rewards",
    "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01": "claim_lock_rewards",
}

# Swap topic signatures (same for v2/v3)
SWAP_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V3_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def aggregate_assets(tx_rows: List[Dict[str, Any]]) -> (str, str):
    """Build per-tx summaries of assets moving in/out (symbol:amount)."""
    from collections import defaultdict

    in_assets = defaultdict(float)
    out_assets = defaultdict(float)

    for r in tx_rows:
        tx_type = r["tx_type"]
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


def action_from_logs(logs: List[Dict[str, Any]]) -> str:
    """
    Returns a single canonical action based on logs.
    Prioritizes swaps, then claims/LP/gauge/lock/approval/vote/reset/rebase.
    """
    actions = set()
    for log in logs or []:
        topics = log.get("topics") or []
        topic0 = topics[0].lower() if topics else ""

        # Swap detection via topics (supports v2/v3)
        if topic0 in {SWAP_TOPIC0, SWAP_V3_TOPIC0} or any(
            (t or "").lower() in {SWAP_TOPIC0, SWAP_V3_TOPIC0} for t in topics
        ):
            actions.add("swap")
            continue

        if topic0 in EVENT_SIGS:
            actions.add(EVENT_SIGS[topic0])
            continue

        ev = (log.get("event") or "").lower()
        if ev == "approval":
            actions.add("approve")
        elif ev == "swap":
            actions.add("swap")
        elif ev == "mint":
            actions.add("lp_mint")
        elif ev == "burn":
            actions.add("lp_burn")
        elif ev == "deposit":
            actions.add("gauge_deposit")
        elif ev == "withdraw":
            actions.add("gauge_withdraw")
        elif ev == "claimrewards":
            actions.add("gauge_claim")
        elif ev == "createlock":
            actions.add("create_lock")
        elif ev == "increaseamount":
            actions.add("increase_lock_amount")
        elif ev == "increaseunlocktime":
            actions.add("increase_lock_duration")
        elif ev in {"claimfees", "claimbribes"}:
            actions.add("claim_lock_rewards")
        elif ev == "rebase":
            actions.add("lock_rebase")
        elif ev == "vote":
            actions.add("vote")
        elif ev == "reset":
            actions.add("reset")

    # Priority mapping
    if "swap" in actions:
        return "SWAP"
    if {"gauge_claim", "claim_lock_rewards", "lock_claim"} & actions:
        return "CLAIM_REWARD"
    if {"lp_mint", "gauge_deposit"} & actions:
        return "LP_DEPOSIT"
    if {"lp_burn", "gauge_withdraw"} & actions:
        return "LP_WITHDRAW"
    if {"increase_lock_amount", "create_lock", "increase_lock_duration"} & actions:
        return "LOCK_INCREASE"
    if "approve" in actions or "erc20_approve" in actions:
        return "APPROVAL"
    if "vote" in actions:
        return "VOTE"
    if "reset" in actions:
        return "RESET_VOTE"
    if "lock_rebase" in actions:
        return "LOCK_REBASE"
    return ""


def classify_transactions(
    rows: List[Dict[str, Any]],
    logs_by_hash: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> None:
    """
    In-place classification: sets 'activity_type' per row.

    Log-first classification:
    - Swap topics -> SWAP
    - Claim events -> CLAIM_REWARD
    - LP/Gauge events -> LP_DEPOSIT / LP_WITHDRAW
    - Lock increase/duration -> LOCK_INCREASE
    - Approvals -> APPROVAL
    - Vote/Reset -> VOTE / RESET_VOTE
    If no actionable logs -> NA
    """
    logs_by_hash = logs_by_hash or {}

    # Group rows by tx hash
    by_hash: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_hash[r.get("hash", "")].append(r)

    for tx_hash, tx_rows in by_hash.items():
        tx_logs = logs_by_hash.get(tx_hash, [])

        tx_level_activity = action_from_logs(tx_logs) or "NA"

        # Assign to token rows
        for r in tx_rows:
            r["activity_type"] = tx_level_activity

        # Aggregate asset movement summaries per tx
        in_summary, out_summary = aggregate_assets(tx_rows)
        for r in tx_rows:
            r["token_in_assets"] = in_summary
            r["token_out_assets"] = out_summary
