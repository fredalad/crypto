from collections import defaultdict
from typing import List, Dict, Any, DefaultDict, Optional

from helpers import is_lp_symbol
from config import (
    VOTE_CONTRACT_HINTS,
    APPROVAL_CONTRACT_HINTS,
    LOCK_CONTRACTS,
    LOCK_VOTE_CONTRACTS,
    VOTE_MIN_GASUSED,
)

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
}


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


def as_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def action_from_logs(logs: List[Dict[str, Any]]) -> str:
    """
    Returns a single canonical action based on logs.
    Note: voting is handled separately in classify_voting_from_logs().
    """
    for log in logs or []:
        topics = log.get("topics") or []
        topic0 = topics[0].lower() if topics else ""
        if topic0 in EVENT_SIGS:
            return EVENT_SIGS[topic0]

        ev = (log.get("event") or "").lower()

        # ERC20 + router/pair/gauge/lock basics
        if ev == "approval":
            return "approve"
        if ev == "swap":
            return "swap"
        if ev == "mint":
            return "lp_mint"
        if ev == "burn":
            return "lp_burn"
        if ev == "deposit":
            return "gauge_deposit"
        if ev == "withdraw":
            return "gauge_withdraw"
        if ev == "claimrewards":
            return "gauge_claim"
        if ev == "createlock":
            return "create_lock"
        if ev == "increaseamount":
            return "increase_lock_amount"
        if ev == "increaseunlocktime":
            return "increase_lock_duration"
        if ev in {"claimfees", "claimbribes"}:
            return "claim_lock_rewards"
        if ev == "rebase":
            return "lock_rebase"

    return ""


def classify_voting_from_logs(logs: List[Dict[str, Any]]) -> Optional[str]:
    """
    Voting classification (must run before approval heuristics):

    - If ClaimBribe / ClaimFees present => CLAIM_REWARD (income)
    - Else if Vote present => VOTE
    - Else if Reset present => RESET_VOTE
    """
    has_vote = False
    has_reset = False

    for log in logs or []:
        ev = (log.get("event") or "").lower()

        # Income claims should win over vote/reset if present
        if ev in {"claimbribe", "claimfees"}:
            return "CLAIM_REWARD"

        if ev == "vote":
            has_vote = True
        elif ev == "reset":
            has_reset = True

    if has_vote:
        return "VOTE"
    if has_reset:
        return "RESET_VOTE"
    return None


def sent_to_lock_total(rows_for_tx: List[Dict[str, Any]]) -> float:
    total = 0.0
    for r in rows_for_tx:
        if r.get("direction") != "out":
            continue
        if (r.get("to") or "").lower() not in LOCK_CONTRACTS:
            continue
        try:
            if r["tx_type"] == "base_token_transfer":
                total += float(r.get("token_amount") or 0)
            elif r["tx_type"] == "base_native":
                total += float(r.get("native_amount_eth") or 0)
        except Exception:
            continue
    return total


def classify_transactions(
    rows: List[Dict[str, Any]],
    logs_by_hash: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> None:
    """
    In-place classification: sets 'activity_type' per row.

    Voting logic added:
    - Vote logs -> VOTE (non-taxable)
    - Reset logs -> RESET_VOTE (non-taxable)
    - ClaimBribe / ClaimFees logs -> CLAIM_REWARD (income)

    Existing heuristics preserved.
    """
    logs_by_hash = logs_by_hash or {}

    # Group rows by tx hash
    by_hash: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_hash[r.get("hash", "")].append(r)

    # Known token contracts in this dataset (for APPROVAL vs VOTE split)
    token_contracts = {
        (r.get("token_contract") or "").lower() for r in rows if r.get("token_contract")
    }

    def aggregate_assets(tx_rows: List[Dict[str, Any]]):
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

    def as_float(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    def action_from_logs(logs: List[Dict[str, Any]]) -> str:
        """
        Returns a single canonical action based on logs.
        Note: voting is handled separately in classify_voting_from_logs().
        """
        for log in logs or []:
            topics = log.get("topics") or []
            topic0 = topics[0].lower() if topics else ""
            if topic0 in EVENT_SIGS:
                return EVENT_SIGS[topic0]

            ev = (log.get("event") or "").lower()

            # ERC20 + router/pair/gauge/lock basics
            if ev == "approval":
                return "approve"
            if ev == "swap":
                return "swap"
            if ev == "mint":
                return "lp_mint"
            if ev == "burn":
                return "lp_burn"
            if ev == "deposit":
                return "gauge_deposit"
            if ev == "withdraw":
                return "gauge_withdraw"
            if ev == "claimrewards":
                return "gauge_claim"
            if ev == "createlock":
                return "create_lock"
            if ev == "increaseamount":
                return "increase_lock_amount"
            if ev == "increaseunlocktime":
                return "increase_lock_duration"
            if ev in {"claimfees", "claimbribes"}:
                return "claim_lock_rewards"
            if ev == "rebase":
                return "lock_rebase"

        return ""

    def classify_voting_from_logs(logs: List[Dict[str, Any]]) -> Optional[str]:
        """
        Voting classification (must run before approval heuristics):

        - If ClaimBribe / ClaimFees present => CLAIM_REWARD (income)
        - Else if Vote present => VOTE
        - Else if Reset present => RESET_VOTE
        """
        has_vote = False
        has_reset = False

        for log in logs or []:
            ev = (log.get("event") or "").lower()

            # Income claims should win over vote/reset if present
            if ev in {"claimbribe", "claimfees"}:
                return "CLAIM_REWARD"

            if ev == "vote":
                has_vote = True
            elif ev == "reset":
                has_reset = True

        if has_vote:
            return "VOTE"
        if has_reset:
            return "RESET_VOTE"
        return None

    for tx_hash, tx_rows in by_hash.items():
        tx_logs = logs_by_hash.get(tx_hash, [])

        token_rows = [
            r
            for r in tx_rows
            if r["tx_type"] in ("base_token_transfer", "base_nft_transfer")
        ]

        # -------------------------
        # 1) Voting: log-driven FIRST
        # -------------------------
        voting_activity = classify_voting_from_logs(tx_logs)
        if voting_activity:
            for r in tx_rows:
                r["activity_type"] = voting_activity

            in_summary, out_summary = aggregate_assets(tx_rows)
            for r in tx_rows:
                r["token_in_assets"] = in_summary
                r["token_out_assets"] = out_summary
            continue

        # ---------------------------------
        # 2) Other event/log-driven activity
        # ---------------------------------
        action = action_from_logs(tx_logs)
        if action:
            if action in {"lp_mint", "gauge_deposit"}:
                tx_level_activity = "LP_DEPOSIT"
            elif action in {"lp_burn", "gauge_withdraw"}:
                tx_level_activity = "LP_WITHDRAW"
            elif action in {"gauge_claim", "claim_lock_rewards", "lock_claim"}:
                tx_level_activity = "CLAIM_REWARD"
            elif action in {
                "increase_lock_amount",
                "create_lock",
                "increase_lock_duration",
            }:
                tx_level_activity = "LOCK_INCREASE"
            elif action in {"approve", "erc20_approve"}:
                tx_level_activity = "APPROVAL"
            elif action == "swap":
                tx_level_activity = "SWAP"
            elif action == "lock_rebase":
                tx_level_activity = "LOCK_REBASE"
            else:
                tx_level_activity = "OTHER"

            for r in tx_rows:
                r["activity_type"] = tx_level_activity

            in_summary, out_summary = aggregate_assets(tx_rows)
            for r in tx_rows:
                r["token_in_assets"] = in_summary
                r["token_out_assets"] = out_summary
            continue

        # -----------------------------------------
        # 3) Transfer-driven lock increases (fallback)
        # -----------------------------------------
        if sent_to_lock_total(tx_rows) > 0:
            for r in tx_rows:
                r["activity_type"] = "LOCK_INCREASE"
            continue

        # ------------------------------------------------
        # 4) If no token rows: native-only classification
        # ------------------------------------------------
        if not token_rows:
            zero_value_outs = [
                r
                for r in tx_rows
                if r["tx_type"] == "base_native"
                and r.get("direction") == "out"
                and as_float(r.get("native_amount_eth", 0)) == 0.0
                and r.get("to")
            ]

            if zero_value_outs:
                native_row = zero_value_outs[0]
                to_addr = (native_row.get("to") or "").lower()

                try:
                    gas_used = int(native_row.get("gasUsed") or 0)
                except Exception:
                    gas_used = 0

                # IMPORTANT: with voting already handled by logs, these heuristics are now safer.
                if to_addr in APPROVAL_CONTRACT_HINTS or to_addr in token_contracts:
                    activity = "APPROVAL"
                elif to_addr in LOCK_VOTE_CONTRACTS:
                    activity = "LOCK_INCREASE"
                elif to_addr in VOTE_CONTRACT_HINTS:
                    activity = "VOTE"
                elif gas_used >= VOTE_MIN_GASUSED:
                    activity = "VOTE"
                else:
                    activity = "APPROVAL"

                for r in tx_rows:
                    r["activity_type"] = activity

                in_summary, out_summary = aggregate_assets(tx_rows)
                for r in tx_rows:
                    r["token_in_assets"] = in_summary
                    r["token_out_assets"] = out_summary
                continue

            for r in tx_rows:
                if r["tx_type"] == "base_native":
                    if r["direction"] == "out" and r.get("native_amount_eth"):
                        r["activity_type"] = "SEND_NATIVE"
                    elif r["direction"] == "in" and r.get("native_amount_eth"):
                        r["activity_type"] = "RECEIVE_NATIVE"
                    else:
                        r["activity_type"] = "OTHER"

            in_summary, out_summary = aggregate_assets(tx_rows)
            for r in tx_rows:
                r["token_in_assets"] = in_summary
                r["token_out_assets"] = out_summary
            continue

        # --------------------------------
        # 5) Token-based analysis (existing)
        # --------------------------------
        lp_out = [
            r
            for r in token_rows
            if r.get("direction") == "out"
            and is_lp_symbol(r.get("token_symbol"), r.get("token_name", ""))
        ]
        lp_in = [
            r
            for r in token_rows
            if r.get("direction") == "in"
            and is_lp_symbol(r.get("token_symbol"), r.get("token_name", ""))
        ]
        nonlp_out = [
            r
            for r in token_rows
            if r.get("direction") == "out"
            and not is_lp_symbol(r.get("token_symbol"), r.get("token_name", ""))
        ]
        nonlp_in = [
            r
            for r in token_rows
            if r.get("direction") == "in"
            and not is_lp_symbol(r.get("token_symbol"), r.get("token_name", ""))
        ]

        # CLAIM_REWARD: in-only, no LP movement
        is_claim = (
            len(nonlp_in) > 0
            and len(nonlp_out) == 0
            and len(lp_in) == 0
            and len(lp_out) == 0
        )

        # LP_DEPOSIT: send non-LP tokens, receive LP
        is_deposit = len(nonlp_out) > 0 and len(lp_in) > 0

        # LP_WITHDRAW: send LP, receive non-LP tokens
        is_withdraw = len(lp_out) > 0 and len(nonlp_in) > 0

        # Gauge stake/unstake: pure LP movement
        if not is_deposit and not is_withdraw:
            if (
                len(lp_out) > 0
                and len(lp_in) == 0
                and len(nonlp_in) == 0
                and len(nonlp_out) == 0
            ):
                is_deposit = True
            elif (
                len(lp_in) > 0
                and len(lp_out) == 0
                and len(nonlp_in) == 0
                and len(nonlp_out) == 0
            ):
                is_withdraw = True

        # SWAP: non-LP in+out, no LP, and not claim
        is_swap = (
            len(nonlp_out) > 0
            and len(nonlp_in) > 0
            and len(lp_in) == 0
            and len(lp_out) == 0
            and not is_claim
        )

        tx_level_activity = "OTHER"
        if is_swap:
            tx_level_activity = "SWAP"
        elif is_deposit:
            tx_level_activity = "LP_DEPOSIT"
        elif is_withdraw:
            tx_level_activity = "LP_WITHDRAW"
        elif is_claim:
            tx_level_activity = "CLAIM_REWARD"

        # Assign to token rows
        for r in token_rows:
            r["activity_type"] = tx_level_activity

        # Also assign same activity type to native rows for that tx (gas cost)
        for r in tx_rows:
            if r["tx_type"] == "base_native":
                r["activity_type"] = tx_level_activity

        # Aggregate asset movement summaries per tx
        in_summary, out_summary = aggregate_assets(tx_rows)
        for r in tx_rows:
            r["token_in_assets"] = in_summary
            r["token_out_assets"] = out_summary
