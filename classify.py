from collections import defaultdict
from typing import List, Dict, Any, DefaultDict

from helpers import is_lp_symbol


def classify_transactions(rows: List[Dict[str, Any]]) -> None:
    """
    In-place classification: sets 'activity_type' per row.

    Heuristics per tx (by hash):

    - CLAIM_REWARD:
        token_in (non-LP), no token_out, no LP tokens move
    - LP_DEPOSIT:
        non-LP token_out, LP token_in
    - LP_WITHDRAW:
        LP token_out, non-LP token_in
    - SWAP:
        non-LP token_out, non-LP token_in, no LP tokens
    - OTHER:
        everything else
    """
    # Group rows by tx hash
    by_hash: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_hash[r.get("hash", "")].append(r)

    for tx_hash, tx_rows in by_hash.items():
        token_rows = [r for r in tx_rows if r["tx_type"] == "base_token_transfer"]

        # If no token rows, then classify based on native only (e.g., simple send)
        if not token_rows:
            for r in tx_rows:
                if r["tx_type"] == "base_native":
                    if r["direction"] == "out" and r["native_amount_eth"]:
                        r["activity_type"] = "SEND_NATIVE"
                    elif r["direction"] == "in" and r["native_amount_eth"]:
                        r["activity_type"] = "RECEIVE_NATIVE"
                    else:
                        r["activity_type"] = "OTHER"
            continue

        # Token-based analysis
        lp_out = [
            r
            for r in token_rows
            if r["direction"] == "out" and is_lp_symbol(r["token_symbol"])
        ]
        lp_in = [
            r
            for r in token_rows
            if r["direction"] == "in" and is_lp_symbol(r["token_symbol"])
        ]
        nonlp_out = [
            r
            for r in token_rows
            if r["direction"] == "out" and not is_lp_symbol(r["token_symbol"])
        ]
        nonlp_in = [
            r
            for r in token_rows
            if r["direction"] == "in" and not is_lp_symbol(r["token_symbol"])
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

        # SWAP: non-LP in+out, no LP
        is_swap = (
            len(nonlp_out) > 0
            and len(nonlp_in) > 0
            and len(lp_in) == 0
            and len(lp_out) == 0
            and not is_claim
        )

        tx_level_activity = "OTHER"
        if is_claim:
            tx_level_activity = "CLAIM_REWARD"
        elif is_deposit:
            tx_level_activity = "LP_DEPOSIT"
        elif is_withdraw:
            tx_level_activity = "LP_WITHDRAW"
        elif is_swap:
            tx_level_activity = "SWAP"

        # Assign to token rows
        for r in token_rows:
            r["activity_type"] = tx_level_activity

        # Also assign same activity type to native rows for that tx (gas cost)
        for r in tx_rows:
            if r["tx_type"] == "base_native":
                r["activity_type"] = tx_level_activity
