import os
import pandas as pd
from typing import Dict, Set

from ..shared.config import (
    AERODROME_CONTRACTS,
    AERO_POOL_FACTORY_VAMM,
    LOCK_CONTRACTS,
    LOCK_VOTE_CONTRACTS,
    PROTOCOL_DATA_DIR,
    VOTE_CONTRACT_HINTS,
    APPROVAL_CONTRACT_HINTS,
)


def _load_pool_addresses_from_csv(path: str) -> Dict[str, Dict[str, str]]:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
        pools = {}
        for _, row in df.iterrows():
            pool = str(row.get("pool") or "").lower()
            if not pool:
                continue
            pools[pool] = {
                "token0": row.get("token0", ""),
                "token1": row.get("token1", ""),
                "pool_type": row.get("pool_type", ""),
            }
        return pools
    except Exception:
        return {}


def load_aerodrome_registry() -> Dict[str, Set[str]]:
    """
    Load known Aerodrome contract addresses.
    Returns lowercase address sets keyed by category.
    """
    pools_csv = os.path.join(PROTOCOL_DATA_DIR, "aerodrome_pools.csv")
    pools_meta = _load_pool_addresses_from_csv(pools_csv)
    pools = set(pools_meta.keys())

    gauges = {a.lower() for a in AERODROME_CONTRACTS.get("gauge", set())}
    voting_escrow = {a.lower() for a in AERODROME_CONTRACTS.get("voting_escrow", set())}

    lock_contracts = {a.lower() for a in LOCK_CONTRACTS}
    lock_vote = {a.lower() for a in LOCK_VOTE_CONTRACTS}
    vote_hints = {a.lower() for a in VOTE_CONTRACT_HINTS}
    approval_hints = {a.lower() for a in APPROVAL_CONTRACT_HINTS}

    return {
        "pools": pools,
        "gauges": gauges,
        "voting_escrow": voting_escrow,
        "lock_contracts": lock_contracts,
        "lock_vote": lock_vote,
        "vote_hints": vote_hints,
        "approval_hints": approval_hints,
        # Allowed emitters: pools/gauges/escrow/vote/lock contracts only.
        "all": pools
        | gauges
        | voting_escrow
        | lock_contracts
        | lock_vote
        | vote_hints,
        "pool_metadata": pools_meta,
    }
