import csv
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Set

from ..shared.config import (
    AERODROME_CONTRACTS,
    LOCK_CONTRACTS,
    LOCK_VOTE_CONTRACTS,
    PROTOCOL_DATA_DIR,
    TOKEN_CACHE_PATH,
    WALLET_SPAM_TOKENS_PATH,
    WALLET_SPAM_TX_HASHES_PATH,
    unique_successful_hash,
    topic0,
)

AERODROME_POOLS_CSV = os.path.join(PROTOCOL_DATA_DIR, "aerodrome_pools.csv")

TRANSFER_TOPIC0 = topic0("Transfer(address,address,uint256)")
APPROVAL_TOPIC0 = topic0("Approval(address,address,uint256)")

# Uniswap V2-style events (LP pools)
MINT_V2_TOPIC0 = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
BURN_V2_TOPIC0 = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
SWAP_V2_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

# Uniswap V3-style events
MINT_V3_TOPIC0 = topic0("Mint(address,address,int24,int24,uint128,uint256,uint256)")
BURN_V3_TOPIC0 = topic0("Burn(address,int24,int24,uint128,uint256,uint256)")
SWAP_V3_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# WETH-style deposit/withdraw
DEPOSIT_TOPIC0 = topic0("Deposit(address,uint256)")
WITHDRAW_TOPIC0 = topic0("Withdraw(address,uint256)")

# Aerodrome gauge deposit/withdraw
GAUGE_DEPOSIT_TOPIC0 = "0xe1fffcc4923d02f16c1d26a8e22a87b9345c16a3c2e2d3b6a6d9d5f4f4f3c5b5"
GAUGE_WITHDRAW_TOPIC0 = "0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364"

NON_PRICEABLE_DUST_TOKENS = 1e-6
NON_PRICEABLE_DUST_RAW_FALLBACK = 1000


@dataclass(frozen=True)
class LogFilterContext:
    known_contracts: Set[str]
    priceable_tokens: Set[str]
    allowed_topic0: Set[str]
    token_decimals: Dict[str, int]


def _norm_addr(addr: str) -> str:
    return (addr or "").strip().lower()


def _load_spam_tokens(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {_norm_addr(x) for x in data if x}
        if isinstance(data, dict):
            tokens = data.get("tokens", data)
            if isinstance(tokens, list):
                return {_norm_addr(x) for x in tokens if x}
            if isinstance(tokens, dict):
                return {_norm_addr(x) for x in tokens.keys() if x}
    except Exception:
        return set()
    return set()


def load_spam_tokens(path: str) -> Set[str]:
    return _load_spam_tokens(path)


def load_spam_tx_hashes(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x).strip().lower() for x in data if x}
    except Exception:
        return set()
    return set()


def _save_spam_tokens(path: str, tokens: Iterable[str]) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted({t for t in tokens if t}), f)
    except Exception:
        return


def _save_spam_tx_hashes(path: str, tx_hashes: Iterable[str]) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted({h for h in tx_hashes if h}), f)
    except Exception:
        return


def _load_token_decimals(path: str) -> Dict[str, int]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        out: Dict[str, int] = {}
        if isinstance(data, dict):
            for addr, meta in data.items():
                if not addr:
                    continue
                if isinstance(meta, dict):
                    dec = meta.get("decimals")
                    if isinstance(dec, int):
                        out[_norm_addr(addr)] = dec
        return out
    except Exception:
        return {}


def _load_aerodrome_pools(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    pools: Set[str] = set()
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                addr = _norm_addr(row.get("pool", ""))
                if addr:
                    pools.add(addr)
    except Exception:
        return set()
    return pools


def _decode_uint256(hex_str: str) -> int:
    if not hex_str or not isinstance(hex_str, str):
        return 0
    hs = hex_str[2:] if hex_str.startswith("0x") else hex_str
    if not hs:
        return 0
    try:
        return int(hs, 16)
    except Exception:
        return 0


def _is_dust_transfer(amount_raw: int, decimals: int) -> bool:
    if amount_raw <= 0:
        return True
    if decimals <= 0:
        return amount_raw <= NON_PRICEABLE_DUST_RAW_FALLBACK
    decimals = max(0, min(decimals, 36))
    cutoff = int((10**decimals) * NON_PRICEABLE_DUST_TOKENS)
    if cutoff <= 0:
        cutoff = 1
    return amount_raw <= cutoff


def _allowed_topic0_set() -> Set[str]:
    return {
        TRANSFER_TOPIC0,
        APPROVAL_TOPIC0,
        MINT_V2_TOPIC0,
        BURN_V2_TOPIC0,
        SWAP_V2_TOPIC0,
        MINT_V3_TOPIC0,
        BURN_V3_TOPIC0,
        SWAP_V3_TOPIC0,
        DEPOSIT_TOPIC0,
        WITHDRAW_TOPIC0,
        GAUGE_DEPOSIT_TOPIC0,
        GAUGE_WITHDRAW_TOPIC0,
    }


def build_log_filter_context() -> LogFilterContext:
    canonical_tokens = {_norm_addr(x) for x in unique_successful_hash().values()}
    aerodrome_pools = _load_aerodrome_pools(AERODROME_POOLS_CSV)
    lp_tokens = {_norm_addr(x) for x in AERODROME_CONTRACTS.get("lp_pair", set())}
    gauges = {_norm_addr(x) for x in AERODROME_CONTRACTS.get("gauge", set())}
    voting_escrow = {_norm_addr(x) for x in AERODROME_CONTRACTS.get("voting_escrow", set())}
    locks = {_norm_addr(x) for x in LOCK_CONTRACTS} | {_norm_addr(x) for x in LOCK_VOTE_CONTRACTS}

    known_contracts = set()
    known_contracts |= canonical_tokens
    known_contracts |= aerodrome_pools
    known_contracts |= lp_tokens
    known_contracts |= gauges
    known_contracts |= voting_escrow
    known_contracts |= locks

    token_decimals = _load_token_decimals(TOKEN_CACHE_PATH)

    return LogFilterContext(
        known_contracts=known_contracts,
        priceable_tokens=canonical_tokens,
        allowed_topic0=_allowed_topic0_set(),
        token_decimals=token_decimals,
    )


def _get_topic0(log: Dict[str, Any]) -> str:
    if log.get("topic0"):
        return _norm_addr(log.get("topic0"))
    topics = log.get("topics") or []
    if topics:
        return _norm_addr(topics[0] or "")
    return ""


def filter_logs_by_hash(
    logs_by_hash: Dict[str, List[Dict[str, Any]]],
    *,
    spam_tokens_path: str = WALLET_SPAM_TOKENS_PATH,
    spam_tx_hashes_path: str = WALLET_SPAM_TX_HASHES_PATH,
    context: LogFilterContext | None = None,
) -> Dict[str, List[Dict[str, Any]]]:
    context = context or build_log_filter_context()
    spam_tokens = _load_spam_tokens(spam_tokens_path)
    spam_tx_hashes: Set[str] = set()

    filtered: Dict[str, List[Dict[str, Any]]] = {}
    for tx_hash, logs in (logs_by_hash or {}).items():
        kept: List[Dict[str, Any]] = []
        for log in logs or []:
            addr = _norm_addr(log.get("address", ""))
            if not addr:
                continue
            if addr in spam_tokens:
                if tx_hash:
                    spam_tx_hashes.add(tx_hash)
                continue
            if addr not in context.known_contracts:
                spam_tokens.add(addr)
                if tx_hash:
                    spam_tx_hashes.add(tx_hash)
                continue

            topic0_hex = _get_topic0(log)
            if topic0_hex not in context.allowed_topic0:
                continue

            if topic0_hex == TRANSFER_TOPIC0 and addr not in context.priceable_tokens:
                amount_raw = _decode_uint256(log.get("data", ""))
                decimals = context.token_decimals.get(addr, 0)
                if _is_dust_transfer(amount_raw, decimals):
                    continue

            kept.append(log)

        filtered[tx_hash] = kept

    _save_spam_tokens(spam_tokens_path, spam_tokens)
    _save_spam_tx_hashes(spam_tx_hashes_path, spam_tx_hashes)
    return filtered
