from datetime import datetime, timezone
from typing import Dict, Any

from config import LP_SYMBOL_HINTS, PROTOCOL_ADDRESS_LABELS


def to_iso(timestamp: str) -> str:
    try:
        ts_int = int(timestamp)
        return datetime.fromtimestamp(ts_int, tz=timezone.utc).isoformat()
    except Exception:
        return ""


def wei_to_eth(wei_str: str) -> float:
    try:
        return int(wei_str) / 10**18
    except Exception:
        return 0.0


def token_amount(value_raw: str, decimals: str) -> float:
    try:
        return int(value_raw) / (10 ** int(decimals))
    except Exception:
        return 0.0


def direction(address: str, from_addr: str, to_addr: str) -> str:
    addr = address.lower()
    f = (from_addr or "").lower()
    t = (to_addr or "").lower()
    if f == addr and t != addr:
        return "out"
    if t == addr and f != addr:
        return "in"
    if f == addr and t == addr:
        return "self"
    return "other"


def is_lp_symbol(symbol: str, token_name: str = "") -> bool:
    """
    Lightweight LP detector: checks both symbol and token name for common LP hints.
    Aerodrome pools use symbols like 'vAMM-XXX/YYY', so we include 'amm' patterns.
    """
    if not symbol and not token_name:
        return False
    s = f"{symbol or ''} {token_name or ''}".lower()
    return any(hint in s for hint in LP_SYMBOL_HINTS)


def protocol_label_for_address(addr: str) -> str:
    return PROTOCOL_ADDRESS_LABELS.get((addr or "").lower(), "")


def detect_contract_type(tx_to: str, logs: list) -> str:
    """
    Rough contract typing for Aerodrome:
    - Direct address match via AERODROME_CONTRACTS
    - Otherwise infer from log event names
    """
    from config import AERODROME_CONTRACTS

    tx_to = (tx_to or "").lower()

    for category, contracts in AERODROME_CONTRACTS.items():
        if tx_to in contracts:
            return category

    # Infer from logs (expects decoded event names in log["event"])
    for log in logs or []:
        event = (log.get("event") or "").lower()
        if event in {"mint", "burn"}:
            return "lp_pair"
        if event in {"deposit", "withdraw", "claimrewards"}:
            return "gauge"
        if event in {"rebase", "createlock", "increaseamount"}:
            return "voting_escrow"

    return "unknown"
