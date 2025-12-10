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


def is_lp_symbol(symbol: str) -> bool:
    if not symbol:
        return False
    s = symbol.lower()
    return any(hint in s for hint in LP_SYMBOL_HINTS)


def protocol_label_for_address(addr: str) -> str:
    return PROTOCOL_ADDRESS_LABELS.get((addr or "").lower(), "")
