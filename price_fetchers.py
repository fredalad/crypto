import pandas as pd
import time
from config import ASSET_PLATFORM_ID_BASE
from typing import List, Dict, Any
from http_helper import cg_get


def ensure_base_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the dataframe has parsed datetime and numeric timestamp columns
    derived from timeStamp/timeStamp_iso.
    """
    df = df.copy()
    if "dt" not in df.columns:
        if "timeStamp_iso" in df.columns:
            df["dt"] = pd.to_datetime(df["timeStamp_iso"], errors="coerce", utc=True)
        else:
            df["timeStamp"] = pd.to_numeric(df["timeStamp"], errors="coerce")
            df["dt"] = pd.to_datetime(df["timeStamp"], unit="s", errors="coerce", utc=True)

    df["timestamp_sec"] = pd.to_numeric(df["timeStamp"], errors="coerce").astype("Int64")
    return df


THROTTLE_SECONDS = 2.1


def cg_get_throttled(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Throttle CoinGecko calls to ~30/min.
    """
    print(f"Waiting {THROTTLE_SECONDS}s before CoinGecko call to {path}...")
    time.sleep(THROTTLE_SECONDS)
    print("Proceeding with CoinGecko call.")
    return cg_get(path, params)


# --------------------------------------------------------------------
# COINGECKO TOKEN LOOKUPS
# --------------------------------------------------------------------


def search_coingecko_token_contract(symbol: str, name: str) -> str:
    """
    Search CoinGecko by name/symbol and return Base contract address if found.
    """
    symbol = (symbol or "").strip()
    name = (name or "").strip()

    if not symbol and not name:
        return ""

    query = name or symbol
    search = cg_get_throttled("/search", {"query": query})
    coins = search.get("coins", []) if isinstance(search, dict) else []

    def matches(entry: Dict[str, Any]) -> bool:
        sym_ok = entry.get("symbol", "").lower() == symbol.lower()
        name_ok = entry.get("name", "").lower() == name.lower() if name else True
        return sym_ok and name_ok

    match = next((c for c in coins if matches(c)), None)
    if not match and coins:
        # Fallback: symbol match only
        match = next(
            (c for c in coins if c.get("symbol", "").lower() == symbol.lower()), None
        )

    if not match:
        return ""

    cg_id = match.get("id")
    if not cg_id:
        return ""

    try:
        coin = cg_get_throttled(
            f"/coins/{cg_id}",
            {
                "localization": "false",
                "tickers": "false",
                "market_data": "false",
                "community_data": "false",
                "developer_data": "false",
                "sparkline": "false",
            },
        )
    except Exception:
        return ""

    platforms = coin.get("platforms", {}) if isinstance(coin, dict) else {}
    # CoinGecko may use "base" or "base-chain" keys
    for key in ("base", "base-chain"):
        addr = platforms.get(key)
        if addr:
            return addr.lower()

    return ""


def fetch_native_eth_prices(unix_from: int, unix_to: int) -> pd.DataFrame:
    """
    Fetch ETH/USD historical prices for the given UNIX range.
    Uses /coins/{id}/market_chart/range. :contentReference[oaicite:4]{index=4}
    """
    data = cg_get(
        "/coins/ethereum/market_chart/range",
        {
            "vs_currency": "usd",
            "from": unix_from,
            "to": unix_to,
            # "precision": "full"  # optional
        },
    )
    prices = data.get("prices", [])
    if not prices:
        return pd.DataFrame(columns=["timestamp_sec", "price_usd"])

    # prices items are [timestamp_ms, price]
    df = pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"])
    df["timestamp_sec"] = (df["timestamp_ms"] / 1000).astype("int64")
    df = (
        df[["timestamp_sec", "price_usd"]]
        .drop_duplicates()
        .sort_values("timestamp_sec")
    )
    return df


def fetch_erc20_prices_on_base(
    contract_address: str,
    unix_from: int,
    unix_to: int,
) -> pd.DataFrame:
    """
    Fetch ERC-20 token prices on Base by contract address
    using /coins/{asset_platform_id}/contract/{contract_address}/market_chart/range. :contentReference[oaicite:5]{index=5}
    """
    contract_address = contract_address.lower()

    path = f"/coins/{ASSET_PLATFORM_ID_BASE}/contract/{contract_address}/market_chart/range"
    data = cg_get(
        path,
        {
            "vs_currency": "usd",
            "from": unix_from,
            "to": unix_to,
            # "precision": "full"
        },
    )

    prices = data.get("prices", [])
    if not prices:
        return pd.DataFrame(columns=["timestamp_sec", "price_usd"])

    df = pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"])
    df["timestamp_sec"] = (df["timestamp_ms"] / 1000).astype("int64")
    df = (
        df[["timestamp_sec", "price_usd"]]
        .drop_duplicates()
        .sort_values("timestamp_sec")
    )
    return df


# --------------------------------------------------------------------
# BUILD EVENTS FROM YOUR BASE CSV
# --------------------------------------------------------------------


def build_events_from_base_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn your base_activity CSV into a normalized event table:
    - One row per (tx, direction, token)
    - Has token_symbol, token_contract, token_key, quantity, timestamp_sec
    """

    df = ensure_base_datetime_columns(df)

    events: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        tx_type = row.get("tx_type")
        direction = row.get("direction")
        dt = row.get("dt")
        ts = row.get("timestamp_sec")
        if pd.isna(dt) or pd.isna(ts):
            continue

        if direction not in ("in", "out"):
            continue

        # Determine token + quantity
        if tx_type == "base_native":
            qty = row.get("native_amount_eth")
            if pd.isna(qty) or qty == 0:
                continue
            token_symbol = "ETH_BASE"
            token_contract = "NATIVE"
        elif tx_type == "base_token_transfer":
            qty = row.get("token_amount")
            if pd.isna(qty) or qty == 0:
                continue
            token_symbol = row.get("token_symbol") or "UNKNOWN"
            token_contract = row.get("token_contract") or "UNKNOWN"
        else:
            continue

        token_key = f"{token_symbol}|{token_contract}"

        events.append(
            {
                "idx": idx,  # index in original DF
                "hash": row.get("hash"),
                "dt": dt,
                "timestamp_sec": int(ts),
                "year": dt.year,
                "tx_type": tx_type,
                "direction": direction,
                "activity_type": row.get("activity_type"),
                "token_symbol": token_symbol,
                "token_contract": token_contract,
                "token_name": row.get("token_name"),
                "cg_base_contract": row.get("cg_base_contract"),
                "token_key": token_key,
                "quantity": float(qty),
            }
        )

    events_df = pd.DataFrame(events)
    return events_df
