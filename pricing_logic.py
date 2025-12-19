import time
import pandas as pd

from price_fetchers import (
    fetch_erc20_prices_on_base,
    fetch_native_eth_prices,
    build_events_from_base_csv,
    ensure_base_datetime_columns,
)
from config import REQUEST_SLEEP_SEC


def attach_prices_to_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Price events by unique CoinGecko Base contract to minimize API calls.
    """
    if events_df.empty:
        return events_df.assign(price_usd=pd.NA, value_usd=pd.NA)

    events_df = events_df.copy()
    events_df["price_usd"] = pd.NA
    events_df["value_usd"] = pd.NA
    events_df["cg_base_contract"] = events_df["cg_base_contract"].fillna("").astype(str)

    # Build unique contract list (skip blanks)
    unique_contracts = sorted(
        c for c in events_df["cg_base_contract"].unique() if c and c != "nan"
    )
    print(f"Unique CoinGecko Base contracts to price: {len(unique_contracts)}")

    # Show which tokens map to each contract before fetching
    contract_summaries = []
    for contract in unique_contracts:
        subset = events_df[
            events_df["cg_base_contract"].str.lower() == contract.lower()
        ]
        token_pairs = (
            subset[["token_symbol", "token_name"]]
            .dropna(how="all")
            .drop_duplicates()
            .values
        )
        token_desc = [f"{sym} ({name})" if name else sym for sym, name in token_pairs]
        contract_summaries.append({"contract": contract, "tokens": token_desc})

    print("Contracts to price with associated tokens:")
    for entry in contract_summaries:
        tokens_str = ", ".join(entry["tokens"]) if entry["tokens"] else "None"
        print(f"  {entry['contract']}: {tokens_str}")

    for i, contract in enumerate(unique_contracts, start=1):
        contract_lower = contract.lower()
        ev_mask = events_df["cg_base_contract"].str.lower() == contract_lower
        ev_sub = events_df.loc[ev_mask, ["idx", "timestamp_sec", "quantity"]].copy()

        if ev_sub.empty:
            continue

        # Token-specific time range with small padding
        min_ts = int(ev_sub["timestamp_sec"].min()) - 3600
        max_ts = int(ev_sub["timestamp_sec"].max()) + 3600

        print(
            f"[{i}/{len(unique_contracts)}] Fetching prices for contract {contract}..."
        )
        try:
            if contract_lower == "native":
                price_df = fetch_native_eth_prices(min_ts, max_ts)
            else:
                price_df = fetch_erc20_prices_on_base(contract, min_ts, max_ts)
        except Exception as exc:
            print(f"  !! Failed to fetch prices for {contract}: {exc}")
            time.sleep(REQUEST_SLEEP_SEC)
            continue

        if price_df.empty:
            print(f"  !! No price data returned for {contract}")
            continue

        price_df = price_df.sort_values("timestamp_sec")
        ev_sub = ev_sub.sort_values("timestamp_sec")

        merged = pd.merge_asof(
            ev_sub,
            price_df[["timestamp_sec", "price_usd"]],
            on="timestamp_sec",
            direction="nearest",
        )

        events_df.loc[ev_mask, "price_usd"] = merged["price_usd"].values
        events_df.loc[ev_mask, "value_usd"] = (
            merged["price_usd"].astype(float) * merged["quantity"].astype(float)
        ).values

        time.sleep(REQUEST_SLEEP_SEC)

    return events_df


def merge_events_back_to_base_csv(
    base_df: pd.DataFrame,
    events_with_prices: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join price_usd and value_usd (per row index) back into the original CSV.
    """
    cols_to_merge = events_with_prices[["idx", "price_usd", "value_usd"]].copy()

    merged = base_df.copy()
    merged = merged.reset_index().rename(columns={"index": "idx"})
    merged = merged.merge(cols_to_merge, on="idx", how="left")
    merged = merged.set_index("idx").sort_index()

    return merged


def enrich_yearly_csv(
    input_path: str,
    output_path: str,
    year: int = 2025,
) -> None:
    """
    Read a base activity CSV, filter to a specific year, fetch prices,
    merge back, and write to disk.
    """
    print(f"Loading CSV: {input_path}")
    df = pd.read_csv(input_path)
    df = ensure_base_datetime_columns(df)

    print(f"Filtering to {year} transactions only...")
    df_year = df[df["dt"].dt.year == year].copy()
    print(f"Total rows in {year}: {len(df_year)}")

    print(f"Building normalized events ({year} only)...")
    events_df = build_events_from_base_csv(df_year)
    print(f"Total in/out events in {year}: {len(events_df)}")

    print(f"Fetching CoinGecko prices ({year} only) and attaching to events...")
    events_priced = attach_prices_to_events(events_df)

    print(f"Merging {year} price data back into {year} CSV rows...")
    df_year_with_prices = merge_events_back_to_base_csv(df_year, events_priced)

    print(f"Writing output to: {output_path}")
    df_year_with_prices.to_csv(output_path, index=False)
