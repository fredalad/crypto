import pandas as pd
from price_fetchers import fetch_erc20_prices_on_base, fetch_native_eth_prices
from config import REQUEST_SLEEP_SEC
import time
from config import unique_tokens, unique_successful_hash


def attach_prices_to_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (token_key), fetch a price series once and map to each event timestamp
    using nearest time.
    """
    if events_df.empty:
        return events_df.assign(price_usd=pd.NA, value_usd=pd.NA)

    # Global time range for the events
    min_ts = int(events_df["timestamp_sec"].min())
    max_ts = int(events_df["timestamp_sec"].max())
    # Add small padding
    min_ts -= 3600
    max_ts += 3600

    events_df = events_df.copy()
    events_df["price_usd"] = pd.NA
    events_df["value_usd"] = pd.NA

    split_cols = events_df["token_key"].str.split("|", n=1, expand=True)

    events_df["token_symbol"] = split_cols[0]
    events_df["token_hash"] = split_cols[1]
    # --- Find invalid (unrecognized) token symbols ---
    all_symbols = set(events_df["token_symbol"].unique())
    invalid_symbols = sorted(all_symbols - unique_tokens())

    print("❌ Invalid token symbols found:")
    print(invalid_symbols)

    # --- Keep only valid rows ---
    events_df = events_df[events_df["token_symbol"].isin(unique_tokens())]

    # --- Final sorted list of valid token symbols in dataframe ---
    token_keys = sorted(
        [
            {"symbol": sym, "hash": h}
            for sym, h in events_df[["token_symbol", "token_hash"]]
            .drop_duplicates()
            .values
        ],
        key=lambda x: x["symbol"],
    )

    unique_contract_hash = unique_successful_hash()
    print(unique_contract_hash)
    # print("as;ldjflasjdflkasdklfjaskldjfklsajdklfjskldj")
    for i, token_key in enumerate(token_keys, start=1):
        break
        symbol = token_key["symbol"]
        contract = unique_contract_hash[symbol]
        print(f"[{i}/{len(token_keys)}] Fetching prices for {token_key}...")
        try:
            print()
            if contract == "NATIVE" and symbol == "ETH_BASE":
                # Native ETH on Base -> use Ethereum spot price
                price_df = fetch_native_eth_prices(min_ts, max_ts)

            else:
                # ERC-20 on Base by contract address
                price_df = fetch_erc20_prices_on_base(contract, min_ts, max_ts)

        except Exception as e:
            print(f"  !! Failed to fetch prices for {token_key}: {e}")
            time.sleep(REQUEST_SLEEP_SEC)

            continue

        if price_df.empty:
            print(f"  !! No price data returned for {token_key}")
            continue

        # Prepare for merge_asof
        # print(price_df)
        price_df = price_df.sort_values("timestamp_sec")
        ev_tok = events_df["token_symbol"] == symbol

        # Use merge_asof for nearest price mapping
        # We do this per token to keep it simple
        ev_sub = events_df.loc[ev_tok, ["idx", "timestamp_sec", "quantity"]].copy()
        ev_sub = ev_sub.sort_values("timestamp_sec")

        merged = pd.merge_asof(
            ev_sub,
            price_df[["timestamp_sec", "price_usd"]],
            on="timestamp_sec",
            direction="nearest",
        )

        # Assign back
        events_df.loc[ev_tok, "price_usd"] = merged["price_usd"].values
        events_df.loc[ev_tok, "value_usd"] = (
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
    # events_with_prices has "idx" referencing base_df.index
    cols_to_merge = events_with_prices[["idx", "price_usd", "value_usd"]].copy()

    # Merge on index via idx
    merged = base_df.copy()
    merged = merged.reset_index().rename(columns={"index": "idx"})
    merged = merged.merge(cols_to_merge, on="idx", how="left")
    merged = merged.set_index("idx").sort_index()

    return merged
