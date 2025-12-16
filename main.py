from config import BASE_WALLET_ADDRESS
from etherscan_api import (
    fetch_all_base_native_txs,
    fetch_all_base_token_transfers,
)
from normalize import normalize_for_csv
from classify import classify_transactions
from csv_export import write_csv
import os
import pandas as pd
from price_fetchers import build_events_from_base_csv
from pricing_logic import attach_prices_to_events, merge_events_back_to_base_csv

# CG - wdNzZX6qnger7EZJQtRmgqkP
filename = f"csv/base_activity.csv"


def main():
    address = BASE_WALLET_ADDRESS
    if not address:
        raise RuntimeError(
            "Set BASE_WALLET_ADDRESS in .env to your Base wallet address "
            "(e.g. 0xabc...)"
        )

    address = address.strip()

    print(f"Exporting Base activity for address: {address}")

    native_txs = fetch_all_base_native_txs(address)
    token_txs = fetch_all_base_token_transfers(address)

    rows = normalize_for_csv(address, native_txs, token_txs)

    # Add classification in-place
    classify_transactions(rows)

    # safe = address.lower().replace("0x", "")

    write_csv(filename, rows)


def enrich():
    input_path = filename
    output_filename = "base_2025"
    output_path = output_filename + "_with_usd.csv"

    print(f"Loading CSV: {input_path}")
    df = pd.read_csv(input_path)

    # --- FILTER TO 2025 ONLY ---
    print("Filtering to 2025 transactions only...")

    # Ensure we have a datetime column
    if "timeStamp_iso" in df.columns:
        df["dt"] = pd.to_datetime(df["timeStamp_iso"], errors="coerce", utc=True)
    else:
        df["timeStamp"] = pd.to_numeric(df["timeStamp"], errors="coerce")
        df["dt"] = pd.to_datetime(df["timeStamp"], unit="s", errors="coerce", utc=True)

    df_2025 = df[df["dt"].dt.year == 2025].copy()
    print(f"Total rows in 2025: {len(df_2025)}")

    # -------------------------------------------------------
    print("Building normalized events (2025 only)...")
    events_df = build_events_from_base_csv(df_2025)
    print(f"Total in/out events in 2025: {len(events_df)}")

    print("Fetching CoinGecko prices (2025 only) and attaching to events...")
    events_priced = attach_prices_to_events(events_df)

    print("Merging 2025 price data back into 2025 CSV rows...")
    df_2025_with_prices = merge_events_back_to_base_csv(df_2025, events_priced)

    print(f"Writing output to: {output_path}")
    df_2025_with_prices.to_csv(output_path, index=False)

    print("Done — 2025-only enriched file created.")


if __name__ == "__main__":
    if os.path.exists(filename):
        enrich()
    else:
        main()
