from config import (
    BASE_WALLET_ADDRESS,
    LOCK_CONTRACTS,
    LOCK_VOTE_CONTRACTS,
    APPROVAL_CONTRACT_HINTS,
    VOTE_CONTRACT_HINTS,
)
from etherscan_api import (
    fetch_all_base_native_txs,
    fetch_all_base_token_transfers,
    fetch_all_base_nft_transfers,
    fetch_tx_logs,
)
from normalize import normalize_for_csv
from classify import classify_transactions
from csv_export import write_csv
import os
import json
import pandas as pd
from price_fetchers import build_events_from_base_csv
from pricing_logic import attach_prices_to_events, merge_events_back_to_base_csv

# CG - wdNzZX6qnger7EZJQtRmgqkP
filename = f"csv/base_activity.csv"
LOG_CACHE_PATH = "csv/log_cache.jsonl"


def load_log_cache(path: str) -> dict:
    cache = {}
    if not os.path.exists(path):
        return cache
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    h = obj.get("hash")
                    logs = obj.get("logs", [])
                    if h:
                        cache[h] = logs
                except Exception:
                    continue
    except Exception:
        return {}
    return cache


def write_log_cache(path: str, cache: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            for h, logs in cache.items():
                json.dump({"hash": h, "logs": logs}, f)
                f.write("\n")
    except Exception as e:
        print(f"[logs] Failed to write cache: {e}")


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
    nft_txs = fetch_all_base_nft_transfers(address)

    rows = normalize_for_csv(address, native_txs, token_txs, nft_txs)

    # Pass 1: classify without logs (fast)
    classify_transactions(rows, logs_by_hash={})

    # Determine which tx hashes need logs (to refine OTHER/lock/vote/approval)
    candidate_hashes = set()
    for r in rows:
        h = r.get("hash")
        if not h:
            continue
        candidate_hashes.add(h)
        # if r.get("activity_type") == "OTHER":
        #     candidate_hashes.add(h)
        # to_addr = (r.get("to") or "").lower()
        # if to_addr in LOCK_CONTRACTS or to_addr in LOCK_VOTE_CONTRACTS:
        #     candidate_hashes.add(h)
        # if to_addr in APPROVAL_CONTRACT_HINTS or to_addr in VOTE_CONTRACT_HINTS:
        #     candidate_hashes.add(h)

    print(f"[logs] Candidate tx hashes for logs: {len(candidate_hashes)}")

    # Collect logs only for candidates; reuse cache to avoid re-fetching
    import time

    logs_cache = load_log_cache(LOG_CACHE_PATH)
    logs_by_hash = dict(logs_cache)
    missing_hashes = [h for h in candidate_hashes if h not in logs_cache]
    total_missing = len(missing_hashes)

    print(f"[logs] Cache size: {len(logs_cache)} | missing to fetch: {total_missing}")

    for idx, h in enumerate(sorted(missing_hashes), start=1):
        if idx % 25 == 0 or idx == total_missing:
            print(f"[logs] Fetching {idx}/{total_missing} (hash {h})")
        try:
            logs_by_hash[h] = fetch_tx_logs(h)
        except Exception as e:
            print(f"[logs] Failed for {h}: {e}")
            logs_by_hash[h] = []
        time.sleep(0.1)  # gentle rate limit

    # Persist cache with any newly fetched logs
    write_log_cache(LOG_CACHE_PATH, logs_by_hash)

    # Pass 2: re-classify with logs for candidates
    classify_transactions(rows, logs_by_hash=logs_by_hash)

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
    # if os.path.exists(filename):
    #     enrich()
    # else:
    main()
