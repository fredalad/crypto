import json
import os
import time
from typing import Dict, List, Optional

import pandas as pd

from .classify import classify_transactions
from .config import (
    APPROVAL_CONTRACT_HINTS,
    BASE_WALLET_ADDRESS,
    LOCK_CONTRACTS,
    LOCK_VOTE_CONTRACTS,
    VOTE_CONTRACT_HINTS,
)
from .csv_export import write_csv
from .etherscan_api import (
    fetch_all_base_native_txs,
    fetch_all_base_nft_transfers,
    fetch_all_base_token_transfers,
    fetch_tx_logs,
)
from .normalize import normalize_for_csv
from .price_fetchers import build_events_from_base_csv
from .pricing_logic import attach_prices_to_events, merge_events_back_to_base_csv

DEFAULT_OUTPUT_CSV = "csv/base_activity.csv"
DEFAULT_LOG_CACHE_PATH = "csv/log_cache.jsonl"


def load_log_cache(path: str) -> Dict[str, List[Dict[str, str]]]:
    cache: Dict[str, List[Dict[str, str]]] = {}
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


def write_log_cache(path: str, cache: Dict[str, List[Dict[str, str]]]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            for h, logs in cache.items():
                json.dump({"hash": h, "logs": logs}, f)
                f.write("\n")
    except Exception as e:
        print(f"[logs] Failed to write cache: {e}")


def run_export(
    *,
    address: Optional[str] = None,
    output_path: str = DEFAULT_OUTPUT_CSV,
    log_cache_path: str = DEFAULT_LOG_CACHE_PATH,
) -> None:
    wallet_address = address or BASE_WALLET_ADDRESS
    if not wallet_address:
        raise RuntimeError(
            "Set BASE_WALLET_ADDRESS in .env to your Base wallet address "
            "(e.g. 0xabc...)"
        )

    wallet_address = wallet_address.strip()

    print(f"Exporting Base activity for address: {wallet_address}")

    native_txs = fetch_all_base_native_txs(wallet_address)
    token_txs = fetch_all_base_token_transfers(wallet_address)
    nft_txs = fetch_all_base_nft_transfers(wallet_address)

    rows = normalize_for_csv(wallet_address, native_txs, token_txs, nft_txs)

    # Pass 1: classify without logs (fast)
    classify_transactions(rows, logs_by_hash={})

    # Determine which tx hashes need logs (to refine OTHER/lock/vote/approval)
    candidate_hashes = set()
    for r in rows:
        h = r.get("hash")
        if not h:
            continue
        candidate_hashes.add(h)

    print(f"[logs] Candidate tx hashes for logs: {len(candidate_hashes)}")

    # Collect logs only for candidates; reuse cache to avoid re-fetching
    logs_cache = load_log_cache(log_cache_path)
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
    write_log_cache(log_cache_path, logs_by_hash)

    # Pass 2: re-classify with logs for candidates
    classify_transactions(rows, logs_by_hash=logs_by_hash)

    write_csv(output_path, rows)


def enrich_2025(
    *,
    input_path: str = DEFAULT_OUTPUT_CSV,
    output_filename: str = "base_2025",
) -> None:
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

    print("Done - 2025-only enriched file created.")


def enrich() -> None:
    enrich_2025()
