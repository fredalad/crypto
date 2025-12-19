import json
import os
from typing import Optional

import pandas as pd

from config import BASE_WALLET_ADDRESS
from etherscan_api import (
    fetch_all_base_native_txs,
    fetch_all_base_token_transfers,
)
from normalize import normalize_for_csv
from classify import classify_transactions
from csv_export import write_csv
from pricing_logic import enrich_yearly_csv
from price_fetchers import (
    fetch_erc20_prices_on_base,
    fetch_native_eth_prices,
    ensure_base_datetime_columns,
    search_coingecko_token_contract,
)

# CG - wdNzZX6qnger7EZJQtRmgqkP
ACTIVITY_CSV = "csv/base_activity.csv"
YEAR_TO_ENRICH = 2025


def export_base_activity(address: str, output_path: str = ACTIVITY_CSV) -> None:
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
    classify_transactions(rows)

    # Attempt to enrich Base contract from CoinGecko (per unique token)
    unique_tokens = {}
    for r in rows:
        if r["tx_type"] != "base_token_transfer":
            continue
        sym = (r.get("token_symbol") or "").strip()
        name = (r.get("token_name") or "").strip()
        if not sym and not name:
            continue
        key = (sym.lower(), name.lower())
        if key not in unique_tokens:
            unique_tokens[key] = {"symbol": sym, "name": name}

    token_contract_map = {}
    for key, entry in unique_tokens.items():
        sym = entry["symbol"]
        name = entry["name"]
        cg_addr = search_coingecko_token_contract(sym, name)
        token_contract_map[key] = cg_addr

    for r in rows:
        if r["tx_type"] == "base_token_transfer":
            sym = (r.get("token_symbol") or "").strip().lower()
            name = (r.get("token_name") or "").strip().lower()
            r["cg_base_contract"] = token_contract_map.get((sym, name), "")
        else:
            r["cg_base_contract"] = r.get("cg_base_contract") or "NATIVE"

    write_csv(output_path, rows)
    print(f"Wrote {len(rows)} rows to {output_path}")


def enrich_base_activity(
    input_path: str = ACTIVITY_CSV,
    year: int = YEAR_TO_ENRICH,
) -> None:
    output_path = f"csv/base_{year}_with_usd.csv"
    enrich_yearly_csv(input_path, output_path, year=year)
    print(f"Done - {year}-only enriched file created at {output_path}.")


def find_missing_usd_rows(
    input_path: str,
    output_path: Optional[str] = None,
    token_json_path: Optional[str] = None,
) -> None:
    """
    Locate rows missing USD pricing columns and optionally write them out.
    """
    print(f"Loading enriched CSV: {input_path}")
    df = pd.read_csv(input_path)

    if "price_usd" not in df.columns or "value_usd" not in df.columns:
        raise RuntimeError("price_usd/value_usd columns not found in the CSV.")

    missing_mask = df["price_usd"].isna() | (df["price_usd"] == "")
    missing_df = df[missing_mask]
    print(f"Rows missing USD price: {len(missing_df)}")

    # Gather token name/symbol summary for missing rows
    token_pairs = (
        missing_df[["token_symbol", "token_name"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(by=["token_symbol", "token_name"])
    )
    token_list = [
        {"token_symbol": row["token_symbol"], "token_name": row["token_name"]}
        for _, row in token_pairs.iterrows()
    ]

    # Write CSV subset if any rows are missing
    if not missing_df.empty:
        output_path = output_path or input_path.replace(".csv", "_missing_usd.csv")
        missing_df.to_csv(output_path, index=False)
        print(f"Wrote missing USD rows to {output_path}")

    # Always write the token summary JSON for clarity
    token_json_path = token_json_path or input_path.replace(
        ".csv", "_missing_usd_tokens.json"
    )
    with open(token_json_path, "w", encoding="utf-8") as f:
        json.dump(token_list, f, ensure_ascii=False, indent=2)
    print(f"Wrote missing USD token summary to {token_json_path}")


def investigate_tokens_without_usd(
    inv_path: str,
    enriched_csv_path: str,
    output_path: Optional[str] = None,
) -> None:
    """
    Cross-check an investigation list (inv.json) against the enriched CSV
    to see which tokens are missing USD prices.
    """
    print(f"Loading investigation list: {inv_path}")
    with open(inv_path, "r", encoding="utf-8") as f:
        inv_tokens = json.load(f)

    print(f"Loading enriched CSV: {enriched_csv_path}")
    df = pd.read_csv(enriched_csv_path)
    df = ensure_base_datetime_columns(df)

    if "price_usd" not in df.columns or "value_usd" not in df.columns:
        raise RuntimeError("price_usd/value_usd columns not found in the CSV.")

    df["symbol_norm"] = df["token_symbol"].fillna("").str.lower()
    df["name_norm"] = df["token_name"].fillna("").str.lower()

    def norm(val: str) -> str:
        return (val or "").strip().lower()

    report = []

    price_cache = {}
    success_hashes = []
    failed_tokens = []

    for entry in inv_tokens:
        sym_norm = norm(entry.get("token_symbol", ""))
        name_norm = norm(entry.get("token_name", ""))

        matches = df[
            (df["symbol_norm"] == sym_norm)
            | ((name_norm != "") & (df["name_norm"] == name_norm))
        ]

        missing_mask = matches["price_usd"].isna() | (matches["price_usd"] == "")
        missing_count = int(missing_mask.sum())
        priced_count = int(len(matches) - missing_count)

        sample_missing_hashes = (
            matches.loc[missing_mask, "hash"].dropna().head(5).tolist()
        )

        repriced_hits = []
        success_hash = None

        # Attempt to re-query CoinGecko for missing hashes for this token
        if missing_count > 0:
            print(
                f"Re-querying CoinGecko for {entry.get('token_symbol')} missing rows..."
            )
            for _, row in matches.loc[missing_mask].iterrows():
                token_sym = row.get("token_symbol") or ""
                token_contract = row.get("token_contract") or ""
                ts = row.get("timestamp_sec")
                tx_hash = row.get("hash")

                if pd.isna(ts):
                    continue

                token_key = (token_sym.lower(), token_contract.lower())
                price_df = price_cache.get(token_key)

                if price_df is None:
                    # +/- 6 hours around the event timestamp
                    ts_from = int(ts) - 6 * 3600
                    ts_to = int(ts) + 6 * 3600
                    try:
                        if token_contract == "NATIVE" or token_sym == "ETH_BASE":
                            price_df = fetch_native_eth_prices(ts_from, ts_to)
                        else:
                            price_df = fetch_erc20_prices_on_base(
                                token_contract, ts_from, ts_to
                            )
                    except Exception as exc:
                        print(f"  Price fetch error for {tx_hash}: {exc}")
                        price_df = pd.DataFrame()

                    price_cache[token_key] = price_df

                if price_df is not None and not price_df.empty:
                    repriced_hits.append(tx_hash)
                    success_hash = success_hash or tx_hash
                    # Only need one successful hash per token
                    break

        report.append(
            {
                "token_symbol": entry.get("token_symbol"),
                "token_name": entry.get("token_name"),
                "total_rows": int(len(matches)),
                "rows_missing_usd": missing_count,
                "rows_with_usd": priced_count,
                "sample_missing_hashes": sample_missing_hashes,
                "repriced_hashes_found": repriced_hits,
            }
        )

        if missing_count > 0:
            if success_hash:
                success_hashes.append(
                    {
                        "token_symbol": entry.get("token_symbol"),
                        "token_name": entry.get("token_name"),
                        "hash": success_hash,
                    }
                )
            else:
                failed_tokens.append(
                    {
                        "token_symbol": entry.get("token_symbol"),
                        "token_name": entry.get("token_name"),
                    }
                )

    output_path = output_path or enriched_csv_path.replace(
        ".csv", "_investigation_report.json"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Wrote investigation report to {output_path}")
    if success_hashes:
        print("One successful hash per token (for unique_successful_hash):")
        print(json.dumps(success_hashes, indent=2))
    if failed_tokens:
        print("Tokens still missing USD prices after re-query:")
        print(json.dumps(failed_tokens, indent=2))


def prompt_with_default(prompt: str, default: str) -> str:
    entered = input(f"{prompt} [{default}]: ").strip()
    return entered or default


if __name__ == "__main__":
    print("Select action:")
    print("1) Export Base activity")
    print("2) Enrich CSV with USD pricing")
    print("3) Find rows missing USD pricing in enriched CSV")
    print("4) Investigate specific tokens (inv.json) for missing USD pricing")
    choice = input("Enter choice (1/2/3/4): ").strip()

    if choice == "1":
        address = prompt_with_default("Wallet address", BASE_WALLET_ADDRESS or "")
        export_base_activity(address, ACTIVITY_CSV)
    elif choice == "2":
        input_path = prompt_with_default("Input CSV path", ACTIVITY_CSV)
        year_raw = prompt_with_default("Year to enrich", str(YEAR_TO_ENRICH))
        year = int(year_raw)
        enrich_base_activity(input_path, year)
    elif choice == "3":
        default_enriched = f"csv/base_{YEAR_TO_ENRICH}_with_usd.csv"
        enriched_path = prompt_with_default(
            "Enriched CSV path (from step 2)", default_enriched
        )
        find_missing_usd_rows(enriched_path)
    elif choice == "4":
        default_enriched = f"csv/base_{YEAR_TO_ENRICH}_with_usd.csv"
        inv_path = prompt_with_default("Investigation JSON path", "inv.json")
        enriched_path = prompt_with_default(
            "Enriched CSV path (from step 2)", default_enriched
        )
        investigate_tokens_without_usd(inv_path, enriched_path)
    else:
        print("Invalid choice. Exiting.")
