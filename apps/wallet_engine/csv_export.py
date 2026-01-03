import csv
from collections import defaultdict
from typing import List, Dict, Any


def write_csv(filename: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No rows to write.")
        return

    fieldnames = [
        "hash",
        "tx_type",  # base_native | base_token_transfer
        "blockNumber",
        "timeStamp",
        "timeStamp_iso",
        # "from",
        # "to",
        "direction",
        "protocol",
        "activity_type",  # CLAIM_REWARD | LP_DEPOSIT | LP_WITHDRAW | SWAP | OTHER | SEND_NATIVE | RECEIVE_NATIVE
        "native_amount_eth",
        "token_contract",
        "token_symbol",
        "token_name",
        "token_decimals",
        "token_amount",
        "gas",
        "gasPrice",
        "gasUsed",
        "tx_fee_eth",
        "token_in_assets",  # aggregated per tx (symbol:amount | ...)
        "token_out_assets",  # aggregated per tx (symbol:amount | ...)
    ]

    # Collapse to one row per tx hash, keeping useful base/native metadata and
    # the aggregated asset summaries produced during classification.
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["hash"]].append(row)

    def safe_float(val: Any) -> float:
        try:
            return float(val)
        except Exception:
            return 0.0

    collapsed_rows: List[Dict[str, Any]] = []
    for tx_hash, tx_rows in grouped.items():
        # Prefer the base_native row for gas/fee/context; otherwise take first.
        base_row = next(
            (r for r in tx_rows if r["tx_type"] == "base_native"), tx_rows[0]
        )
        combined = {k: base_row.get(k, "") for k in fieldnames}

        # Clear single-asset fields to avoid implying only one token in the tx.
        combined.update(
            {
                "tx_type": "tx_grouped",
                "token_contract": "",
                "token_symbol": "",
                "token_name": "",
                "token_decimals": "",
                "token_amount": "",
            }
        )

        # Keep the already-built per-tx summaries (same on every row for the tx).
        combined["token_in_assets"] = tx_rows[0].get("token_in_assets", "")
        combined["token_out_assets"] = tx_rows[0].get("token_out_assets", "")

        # Sum transaction fees across all rows of the tx
        total_fee = sum(safe_float(r.get("tx_fee_eth", 0)) for r in tx_rows)
        combined["tx_fee_eth"] = total_fee if total_fee else ""

        collapsed_rows.append(combined)

    print(f"Writing {len(rows)} rows to {filename}")
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in collapsed_rows:
            writer.writerow(row)

    print(f"CSV written: {filename} ({len(collapsed_rows)} grouped rows)")
