import csv
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
        "from",
        "to",
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
    ]

    print(f"Writing {len(rows)} rows to {filename}")
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"CSV written: {filename}")
