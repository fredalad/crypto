import csv
import json
import os
from eth_abi import decode

SWAP_TOPIC0_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_TOPIC0_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def load_cache(path="csv/log_cache.jsonl"):
    cache = {}
    if not os.path.exists(path):
        return cache
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            h = obj.get("hash")
            logs = obj.get("logs", [])
            if h:
                cache[h] = logs
    return cache


def is_swap_log(log):
    topics = log.get("topics") or []
    if not topics:
        return False
    topic0 = topics[0].lower()
    return topic0 in {SWAP_TOPIC0_V2, SWAP_TOPIC0_V3} or any(
        (t or "").lower() in {SWAP_TOPIC0_V2, SWAP_TOPIC0_V3} for t in topics
    )


def decode_swap(log):
    try:
        amount0_in, amount1_in, amount0_out, amount1_out = decode(
            ["uint256", "uint256", "uint256", "uint256"], bytes.fromhex(log["data"][2:])
        )
        return {
            "amount0_in": amount0_in,
            "amount1_in": amount1_in,
            "amount0_out": amount0_out,
            "amount1_out": amount1_out,
        }
    except Exception:
        return None


def main():
    # Read first 100 rows from CSV and collect hashes
    hashes = []
    with open("csv/base_activity.csv", newline="") as f:
        for i, row in enumerate(csv.DictReader(f)):
            hashes.append(row["hash"])
            if i >= 99:
                break

    hashes = list(dict.fromkeys(hashes))  # de-dupe preserving order
    cache = load_cache()

    print(f"Analyzing {len(hashes)} tx hashes (from first 100 CSV rows)")

    for h in hashes:
        logs = cache.get(h, [])
        swap_logs = [log for log in logs if is_swap_log(log)]
        if not swap_logs:
            continue
        print(h)


if __name__ == "__main__":
    main()
