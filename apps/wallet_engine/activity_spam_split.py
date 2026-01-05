import argparse
import csv
import json
import os
from pathlib import Path
from typing import Dict, Iterable, Set, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
WALLET_DATA_DIR = ROOT_DIR / "apps" / "shared" / "data" / "wallet"
WALLET_ACTIVITY_CSV_PATH = str(WALLET_DATA_DIR / "base_activity.csv")
WALLET_SPAM_TOKENS_PATH = str(WALLET_DATA_DIR / "spam_tokens.json")
WALLET_SPAM_TX_HASHES_PATH = str(WALLET_DATA_DIR / "spam_tx_hashes.json")


def _norm_addr(addr: str) -> str:
    return (addr or "").strip().lower()


def _load_spam_tokens(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {_norm_addr(x) for x in data if x}
        if isinstance(data, dict):
            tokens = data.get("tokens", data)
            if isinstance(tokens, list):
                return {_norm_addr(x) for x in tokens if x}
            if isinstance(tokens, dict):
                return {_norm_addr(x) for x in tokens.keys() if x}
    except Exception:
        return set()
    return set()


def _default_output_paths(input_path: str) -> Tuple[str, str]:
    base, ext = os.path.splitext(input_path)
    if not ext:
        ext = ".csv"
    return f"{base}_clean{ext}", f"{base}_spam{ext}"


def _load_spam_tx_hashes(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x).strip() for x in data if x}
        return set()
    except Exception:
        return set()


def split_activity_csv(
    input_path: str,
    *,
    output_clean_path: str | None = None,
    output_spam_path: str | None = None,
    spam_tokens_path: str = WALLET_SPAM_TOKENS_PATH,
    spam_tx_hashes_path: str = WALLET_SPAM_TX_HASHES_PATH,
) -> Dict[str, int]:
    spam_tokens = _load_spam_tokens(spam_tokens_path)
    spam_tx_hashes = _load_spam_tx_hashes(spam_tx_hashes_path)
    if output_clean_path is None or output_spam_path is None:
        output_clean_path, output_spam_path = _default_output_paths(input_path)

    counts = {"total": 0, "clean": 0, "spam": 0}

    with open(input_path, "r", encoding="utf-8", newline="") as f_in:
        with open(output_clean_path, "w", encoding="utf-8", newline="") as f_clean:
            with open(output_spam_path, "w", encoding="utf-8", newline="") as f_spam:
                reader = csv.DictReader(f_in)
                fieldnames = reader.fieldnames or []
                clean_writer = csv.DictWriter(f_clean, fieldnames=fieldnames)
                spam_writer = csv.DictWriter(f_spam, fieldnames=fieldnames)
                clean_writer.writeheader()
                spam_writer.writeheader()

                for row in reader:
                    counts["total"] += 1
                    token_contract = _norm_addr(row.get("token_contract", ""))
                    row_hash = (row.get("hash") or "").strip()
                    is_spam = False
                    if token_contract and token_contract in spam_tokens:
                        is_spam = True
                    elif row_hash and row_hash in spam_tx_hashes:
                        is_spam = True
                    if is_spam:
                        spam_writer.writerow(row)
                        counts["spam"] += 1
                    else:
                        clean_writer.writerow(row)
                        counts["clean"] += 1

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split base activity CSV into clean and spam CSVs."
    )
    parser.add_argument(
        "--input",
        default=WALLET_ACTIVITY_CSV_PATH,
        help="Input base activity CSV (default: wallet base_activity.csv)",
    )
    parser.add_argument("--output-clean", default=None)
    parser.add_argument("--output-spam", default=None)
    parser.add_argument(
        "--spam-tokens",
        default=WALLET_SPAM_TOKENS_PATH,
        help="Spam token registry JSON path",
    )
    parser.add_argument(
        "--spam-tx-hashes",
        default=WALLET_SPAM_TX_HASHES_PATH,
        help="Spam tx hash JSON path",
    )
    args = parser.parse_args()

    counts = split_activity_csv(
        args.input,
        output_clean_path=args.output_clean,
        output_spam_path=args.output_spam,
        spam_tokens_path=args.spam_tokens,
        spam_tx_hashes_path=args.spam_tx_hashes,
    )

    output_clean, output_spam = _default_output_paths(args.input)
    if args.output_clean:
        output_clean = args.output_clean
    if args.output_spam:
        output_spam = args.output_spam

    print(
        f"Wrote {counts['clean']} clean rows to {output_clean} | "
        f"{counts['spam']} spam rows to {output_spam} (total {counts['total']})"
    )


if __name__ == "__main__":
    main()
