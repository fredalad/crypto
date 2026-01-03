import argparse

from .pipeline import DEFAULT_LOG_CACHE_PATH, DEFAULT_OUTPUT_CSV, enrich_2025, run_export


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wallet transactions + tax engine pipeline."
    )
    parser.add_argument(
        "--address",
        type=str,
        default="",
        help="Override BASE_WALLET_ADDRESS from .env",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_CSV,
        help="Output CSV path for the wallet export",
    )
    parser.add_argument(
        "--log-cache",
        type=str,
        default=DEFAULT_LOG_CACHE_PATH,
        help="JSONL cache path for tx logs",
    )
    parser.add_argument(
        "--enrich-2025",
        action="store_true",
        help="Generate a 2025-only USD enriched CSV from the export",
    )
    args = parser.parse_args()

    if args.enrich_2025:
        enrich_2025(input_path=args.output)
        return

    run_export(
        address=args.address or None,
        output_path=args.output,
        log_cache_path=args.log_cache,
    )


if __name__ == "__main__":
    main()
