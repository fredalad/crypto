import argparse

from apps.wallet_engine import pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        nargs="?",
        default="export",
        choices=("export", "enrich", "split-contracts"),
    )
    args = parser.parse_args()

    if args.command == "export":
        pipeline.run_export()
        return
    if args.command == "split-contracts":
        pipeline.split_base_activity_by_contract_address()
        return
    pipeline.enrich()


def enrich() -> None:
    pipeline.enrich()


if __name__ == "__main__":
    main()
