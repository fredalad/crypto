from apps.wallet_engine import pipeline


def main() -> None:
    pipeline.run_export()


def enrich() -> None:
    pipeline.enrich()


if __name__ == "__main__":
    main()
