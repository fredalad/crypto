import argparse
from dataclasses import asdict
from typing import Any, Dict, List

from ...shared.config import (
    AERO_CL_FACTORY,
    AERO_POOL_FACTORY_VAMM,
    CHAIN_ID_BASE,
    ETHERSCAN_API_KEY,
    ETHERSCAN_V2_URL,
    PROTOCOL_POOLS_OUT_PREFIX,
    require_api_key,
)
from ...shared.etherscan_v2 import EtherscanV2
from ...shared.csv_utils import write_csv_rows, write_jsonl
from .indexer import AerodromePoolIndexer, dedupe_rows
from .logging_utils import log, setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Index Aerodrome vAMM/sAMM + SlipStream CL pools on Base via Etherscan v2 only."
    )
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="start block (0 = auto from factory creation)",
    )
    parser.add_argument("--end", type=int, default=0, help="end block (0 = latest)")
    parser.add_argument(
        "--step", type=int, default=50_000, help="block step (default 50,000)"
    )
    parser.add_argument(
        "--min-span",
        type=int,
        default=1_000,
        help="min split span when Etherscan times out",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=PROTOCOL_POOLS_OUT_PREFIX,
        help="output prefix (default shared protocol data dir)",
    )
    parser.add_argument("--log-level", type=str, default="INFO", help="INFO or DEBUG")
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=60.0,
        help="read timeout seconds (connect fixed at 10s)",
    )
    parser.add_argument("--http-retries", type=int, default=8, help="max HTTP retries")
    args = parser.parse_args()

    setup_logging(args.log_level)

    require_api_key()
    client = EtherscanV2(
        ETHERSCAN_API_KEY,
        CHAIN_ID_BASE,
        base_url=ETHERSCAN_V2_URL,
        timeout=(10.0, float(args.http_timeout)),
        max_retries=int(args.http_retries),
    )
    indexer = AerodromePoolIndexer(client)

    # start block
    if args.start and args.start > 0:
        start_block = args.start
        log.info("Using provided start_block=%d", start_block)
    else:
        c1 = indexer.get_creation_block(
            AERO_POOL_FACTORY_VAMM, "AERO_POOL_FACTORY_VAMM"
        )
        c2 = indexer.get_creation_block(AERO_CL_FACTORY, "AERO_CL_FACTORY")
        start_block = min(c1, c2)
        log.info("Auto start_block=%d (min(factory creation blocks))", start_block)

    # end block
    if args.end and args.end > 0:
        end_block = args.end
        log.info("Using provided end_block=%d", end_block)
    else:
        log.info("Fetching latest block (eth_blockNumber)")
        end_block = client.eth_block_number()
        log.info("Auto end_block=%d (latest)", end_block)

    if end_block < start_block:
        raise RuntimeError(f"end_block ({end_block}) < start_block ({start_block})")

    log.info("Starting scan: %d..%d step=%d", start_block, end_block, args.step)

    vamm, cl = indexer.scan(
        start_block,
        end_block,
        step=args.step,
        min_span=args.min_span,
    )

    rows: List[Dict[str, Any]] = [asdict(p) for p in vamm] + [asdict(p) for p in cl]
    rows = dedupe_rows(rows)

    unique_pools = {r["pool"].lower() for r in rows if r.get("pool")}
    log.info(
        "Done. vAMM=%d CL=%d unique_pools=%d", len(vamm), len(cl), len(unique_pools)
    )

    jsonl_path = f"{args.out}.jsonl"
    csv_path = f"{args.out}.csv"
    write_jsonl(jsonl_path, rows)
    keys = sorted({k for r in rows for k in r.keys()})
    write_csv_rows(csv_path, rows, keys)

    log.info("Wrote %s", jsonl_path)
    log.info("Wrote %s", csv_path)
