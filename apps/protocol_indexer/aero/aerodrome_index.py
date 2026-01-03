#!/usr/bin/env python3
"""
Index ALL Aerodrome pools on Base using ONLY Etherscan v2:

- vAMM/sAMM pools from Aerodrome Pool Factory (PoolCreated with `stable` flag)
- SlipStream (CL) pools from CLFactory (PoolCreated with `tickSpacing`)
  + attaches `fee` by calling CLFactory.tickSpacingToFee(int24)

Outputs:
  - <out>.csv
  - <out>.jsonl

Env:
  ETHERSCAN_API_KEY=...
Optional:
  Put ETHERSCAN_API_KEY in a .env file; script will load it.
"""

from __future__ import annotations

import os
import csv
import json
import time
import random
import argparse
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterator, List, Tuple, Optional

import requests
from requests import Response
from requests.exceptions import (
    ReadTimeout,
    ConnectionError as ReqConnectionError,
    HTTPError,
    RequestException,
)
from eth_utils import keccak, to_checksum_address

try:
    from dotenv import load_dotenv  # optional

    load_dotenv()
except Exception:
    pass


# -----------------------------
# Config (Base)
# -----------------------------

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    raise RuntimeError("Missing ETHERSCAN_API_KEY env var")

ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
CHAINID_BASE = "8453"

# Aerodrome vAMM/sAMM Pool Factory
AERO_POOL_FACTORY_VAMM = "0x420dd381b31aef6683db6b902084cb0ffece40da"

# Aerodrome SlipStream CLFactory (creates CL pools)
AERO_CL_FACTORY = "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a"

# vAMM/sAMM event:
# event PoolCreated(address indexed token0, address indexed token1, bool indexed stable, address pool, uint256);
VAMM_POOLCREATED_SIG = "PoolCreated(address,address,bool,address,uint256)"

# SlipStream CL event (CLFactory):
# event PoolCreated(address indexed token0, address indexed token1, int24 indexed tickSpacing, address pool);
CL_POOLCREATED_SIG = "PoolCreated(address,address,int24,address)"

# tickSpacingToFee(int24)
TICKSPACING_TO_FEE_SELECTOR = "0x" + keccak(text="tickSpacingToFee(int24)").hex()[:8]


def topic0(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


VAMM_POOLCREATED_TOPIC0 = topic0(VAMM_POOLCREATED_SIG)
CL_POOLCREATED_TOPIC0 = topic0(CL_POOLCREATED_SIG)

TIMEOUT_MARKERS = (
    "query timeout",
    "timeout occured",
    "timeout occurred",
    "server too busy",
)
RATE_LIMIT_MARKERS = ("rate limit", "max rate limit")


# -----------------------------
# Logging
# -----------------------------

log = logging.getLogger("aero_indexer")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-5s | %(message)s",
    )


# -----------------------------
# Data models
# -----------------------------


@dataclass(frozen=True)
class VammPoolCreated:
    pool_type: str  # "vamm"
    pool: str
    token0: str
    token1: str
    stable: bool
    created_block: int
    tx_hash: str


@dataclass(frozen=True)
class CLPoolCreated:
    pool_type: str  # "cl"
    pool: str
    token0: str
    token1: str
    tick_spacing: int
    fee: int
    created_block: int
    tx_hash: str


# -----------------------------
# Etherscan v2 client (retries + backoff)
# -----------------------------


class EtherscanV2:
    def __init__(
        self,
        api_key: str,
        chainid: str = CHAINID_BASE,
        *,
        timeout: Tuple[float, float] = (10.0, 60.0),  # (connect, read)
        max_retries: int = 8,
        backoff_base_s: float = 0.8,
    ) -> None:
        self.api_key = api_key
        self.chainid = chainid
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.session = requests.Session()

    def _sleep_backoff(self, attempt: int) -> None:
        backoff = self.backoff_base_s * (2 ** min(attempt, 6))
        jitter = random.uniform(0.0, 0.25 * backoff)
        time.sleep(backoff + jitter)

    def _is_transient_message(self, msg: str) -> bool:
        m = (msg or "").lower()
        return (
            any(x in m for x in TIMEOUT_MARKERS)
            or any(x in m for x in RATE_LIMIT_MARKERS)
            or "busy" in m
        )

    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params = dict(params)
        params["apikey"] = self.api_key
        params["chainid"] = self.chainid

        last_err: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                r: Response = self.session.get(
                    ETHERSCAN_V2_URL, params=params, timeout=self.timeout
                )

                # Retry some 5xx
                if r.status_code in (502, 503, 504, 520, 521, 522):
                    raise HTTPError(f"HTTP {r.status_code}: {r.text[:200]}", response=r)

                r.raise_for_status()
                data = r.json()

                # Etherscan-style response
                if "status" in data:
                    status = str(data.get("status"))
                    message = str(data.get("message", ""))

                    if status == "0":
                        msg_l = message.lower()

                        # "No records found" is not an error for logs, etc.
                        if "no records found" in msg_l:
                            return data

                        # Retry on transient errors
                        if self._is_transient_message(message):
                            raise RuntimeError(f"Etherscan transient error: {data}")

                        raise RuntimeError(f"Etherscan error: {data}")

                # JSON-RPC proxy responses: {"jsonrpc":"2.0","id":...,"result":"0x..."}
                return data

            except (
                ReadTimeout,
                ReqConnectionError,
                HTTPError,
                RuntimeError,
                RequestException,
            ) as ex:
                last_err = ex
                if attempt == self.max_retries - 1:
                    raise
                log.warning(
                    "Etherscan request failed (attempt %d/%d): %s | %s",
                    attempt + 1,
                    self.max_retries,
                    repr(ex),
                    {k: params.get(k) for k in ("module", "action")},
                )
                self._sleep_backoff(attempt)

        raise RuntimeError(f"Failed after retries: {last_err}")

    def get_logs(
        self,
        address: str,
        from_block: int,
        to_block: int,
        topic0_hex: str,
        page: int = 1,
        offset: int = 1000,
    ) -> Dict[str, Any]:
        return self._get(
            {
                "module": "logs",
                "action": "getLogs",
                "address": address,
                "fromBlock": str(from_block),
                "toBlock": str(to_block),
                "topic0": topic0_hex,
                "page": str(page),
                "offset": str(offset),
            }
        )

    def get_contract_creation(self, contract_address: str) -> Dict[str, Any]:
        return self._get(
            {
                "module": "contract",
                "action": "getcontractcreation",
                "contractaddresses": contract_address,
            }
        )

    def eth_block_number(self) -> int:
        data = self._get({"module": "proxy", "action": "eth_blockNumber"})
        result = data.get("result")
        if not isinstance(result, str) or not result.startswith("0x"):
            raise RuntimeError(f"Unexpected eth_blockNumber response: {data}")
        return int(result, 16)

    def eth_call(self, to_addr: str, data_hex: str, tag: str = "latest") -> str:
        data = self._get(
            {
                "module": "proxy",
                "action": "eth_call",
                "to": to_addr,
                "data": data_hex,
                "tag": tag,
            }
        )
        result = data.get("result")
        if not isinstance(result, str) or not result.startswith("0x"):
            raise RuntimeError(f"Unexpected eth_call response: {data}")
        return result


# -----------------------------
# ABI + decoding helpers
# -----------------------------


def _hex_to_int(x: str) -> int:
    return int(x, 16)


def _decode_indexed_address(topic: str) -> str:
    return to_checksum_address("0x" + topic[-40:])


def _decode_bool_topic(topic: str) -> bool:
    return _hex_to_int(topic) != 0


def _decode_int24_topic(topic: str) -> int:
    """
    topic is 32-byte hex; int24 is stored in low 24 bits with sign.
    """
    raw = int(topic, 16)
    v = raw & ((1 << 24) - 1)
    if v & (1 << 23):
        v -= 1 << 24
    return v


def _encode_int24_as_int256(val: int) -> str:
    """
    ABI encodes an int24 as a 32-byte signed integer (two's complement).
    """
    if val < -(1 << 23) or val > (1 << 23) - 1:
        raise ValueError(f"int24 out of range: {val}")
    if val < 0:
        val = (1 << 256) + val
    return hex(val)[2:].rjust(64, "0")


def _decode_uint256(result_hex: str) -> int:
    return int(result_hex, 16)


def get_fee_for_tick_spacing(client: EtherscanV2, tick_spacing: int) -> int:
    """
    CLFactory.tickSpacingToFee(int24) -> uint24 fee (e.g. 100, 500, 3000, 10000).
    """
    calldata = TICKSPACING_TO_FEE_SELECTOR + _encode_int24_as_int256(tick_spacing)
    out = client.eth_call(AERO_CL_FACTORY, calldata)
    return _decode_uint256(out)


# -----------------------------
# Event decoders
# -----------------------------


def decode_vamm_poolcreated(log_row: Dict[str, Any]) -> VammPoolCreated:
    topics = log_row["topics"]
    token0 = _decode_indexed_address(topics[1])
    token1 = _decode_indexed_address(topics[2])
    stable = _decode_bool_topic(topics[3])

    # data = pool (32) + uint256 (32)
    data_bytes = bytes.fromhex(log_row["data"][2:])
    pool = to_checksum_address("0x" + data_bytes[12:32].hex())

    return VammPoolCreated(
        pool_type="vamm",
        pool=pool,
        token0=token0,
        token1=token1,
        stable=stable,
        created_block=_hex_to_int(log_row["blockNumber"]),
        tx_hash=log_row["transactionHash"],
    )


def decode_cl_poolcreated(
    log_row: Dict[str, Any],
    client: EtherscanV2,
    tick_fee_cache: Dict[int, int],
) -> CLPoolCreated:
    topics = log_row["topics"]
    token0 = _decode_indexed_address(topics[1])
    token1 = _decode_indexed_address(topics[2])
    tick_spacing = _decode_int24_topic(topics[3])

    # data = pool (single 32-byte word)
    data_bytes = bytes.fromhex(log_row["data"][2:])
    pool = to_checksum_address("0x" + data_bytes[12:32].hex())

    if tick_spacing not in tick_fee_cache:
        tick_fee_cache[tick_spacing] = get_fee_for_tick_spacing(client, tick_spacing)

    fee = tick_fee_cache[tick_spacing]

    return CLPoolCreated(
        pool_type="cl",
        pool=pool,
        token0=token0,
        token1=token1,
        tick_spacing=int(tick_spacing),
        fee=int(fee),
        created_block=_hex_to_int(log_row["blockNumber"]),
        tx_hash=log_row["transactionHash"],
    )


# -----------------------------
# Resilient getLogs iterator (pagination + split on "Query Timeout")
# -----------------------------


def iter_logs_paginated(
    client: EtherscanV2,
    address: str,
    from_block: int,
    to_block: int,
    topic0_hex: str,
    *,
    label: str,
    sleep_s: float = 0.21,
    offset: int = 1000,
    min_span: int = 1_000,
) -> Iterator[Dict[str, Any]]:
    def looks_like_timeout_msg(msg: str) -> bool:
        m = (msg or "").lower()
        return any(x in m for x in TIMEOUT_MARKERS)

    def looks_like_rate_limit_msg(msg: str) -> bool:
        m = (msg or "").lower()
        return any(x in m for x in RATE_LIMIT_MARKERS)

    stack: List[Tuple[int, int]] = [(from_block, to_block)]

    while stack:
        a, b = stack.pop()
        log.info("%s | getLogs range %d..%d", label, a, b)

        page = 1
        while True:
            try:
                data = client.get_logs(
                    address=address,
                    from_block=a,
                    to_block=b,
                    topic0_hex=topic0_hex,
                    page=page,
                    offset=offset,
                )
            except RuntimeError as ex:
                msg = str(ex)

                if looks_like_rate_limit_msg(msg):
                    log.warning("%s | rate-limited on page %d; sleeping", label, page)
                    time.sleep(1.5)
                    continue

                if looks_like_timeout_msg(msg):
                    span = b - a
                    if span <= min_span:
                        log.error(
                            "%s | cannot split further (span=%d). Raising.", label, span
                        )
                        raise
                    mid = (a + b) // 2
                    log.warning(
                        "%s | Query timeout -> split %d..%d into %d..%d and %d..%d",
                        label,
                        a,
                        b,
                        a,
                        mid,
                        mid + 1,
                        b,
                    )
                    stack.append((mid + 1, b))
                    stack.append((a, mid))
                    break

                raise

            logs_list = data.get("result") or []
            if not logs_list:
                log.debug("%s | page=%d -> 0 logs (done for this range)", label, page)
                break

            log.debug("%s | page=%d -> %d logs", label, page, len(logs_list))
            for row in logs_list:
                yield row

            if len(logs_list) < offset:
                break

            page += 1
            time.sleep(sleep_s)

        time.sleep(sleep_s)


# -----------------------------
# Scanning + output
# -----------------------------


def get_creation_block_number(
    client: EtherscanV2, contract_address: str, label: str
) -> int:
    log.info("Fetching creation block for %s (%s)", label, contract_address)
    data = client.get_contract_creation(contract_address)
    result = data.get("result") or []
    if not isinstance(result, list) or not result:
        raise RuntimeError(
            f"Could not get creation block for {contract_address}: {data}"
        )
    blk = int(result[0]["blockNumber"])
    log.info("Creation block for %s: %d", label, blk)
    return blk


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def scan_aerodrome_pools(
    client: EtherscanV2,
    start_block: int,
    end_block: int,
    *,
    step: int = 50_000,
    min_span: int = 1_000,
) -> Tuple[List[VammPoolCreated], List[CLPoolCreated]]:
    vamm_created: List[VammPoolCreated] = []
    cl_created: List[CLPoolCreated] = []
    tick_fee_cache: Dict[int, int] = {}

    total_ranges = ((end_block - start_block) // step) + 1
    range_i = 0

    for b0 in range(start_block, end_block + 1, step):
        b1 = min(b0 + step - 1, end_block)
        range_i += 1
        log.info("=== Range %d/%d | blocks %d..%d ===", range_i, total_ranges, b0, b1)

        # vAMM/sAMM pools
        v_before = len(vamm_created)
        for row in iter_logs_paginated(
            client,
            AERO_POOL_FACTORY_VAMM,
            b0,
            b1,
            VAMM_POOLCREATED_TOPIC0,
            label="VAMM_FACTORY",
            min_span=min_span,
        ):
            vamm_created.append(decode_vamm_poolcreated(row))
        log.info("VAMM_FACTORY | +%d pools in this range", len(vamm_created) - v_before)

        # SlipStream CL pools
        c_before = len(cl_created)
        for row in iter_logs_paginated(
            client,
            AERO_CL_FACTORY,
            b0,
            b1,
            CL_POOLCREATED_TOPIC0,
            label="CL_FACTORY",
            min_span=min_span,
        ):
            cl_created.append(decode_cl_poolcreated(row, client, tick_fee_cache))
        log.info("CL_FACTORY | +%d pools in this range", len(cl_created) - c_before)

        if tick_fee_cache:
            log.debug(
                "CL_FACTORY | tickSpacings cached: %s", sorted(tick_fee_cache.items())
            )

    return vamm_created, cl_created


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Dedupe by pool address (lowercased). Keeps first occurrence.
    """
    seen = set()
    out = []
    for r in rows:
        p = (r.get("pool") or "").lower()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(r)
    return out


# -----------------------------
# Main
# -----------------------------


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
        "--out", type=str, default="pools", help="output prefix (default 'pools')"
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

    client = EtherscanV2(
        ETHERSCAN_API_KEY,
        CHAINID_BASE,
        timeout=(10.0, float(args.http_timeout)),
        max_retries=int(args.http_retries),
    )

    # start block
    if args.start and args.start > 0:
        start_block = args.start
        log.info("Using provided start_block=%d", start_block)
    else:
        c1 = get_creation_block_number(
            client, AERO_POOL_FACTORY_VAMM, "AERO_POOL_FACTORY_VAMM"
        )
        c2 = get_creation_block_number(client, AERO_CL_FACTORY, "AERO_CL_FACTORY")
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

    vamm, cl = scan_aerodrome_pools(
        client,
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
    write_csv(csv_path, rows)

    log.info("Wrote %s", jsonl_path)
    log.info("Wrote %s", csv_path)


if __name__ == "__main__":
    main()
