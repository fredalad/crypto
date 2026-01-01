#!/usr/bin/env python3
"""
Enrich Aerodrome pools CSV with human-readable pool names.

Input:  pools.csv (from the indexer)
Output: pools_named.csv (+ token_cache.json for reuse)

Adds columns:
- token0_symbol, token1_symbol
- token0_name, token1_name
- token0_decimals, token1_decimals
- pool_name  (e.g. vAMM-tBTC/USDbC, sAMM-USDC/DAI, CL500-WETH/USDbC)

Uses ONLY Etherscan v2 proxy eth_call for ERC-20 metadata (symbol/name/decimals).
Aggressively caches token metadata to avoid repeated calls.

Env:
  ETHERSCAN_API_KEY=...
Optional: loads .env if python-dotenv installed.
"""

from __future__ import annotations

import os
import csv
import json
import time
import random
import argparse
import logging
from typing import Any, Dict, Optional, Tuple

import requests
from requests import Response
from requests.exceptions import (
    ReadTimeout,
    ConnectionError as ReqConnectionError,
    HTTPError,
    RequestException,
)

try:
    from dotenv import load_dotenv  # optional

    load_dotenv()
except Exception:
    pass


# -----------------------------
# Config
# -----------------------------

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    raise RuntimeError("Missing ETHERSCAN_API_KEY env var")

ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
CHAINID_BASE = "8453"

# ERC-20 function selectors
SEL_NAME = "0x06fdde03"  # name()
SEL_SYMBOL = "0x95d89b41"  # symbol()
SEL_DECIMALS = "0x313ce567"  # decimals()

TIMEOUT_MARKERS = (
    "query timeout",
    "timeout occured",
    "timeout occurred",
    "server too busy",
)
RATE_LIMIT_MARKERS = ("rate limit", "max rate limit")

log = logging.getLogger("pool_namer")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-5s | %(message)s",
    )


# -----------------------------
# Etherscan client (retries + backoff)
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
                if r.status_code in (502, 503, 504, 520, 521, 522):
                    raise HTTPError(f"HTTP {r.status_code}: {r.text[:200]}", response=r)

                r.raise_for_status()
                data = r.json()

                # Etherscan-style
                if "status" in data:
                    status = str(data.get("status"))
                    message = str(data.get("message", ""))
                    if status == "0":
                        msg_l = message.lower()
                        if "no records found" in msg_l:
                            return data
                        if self._is_transient_message(message):
                            raise RuntimeError(f"Etherscan transient error: {data}")
                        raise RuntimeError(f"Etherscan error: {data}")

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
                    "HTTP fail (attempt %d/%d): %s | %s",
                    attempt + 1,
                    self.max_retries,
                    repr(ex),
                    {k: params.get(k) for k in ("module", "action")},
                )
                self._sleep_backoff(attempt)

        raise RuntimeError(f"Failed after retries: {last_err}")

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
# ABI decode helpers
# -----------------------------


def _hex_to_bytes(hex_str: str) -> bytes:
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    return bytes.fromhex(hex_str) if hex_str else b""


def _decode_abi_string_or_bytes32(result_hex: str) -> Optional[str]:
    b = _hex_to_bytes(result_hex)
    if not b:
        return None

    # bytes32
    if len(b) == 32:
        try:
            s = b.rstrip(b"\x00").decode("utf-8", errors="strict").strip()
            return s or None
        except Exception:
            return None

    # dynamic string: [offset][len][data]
    if len(b) >= 64:
        try:
            offset = int.from_bytes(b[0:32], "big")
            if offset + 32 > len(b):
                return None
            strlen = int.from_bytes(b[offset : offset + 32], "big")
            start = offset + 32
            end = start + strlen
            if end > len(b):
                return None
            s = b[start:end].decode("utf-8", errors="strict").strip()
            return s or None
        except Exception:
            return None
    return None


def _decode_uint256(result_hex: str) -> Optional[int]:
    b = _hex_to_bytes(result_hex)
    if len(b) < 32:
        return None
    return int.from_bytes(b[-32:], "big")


# -----------------------------
# Token metadata fetch (cached)
# -----------------------------


def get_token_meta(
    client: EtherscanV2, token: str, cache: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    key = (token or "").lower()
    if not key:
        return {"symbol": None, "name": None, "decimals": None}

    if key in cache and cache[key].get("symbol"):
        return cache[key]

    symbol = None
    name = None
    decimals = None

    # symbol()
    try:
        out = client.eth_call(token, SEL_SYMBOL)
        symbol = _decode_abi_string_or_bytes32(out)
    except Exception:
        symbol = None

    # name()
    try:
        out = client.eth_call(token, SEL_NAME)
        name = _decode_abi_string_or_bytes32(out)
    except Exception:
        name = None

    # decimals()
    try:
        out = client.eth_call(token, SEL_DECIMALS)
        decimals = _decode_uint256(out)
    except Exception:
        decimals = None

    if not symbol:
        symbol = f"TKN_{token[-4:].upper()}"

    cache[key] = {"symbol": symbol, "name": name, "decimals": decimals}
    return cache[key]


# -----------------------------
# Pool naming
# -----------------------------


def make_pool_name(row: Dict[str, str], sym0: str, sym1: str) -> str:
    pool_type = (row.get("pool_type") or "").lower()

    if pool_type == "vamm":
        stable = str(row.get("stable") or "").lower() in ("true", "1", "yes")
        prefix = "sAMM" if stable else "vAMM"
        return f"{prefix}-{sym0}/{sym1}"

    if pool_type == "cl":
        fee = (row.get("fee") or "").strip()
        if fee.isdigit():
            return f"CL{fee}-{sym0}/{sym1}"
        return f"CL-{sym0}/{sym1}"

    return f"POOL-{sym0}/{sym1}"


# -----------------------------
# Cache IO
# -----------------------------


def load_cache(path: str) -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(path: str, cache: Dict[str, Dict[str, Any]]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


# -----------------------------
# Main
# -----------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Attach human-readable Aerodrome pool names to pools.csv (Etherscan-only)."
    )
    ap.add_argument(
        "--in",
        dest="in_csv",
        default="pools.csv",
        help="input pools csv (from indexer)",
    )
    ap.add_argument(
        "--out", dest="out_csv", default="pools_named.csv", help="output enriched csv"
    )
    ap.add_argument(
        "--cache", default="token_cache.json", help="token metadata cache file"
    )
    ap.add_argument("--log-level", default="INFO", help="INFO or DEBUG")
    ap.add_argument(
        "--http-timeout",
        type=float,
        default=60.0,
        help="Etherscan read timeout seconds",
    )
    ap.add_argument("--http-retries", type=int, default=8, help="Etherscan retries")
    ap.add_argument(
        "--sleep", type=float, default=0.05, help="sleep between new token lookups"
    )
    ap.add_argument(
        "--save-every", type=int, default=250, help="save cache every N pools"
    )
    args = ap.parse_args()

    setup_logging(args.log_level)

    client = EtherscanV2(
        ETHERSCAN_API_KEY,
        CHAINID_BASE,
        timeout=(10.0, float(args.http_timeout)),
        max_retries=int(args.http_retries),
    )

    cache = load_cache(args.cache)
    log.info("Loaded token cache: %d entries (%s)", len(cache), args.cache)

    with open(args.in_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        in_fields = reader.fieldnames or []

    if not rows:
        raise RuntimeError(f"No rows found in {args.in_csv}")

    log.info("Read %d pools from %s", len(rows), args.in_csv)

    # Output columns: preserve originals + new columns
    add_fields = [
        "token0_symbol",
        "token1_symbol",
        "token0_name",
        "token1_name",
        "token0_decimals",
        "token1_decimals",
        "pool_name",
    ]
    fieldnames = list(in_fields)
    for f in add_fields:
        if f not in fieldnames:
            fieldnames.append(f)

    # Enrich
    for i, row in enumerate(rows, start=1):
        token0 = row.get("token0", "")
        token1 = row.get("token1", "")

        m0 = get_token_meta(client, token0, cache)
        time.sleep(args.sleep)
        m1 = get_token_meta(client, token1, cache)
        time.sleep(args.sleep)

        row["token0_symbol"] = m0.get("symbol") or ""
        row["token1_symbol"] = m1.get("symbol") or ""
        row["token0_name"] = m0.get("name") or ""
        row["token1_name"] = m1.get("name") or ""
        row["token0_decimals"] = (
            "" if m0.get("decimals") is None else str(m0["decimals"])
        )
        row["token1_decimals"] = (
            "" if m1.get("decimals") is None else str(m1["decimals"])
        )

        row["pool_name"] = make_pool_name(
            row, row["token0_symbol"], row["token1_symbol"]
        )

        if i % 100 == 0:
            log.info("Progress: %d/%d pools", i, len(rows))

        # periodically persist cache so reruns resume quickly
        if i % args.save_every == 0:
            save_cache(args.cache, cache)
            log.info("Saved cache (%d entries) at pool %d", len(cache), i)

    save_cache(args.cache, cache)
    log.info("Saved final cache: %d entries", len(cache))

    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    log.info("Wrote enriched CSV: %s", args.out_csv)


if __name__ == "__main__":
    main()
