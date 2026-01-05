from __future__ import annotations

import random
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import Response
from requests.exceptions import (
    ReadTimeout,
    ConnectionError as ReqConnectionError,
    HTTPError,
    RequestException,
)

from .config import RATE_LIMIT_MARKERS, TIMEOUT_MARKERS

NO_RECORD_MARKERS = ("no records found", "no transactions found")


class EtherscanV2:
    def __init__(
        self,
        api_key: str,
        chainid: str | int,
        *,
        base_url: str = "https://api.etherscan.io/v2/api",
        timeout: Tuple[float, float] = (10.0, 60.0),  # (connect, read)
        max_retries: int = 8,
        backoff_base_s: float = 0.8,
    ) -> None:
        self.api_key = api_key
        self.chainid = str(chainid)
        self.base_url = base_url
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
                    self.base_url, params=params, timeout=self.timeout
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
                    result = data.get("result")

                    if status == "0":
                        msg = f"{message} {result}"
                        msg_l = msg.lower()

                        if any(x in msg_l for x in NO_RECORD_MARKERS):
                            return data

                        if self._is_transient_message(msg):
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
                self._sleep_backoff(attempt)

        raise RuntimeError(f"Failed after retries: {last_err}")

    def _get_list_result(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = self._get(params)
        status = str(data.get("status", ""))
        result = data.get("result")

        if status == "0":
            return []
        if status != "1":
            raise RuntimeError(f"Unexpected Etherscan response: {data}")
        if not isinstance(result, list):
            raise RuntimeError(f"Expected list in result, got: {type(result)}")

        return result

    def _fetch_all_pages(
        self,
        *,
        label: str,
        fetch_page,
        offset: int = 1000,
        sleep_s: float = 0.2,
    ) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        page = 1

        while True:
            print(f"[{label}] Fetching page {page}...")
            rows = fetch_page(page=page, offset=offset)

            if not rows:
                break

            all_rows.extend(rows)
            if len(rows) < offset:
                break

            page += 1
            time.sleep(sleep_s)

        print(f"[{label}] Total fetched: {len(all_rows)}")
        return all_rows

    def get_logs(
        self,
        address: str,
        from_block: int,
        to_block: int,
        topic0_hex: str,
        page: int = 1,
        offset: int = 1000,
    ) -> List[Dict[str, Any]]:
        return self._get_list_result(
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

    def get_account_txs(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 9_999_999_999,
        sort: str = "asc",
        page: int = 1,
        offset: int = 1000,
    ) -> List[Dict[str, Any]]:
        return self._get_list_result(
            {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": sort,
            }
        )

    def get_token_transfers(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 9_999_999_999,
        sort: str = "asc",
        page: int = 1,
        offset: int = 1000,
    ) -> List[Dict[str, Any]]:
        return self._get_list_result(
            {
                "module": "account",
                "action": "tokentx",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": sort,
            }
        )

    def get_nft_transfers(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 9_999_999_999,
        sort: str = "asc",
        page: int = 1,
        offset: int = 1000,
    ) -> List[Dict[str, Any]]:
        return self._get_list_result(
            {
                "module": "account",
                "action": "tokennfttx",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": sort,
            }
        )

    def fetch_all_account_txs(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 9_999_999_999,
        sort: str = "asc",
        offset: int = 1000,
        sleep_s: float = 0.2,
    ) -> List[Dict[str, Any]]:
        return self._fetch_all_pages(
            label="base native",
            fetch_page=lambda page, offset: self.get_account_txs(
                address,
                start_block=start_block,
                end_block=end_block,
                sort=sort,
                page=page,
                offset=offset,
            ),
            offset=offset,
            sleep_s=sleep_s,
        )

    def fetch_all_token_transfers(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 9_999_999_999,
        sort: str = "asc",
        offset: int = 1000,
        sleep_s: float = 0.2,
    ) -> List[Dict[str, Any]]:
        return self._fetch_all_pages(
            label="base tokens",
            fetch_page=lambda page, offset: self.get_token_transfers(
                address,
                start_block=start_block,
                end_block=end_block,
                sort=sort,
                page=page,
                offset=offset,
            ),
            offset=offset,
            sleep_s=sleep_s,
        )

    def fetch_all_nft_transfers(
        self,
        address: str,
        *,
        start_block: int = 0,
        end_block: int = 9_999_999_999,
        sort: str = "asc",
        offset: int = 1000,
        sleep_s: float = 0.2,
    ) -> List[Dict[str, Any]]:
        return self._fetch_all_pages(
            label="base nfts",
            fetch_page=lambda page, offset: self.get_nft_transfers(
                address,
                start_block=start_block,
                end_block=end_block,
                sort=sort,
                page=page,
                offset=offset,
            ),
            offset=offset,
            sleep_s=sleep_s,
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

    def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        data = self._get(
            {
                "module": "proxy",
                "action": "eth_getTransactionReceipt",
                "txhash": tx_hash,
            }
        )
        result = data.get("result")
        if not result or result == "null":
            raise RuntimeError(f"Etherscan proxy error for {tx_hash}: {data}")
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected receipt response: {data}")
        return result

    def fetch_tx_logs(self, tx_hash: str) -> List[Dict[str, Any]]:
        result = self.get_transaction_receipt(tx_hash)
        logs = result.get("logs", []) or []

        norm_logs: List[Dict[str, Any]] = []
        for log in logs:
            norm_logs.append(
                {
                    "address": log.get("address", ""),
                    "topics": log.get("topics", []),
                    "data": log.get("data", ""),
                    "event": log.get("event", ""),
                }
            )

        return norm_logs
