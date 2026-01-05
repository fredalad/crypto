import time
from typing import Any, Dict, Iterator, List, Tuple

from ...shared.config import RATE_LIMIT_MARKERS, TIMEOUT_MARKERS
from ...shared.etherscan_v2 import EtherscanV2
from .logging_utils import log


class LogPaginator:
    def __init__(
        self,
        client: EtherscanV2,
        address: str,
        topic0_hex: str,
        *,
        label: str,
        sleep_s: float = 0.21,
        offset: int = 1000,
        min_span: int = 1_000,
    ) -> None:
        self.client = client
        self.address = address
        self.topic0_hex = topic0_hex
        self.label = label
        self.sleep_s = sleep_s
        self.offset = offset
        self.min_span = min_span

    def _looks_like_timeout(self, msg: str) -> bool:
        m = (msg or "").lower()
        return any(x in m for x in TIMEOUT_MARKERS)

    def _looks_like_rate_limit(self, msg: str) -> bool:
        m = (msg or "").lower()
        return any(x in m for x in RATE_LIMIT_MARKERS)

    def iter_range(self, from_block: int, to_block: int) -> Iterator[Dict[str, Any]]:
        stack: List[Tuple[int, int]] = [(from_block, to_block)]

        while stack:
            a, b = stack.pop()
            log.info("%s | getLogs range %d..%d", self.label, a, b)

            page = 1
            while True:
                try:
                    logs_list = self.client.get_logs(
                        address=self.address,
                        from_block=a,
                        to_block=b,
                        topic0_hex=self.topic0_hex,
                        page=page,
                        offset=self.offset,
                    )
                except RuntimeError as ex:
                    msg = str(ex)

                    if self._looks_like_rate_limit(msg):
                        log.warning(
                            "%s | rate-limited on page %d; sleeping", self.label, page
                        )
                        time.sleep(1.5)
                        continue

                    if self._looks_like_timeout(msg):
                        span = b - a
                        if span <= self.min_span:
                            log.error(
                                "%s | cannot split further (span=%d). Raising.",
                                self.label,
                                span,
                            )
                            raise
                        mid = (a + b) // 2
                        log.warning(
                            "%s | Query timeout -> split %d..%d into %d..%d and %d..%d",
                            self.label,
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

                if not logs_list:
                    log.debug(
                        "%s | page=%d -> 0 logs (done for this range)", self.label, page
                    )
                    break

                log.debug(
                    "%s | page=%d -> %d logs", self.label, page, len(logs_list)
                )
                for row in logs_list:
                    yield row

                if len(logs_list) < self.offset:
                    break

                page += 1
                time.sleep(self.sleep_s)

            time.sleep(self.sleep_s)
