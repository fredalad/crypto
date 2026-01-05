from typing import Any, Dict, List, Tuple

from ...shared.config import (
    AERO_CL_FACTORY,
    AERO_POOL_FACTORY_VAMM,
    CL_POOLCREATED_TOPIC0,
    VAMM_POOLCREATED_TOPIC0,
)
from ...shared.etherscan_v2 import EtherscanV2
from .decoders import CLPoolDecoder, VammPoolDecoder
from .logging_utils import log
from .models import CLPoolCreated, VammPoolCreated
from .paginator import LogPaginator


class AerodromePoolIndexer:
    def __init__(self, client: EtherscanV2) -> None:
        self.client = client
        self.vamm_decoder = VammPoolDecoder()
        self.cl_decoder = CLPoolDecoder(client)

    def get_creation_block(self, contract_address: str, label: str) -> int:
        log.info("Fetching creation block for %s (%s)", label, contract_address)
        data = self.client.get_contract_creation(contract_address)
        result = data.get("result") or []
        if not isinstance(result, list) or not result:
            raise RuntimeError(
                f"Could not get creation block for {contract_address}: {data}"
            )
        blk = int(result[0]["blockNumber"])
        log.info("Creation block for %s: %d", label, blk)
        return blk

    def scan(
        self,
        start_block: int,
        end_block: int,
        *,
        step: int = 50_000,
        min_span: int = 1_000,
    ) -> Tuple[List[VammPoolCreated], List[CLPoolCreated]]:
        vamm_created: List[VammPoolCreated] = []
        cl_created: List[CLPoolCreated] = []

        total_ranges = ((end_block - start_block) // step) + 1
        range_i = 0

        vamm_pager = LogPaginator(
            self.client,
            AERO_POOL_FACTORY_VAMM,
            VAMM_POOLCREATED_TOPIC0,
            label="VAMM_FACTORY",
            min_span=min_span,
        )
        cl_pager = LogPaginator(
            self.client,
            AERO_CL_FACTORY,
            CL_POOLCREATED_TOPIC0,
            label="CL_FACTORY",
            min_span=min_span,
        )

        for b0 in range(start_block, end_block + 1, step):
            b1 = min(b0 + step - 1, end_block)
            range_i += 1
            log.info("=== Range %d/%d | blocks %d..%d ===", range_i, total_ranges, b0, b1)

            # vAMM/sAMM pools
            v_before = len(vamm_created)
            for row in vamm_pager.iter_range(b0, b1):
                vamm_created.append(self.vamm_decoder.decode(row))
            log.info(
                "VAMM_FACTORY | +%d pools in this range",
                len(vamm_created) - v_before,
            )

            # SlipStream CL pools
            c_before = len(cl_created)
            for row in cl_pager.iter_range(b0, b1):
                cl_created.append(self.cl_decoder.decode(row))
            log.info(
                "CL_FACTORY | +%d pools in this range", len(cl_created) - c_before
            )

            if self.cl_decoder.tick_fee_cache:
                log.debug(
                    "CL_FACTORY | tickSpacings cached: %s",
                    sorted(self.cl_decoder.tick_fee_cache.items()),
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
