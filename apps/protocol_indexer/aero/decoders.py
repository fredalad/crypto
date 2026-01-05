from typing import Any, Dict

from eth_utils import to_checksum_address

from .abi import (
    decode_bool_topic,
    decode_indexed_address,
    decode_int24_topic,
    get_fee_for_tick_spacing,
    hex_to_int,
)
from .models import CLPoolCreated, VammPoolCreated
from ...shared.etherscan_v2 import EtherscanV2


class VammPoolDecoder:
    def decode(self, log_row: Dict[str, Any]) -> VammPoolCreated:
        topics = log_row["topics"]
        token0 = decode_indexed_address(topics[1])
        token1 = decode_indexed_address(topics[2])
        stable = decode_bool_topic(topics[3])

        # data = pool (32) + uint256 (32)
        data_bytes = bytes.fromhex(log_row["data"][2:])
        pool = to_checksum_address("0x" + data_bytes[12:32].hex())

        return VammPoolCreated(
            pool_type="vamm",
            pool=pool,
            token0=token0,
            token1=token1,
            stable=stable,
            created_block=hex_to_int(log_row["blockNumber"]),
            tx_hash=log_row["transactionHash"],
        )


class CLPoolDecoder:
    def __init__(self, client: EtherscanV2) -> None:
        self.client = client
        self.tick_fee_cache: Dict[int, int] = {}

    def _fee_for_tick_spacing(self, tick_spacing: int) -> int:
        if tick_spacing not in self.tick_fee_cache:
            self.tick_fee_cache[tick_spacing] = get_fee_for_tick_spacing(
                self.client, tick_spacing
            )
        return self.tick_fee_cache[tick_spacing]

    def decode(self, log_row: Dict[str, Any]) -> CLPoolCreated:
        topics = log_row["topics"]
        token0 = decode_indexed_address(topics[1])
        token1 = decode_indexed_address(topics[2])
        tick_spacing = decode_int24_topic(topics[3])

        # data = pool (single 32-byte word)
        data_bytes = bytes.fromhex(log_row["data"][2:])
        pool = to_checksum_address("0x" + data_bytes[12:32].hex())

        fee = self._fee_for_tick_spacing(tick_spacing)

        return CLPoolCreated(
            pool_type="cl",
            pool=pool,
            token0=token0,
            token1=token1,
            tick_spacing=int(tick_spacing),
            fee=int(fee),
            created_block=hex_to_int(log_row["blockNumber"]),
            tx_hash=log_row["transactionHash"],
        )
