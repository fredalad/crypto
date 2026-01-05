from typing import Any, Dict

from eth_utils import to_checksum_address

from ...shared.etherscan_v2 import EtherscanV2
from ...shared.config import AERO_CL_FACTORY, TICKSPACING_TO_FEE_SELECTOR


def hex_to_int(x: str) -> int:
    return int(x, 16)


def decode_indexed_address(topic: str) -> str:
    return to_checksum_address("0x" + topic[-40:])


def decode_bool_topic(topic: str) -> bool:
    return hex_to_int(topic) != 0


def decode_int24_topic(topic: str) -> int:
    """
    topic is 32-byte hex; int24 is stored in low 24 bits with sign.
    """
    raw = int(topic, 16)
    v = raw & ((1 << 24) - 1)
    if v & (1 << 23):
        v -= 1 << 24
    return v


def encode_int24_as_int256(val: int) -> str:
    """
    ABI encodes an int24 as a 32-byte signed integer (two's complement).
    """
    if val < -(1 << 23) or val > (1 << 23) - 1:
        raise ValueError(f"int24 out of range: {val}")
    if val < 0:
        val = (1 << 256) + val
    return hex(val)[2:].rjust(64, "0")


def decode_uint256(result_hex: str) -> int:
    return int(result_hex, 16)


def get_fee_for_tick_spacing(client: EtherscanV2, tick_spacing: int) -> int:
    """
    CLFactory.tickSpacingToFee(int24) -> uint24 fee (e.g. 100, 500, 3000, 10000).
    """
    calldata = TICKSPACING_TO_FEE_SELECTOR + encode_int24_as_int256(tick_spacing)
    out = client.eth_call(AERO_CL_FACTORY, calldata)
    return decode_uint256(out)
