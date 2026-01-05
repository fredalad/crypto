from dataclasses import dataclass


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
