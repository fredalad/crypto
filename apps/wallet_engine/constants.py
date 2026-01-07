from __future__ import annotations

from enum import Enum

from apps.shared.config import topic0

ERC20_TRANSFER_SIG = "Transfer(address,address,uint256)"
ERC20_APPROVAL_SIG = "Approval(address,address,uint256)"

VAMM_SWAP_SIG = "Swap(address,uint256,uint256,uint256,uint256,address)"
VAMM_MINT_SIG = "Mint(address,uint256,uint256)"
VAMM_BURN_SIG = "Burn(address,uint256,uint256,address)"

CL_SWAP_SIG = "Swap(address,address,int256,int256,uint160,uint128,int24)"
CL_MINT_SIG = "Mint(address,address,int24,int24,uint128,uint256,uint256)"
CL_BURN_SIG = "Burn(address,int24,int24,uint128,uint256,uint256)"
CL_COLLECT_SIG = "Collect(address,address,int24,int24,uint128,uint128)"

ERC20_TRANSFER_TOPIC0 = topic0(ERC20_TRANSFER_SIG)
ERC20_APPROVAL_TOPIC0 = topic0(ERC20_APPROVAL_SIG)

VAMM_SWAP_TOPIC0 = topic0(VAMM_SWAP_SIG)
VAMM_MINT_TOPIC0 = topic0(VAMM_MINT_SIG)
VAMM_BURN_TOPIC0 = topic0(VAMM_BURN_SIG)

CL_SWAP_TOPIC0 = topic0(CL_SWAP_SIG)
CL_MINT_TOPIC0 = topic0(CL_MINT_SIG)
CL_BURN_TOPIC0 = topic0(CL_BURN_SIG)
CL_COLLECT_TOPIC0 = topic0(CL_COLLECT_SIG)


class Action(str, Enum):
    SWAP = "SWAP"
    LP_ADD = "LP_ADD"
    LP_REMOVE = "LP_REMOVE"
    GAUGE_STAKE = "GAUGE_STAKE"
    GAUGE_UNSTAKE = "GAUGE_UNSTAKE"
    LOCK_CREATE = "LOCK_CREATE"
    LOCK_INCREASE = "LOCK_INCREASE"
    LOCK_WITHDRAW = "LOCK_WITHDRAW"
    CLAIM_FEES = "CLAIM_FEES"
    CLAIM_REWARDS = "CLAIM_REWARDS"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"
    SPAM = "SPAM"