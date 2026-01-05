import os

from dotenv import load_dotenv
from eth_utils import keccak

load_dotenv()

# -----------------------------
# Shared API + network config
# -----------------------------

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
ETHERSCAN_API_URL = ETHERSCAN_V2_URL

# Base mainnet chain ID in Etherscan V2
CHAIN_ID_BASE = "8453"
CHAINID_BASE = CHAIN_ID_BASE

BASE_WALLET_ADDRESS = os.getenv("BASE_WALLET_ADDRESS")

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
ASSET_PLATFORM_ID_BASE = "base"
REQUEST_SLEEP_SEC = 3.0

TIMEOUT_MARKERS = (
    "query timeout",
    "timeout occured",
    "timeout occurred",
    "server too busy",
)
RATE_LIMIT_MARKERS = ("rate limit", "max rate limit")

SHARED_DIR = os.path.dirname(__file__)
SHARED_DATA_DIR = os.path.join(SHARED_DIR, "data")
WALLET_DATA_DIR = os.path.join(SHARED_DATA_DIR, "wallet")
PROTOCOL_DATA_DIR = os.path.join(SHARED_DATA_DIR, "protocol")

WALLET_ACTIVITY_CSV_PATH = os.path.join(WALLET_DATA_DIR, "base_activity.csv")
WALLET_LOG_CACHE_PATH = os.path.join(WALLET_DATA_DIR, "log_cache.jsonl")
WALLET_SPAM_TOKENS_PATH = os.path.join(WALLET_DATA_DIR, "spam_tokens.json")
WALLET_SPAM_TX_HASHES_PATH = os.path.join(WALLET_DATA_DIR, "spam_tx_hashes.json")
TOKEN_CACHE_PATH = os.path.abspath(os.path.join(SHARED_DIR, "..", "..", "token_cache.json"))
WALLET_ENRICH_2025_BASENAME = "base_2025"
PROTOCOL_POOLS_OUT_PREFIX = os.path.join(PROTOCOL_DATA_DIR, "pools")


def require_api_key() -> None:
    if not ETHERSCAN_API_KEY:
        raise RuntimeError(
            "Missing ETHERSCAN_API_KEY in environment (.env). "
            "Create an Etherscan API key and set ETHERSCAN_API_KEY=..."
        )


# -----------------------------
# Aerodrome protocol config
# -----------------------------

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


def topic0(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


VAMM_POOLCREATED_TOPIC0 = topic0(VAMM_POOLCREATED_SIG)
CL_POOLCREATED_TOPIC0 = topic0(CL_POOLCREATED_SIG)

# tickSpacingToFee(int24)
TICKSPACING_TO_FEE_SELECTOR = "0x" + keccak(text="tickSpacingToFee(int24)").hex()[:8]


# -----------------------------
# Wallet engine config
# -----------------------------

LP_SYMBOL_HINTS = [
    "lp",
    " uni-v2",
    " uni-v3",
    " v2-lp",
    " v3-lp",
    "vamm-",
    "samm-",
    " amm-",
    " amm ",
    " position",
    "pos nft",
    " amm nft",
]

VOTE_CONTRACT_HINTS = {
    # Aerodrome vote contract (Base)
    "0x16613524e02ad97edfef371bc883f2f5d6c480a5",
}

APPROVAL_CONTRACT_HINTS = {
    # Aerodrome Voter (Base) treat as approval
    "0x827922686190790b37229fd06084350e74485b72",
}

LOCK_CONTRACTS = {
    # Aerodrome Voter Escrow (lock increase)
    "0xebf418fe2512e7e6bd9b87a8f0f294acdc67e6b4",
}

LOCK_VOTE_CONTRACTS = {
    # Aerodrome vote action
    "0x16613524e02ad97edfef371bc883f2f5d6c480a5",
}

VOTE_MIN_GASUSED = 120000

AERODROME_CONTRACTS = {
    "lp_pair": set(),  # e.g., pool/pair contracts
    "gauge": set(),  # e.g., gauge contracts
    "voting_escrow": set(),  # e.g., ve/escrow contract
}

PROTOCOL_ADDRESS_LABELS = {
    # "0x...".lower(): "Aerodrome Gauge",
    # "0x...".lower(): "Aerodrome Pool",
    # "0x...".lower(): "Uniswap V3 Pool",
}


def unique_tokens():
    return {
        "toby",
        "WGC",
        "LINKS",
        "KEVIN",
        "BDOGE",
        "TOSHI",
        "SALUKI",
        "BNKR",
        "SKITTEN",
        "SLAP",
        "Bonk",
        "BUBU",
        "ION",
        "RICKY",
        "PAWSY",
        "BACHI",
        "CRAPPY",
        "SKYA",
        "PERPY",
        "B3",
        "WELL",
        "LOGX",
        "MIGGLES",
        "DRV",
        "aura",
        "fBOMB",
        "EDGE",
        "LITKEY",
        "COOKIE",
        "RFL",
        "AERO",
        "VIRTUAL",
        "USDz",
        "USDC",
        "USD+",
        "cgUSD",
        "Anon",
        "ZEN",
        "LINK",
        "CLANKER",
        "uSOL",
        "AAVE",
        "WETH",
        "superOETHb",
        "aBasWETH",
        "cbBTC",
        "tBTC",
    }


def unique_successful_hash():
    return {
        "AAVE": "0x63706e401c06ac8513145b7687a14804d17f814b",
        "AERO": "0x940181a94a35a4569e4529a3cdfb74e38fd98631",
        "Anon": "0x79bbf4508b1391af3a0f4b30bb5fc4aa9ab0e07c",
        "B3": "0xb3b32f9f8827d4634fe7d973fa1034ec9fddb3b3",
        "BACHI": "0xeecc15f24b8fe65cfd18d4095f91a7adefdd53d5",
        "BDOGE": "0xb3ecba1330fe26bb36f40344992c481c2c916f23",
        "BNKR": "0x22af33fe49fd1fa80c7149773dde5890d3c76f3b",
        "BUBU": "0x2c001233ed5e731b98b15b30267f78c7560b71f2",
        "Bonk": "0x2dc1c8be620b95cba25d78774f716f05b159c8b9",
        "CLANKER": "0x1bc0c42215582d5a085795f4badbac3ff36d1bcb",
        "COOKIE": "0xc0041ef357b183448b235a8ea73ce4e4ec8c265f",
        "CRAPPY": "0xc8e51fefd7d595c217c7ab641513faa4ad522b26",
        "DRV": "0x9d0e8f5b25384c7310cb8c6ae32c8fbeb645d083",
        "EDGE": "0xed6e000def95780fb89734c07ee2ce9f6dcaf110",
        "ION": "0x3ee5e23eee121094f1cfc0ccc79d6c809ebd22e5",
        "KEVIN": "0xd461a534af11ef58e9f9add73129a1f45485a8dc",
        "LINK": "0x88fb150bdc53a65fe94dea0c9ba0a6daf8c6e196",
        "LINKS": "0x901f1d2bf312e6fa1716df603df8f86315bcb355",
        "LITKEY": "0xf732a566121fa6362e9e0fbdd6d66e5c8c925e49",
        "LOGX": "0x04055057677807d2a53d2b25a80ff3b4d932ae1a",
        "MIGGLES": "0xb1a03eda10342529bbf8eb700a06c60441fef25d",
        "PAWSY": "0x29e39327b5b1e500b87fc0fcae3856cd8f96ed2a",
        "PERPY": "0x168fc40dba90a07e821abbdbbc1c6d2303e51eda",
        "RFL": "0x6e2c81b6c2c0e02360f00a0da694e489acb0b05e",
        "RICKY": "0x09b052085e9c6291fbf0dfb0918c861bcb47eb25",
        "SALUKI": "0x26c69e4924bd0d7d52d680b33616042ee13f621c",
        "SKITTEN": "0x4b6104755afb5da4581b81c552da3a25608c73b8",
        "SKYA": "0x623cd3a3edf080057892aaf8d773bbb7a5c9b6e9",
        "SLAP": "0x8890de1637912fbbba36b8b19365cdc99122bd6e",
        "TOSHI": "0xac1bd2486aaf3b5c0fc3fd868558b082a531b2b4",
        "USD+": "0xb79dd08ea68a908a97220c76d19a6aa9cbde4376",
        "USDC": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "USDz": "0x04d5ddf5f3a8939889f11e97f8c4bb48317f1938",
        "VIRTUAL": "0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b",
        "WELL": "0xa88594d404727625a9437c3f886c7643872296ae",
        "WETH": "0x4200000000000000000000000000000000000006",
        "WGC": "0x3d63825b0d8669307366e6c8202f656b9e91d368",
        "ZEN": "0xf43eb8de897fbc7f2502483b2bef7bb9ea179229",
        "aBasWETH": "0xd4a0e0b9149bcee3c920d2e00b5de09138fd8bb7",
        "aura": "0x03264d29e2498284c0043d8f83e040778ab6290a",
        "cbBTC": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "cgUSD": "0xca72827a3d211cfd8f6b00ac98824872b72cab49",
        "fBOMB": "0x74ccbe53f77b08632ce0cb91d3a545bf6b8e0979",
        "superOETHb": "0xdbfefd2e8460a6ee4955a68582f85708baea60a3",
        "tBTC": "0x236aa50979d5f3de3bd1eeb40e81137f22ab794b",
        "toby": "0xb8d98a102b0079b69ffbc760c8d857a31653e56e",
        "uSOL": "0x9b8df6e244526ab5f6e6400d331db28c8fdddb55",
    }
