import os
import requests
from dotenv import load_dotenv

load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
assert ETHERSCAN_API_KEY, "Missing ETHERSCAN_API_KEY"

ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID_BASE = 8453


def get_contract_creation_block(contract_address: str) -> int:
    params = {
        "chainid": CHAIN_ID_BASE,
        "module": "contract",
        "action": "getcontractcreation",
        "contractaddresses": contract_address,
        "apikey": ETHERSCAN_API_KEY,
    }

    r = requests.get(ETHERSCAN_API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if data.get("status") != "1" or not data.get("result"):
        raise ValueError(f"Could not find contract creation for {contract_address}")

    # Result is a list (supports batch lookup)
    tx_hash = data["result"][0]["txHash"]

    # Now fetch block from tx hash
    return get_block_from_tx_hash(tx_hash)


def get_block_from_tx_hash(tx_hash: str) -> int:
    params = {
        "chainid": CHAIN_ID_BASE,
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": tx_hash,
        "apikey": ETHERSCAN_API_KEY,
    }

    r = requests.get(ETHERSCAN_API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    return int(data["result"]["blockNumber"], 16)


block = get_contract_creation_block("0x827922686190790b37229fd06084350E74485b72")
print(block)
