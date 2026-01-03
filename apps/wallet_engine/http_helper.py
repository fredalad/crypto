import requests

from .config import (
    COINGECKO_BASE_URL,
    COINGECKO_API_KEY,
)

from typing import Dict, Any


def coin_gecko_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Small wrapper for CoinGecko API GET.
    - Adds API key if provided (demo style: x_cg_demo_api_key query param). :contentReference[oaicite:3]{index=3}
    """
    url = f"{COINGECKO_BASE_URL}{path}"
    headers = {"Accept": "application/json"}

    if COINGECKO_API_KEY:
        params = {**params, "x_cg_demo_api_key": COINGECKO_API_KEY}

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()
