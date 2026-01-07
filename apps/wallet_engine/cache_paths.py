from __future__ import annotations

import os

from apps.shared.config import WALLET_DATA_DIR

WALLET_CACHE_DIR = os.path.join(WALLET_DATA_DIR, "cache")
LOGS_CACHE_DIR = os.path.join(WALLET_CACHE_DIR, "logs")
RECEIPTS_CACHE_DIR = os.path.join(WALLET_CACHE_DIR, "receipts")
SPAM_CACHE_DIR = os.path.join(WALLET_CACHE_DIR, "spam")