docker compose up -d
docker compose exec app bash
uv run python -m apps.protocol_indexer --help
uv run python -m apps.wallet_engine
uv run python main.py

wsl -l -v
wsl -d Ubuntu
