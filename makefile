# ---------- Config ----------
SERVICE=app

# ---------- Docker lifecycle ----------
build:
	docker compose build

rebuild:
	docker compose build --no-cache

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

# ---------- Dev workflow ----------
shell:
	docker compose exec $(SERVICE) bash

run:
	docker compose exec $(SERVICE) uv run python main.py

watch:
	docker compose exec $(SERVICE) uv run watchfiles "python main.py" .

# ---------- One-shot helpers ----------
restart: down up

clean:
	docker compose down -v --remove-orphans
