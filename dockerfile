# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

# --- Environment -------------------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# --- System deps -------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# --- Install uv --------------------------------------------------------------
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
 && ln -s /root/.local/bin/uv /usr/local/bin/uv

# --- Install Python deps (cached layer) --------------------------------------
COPY pyproject.toml uv.lock ./

# Create venv + install EXACT locked deps
RUN uv venv \
 && uv sync --frozen --no-dev

# --- Copy app code -----------------------------------------------------------
COPY . .

# --- Default: interactive shell (dev) ---------------------------------------
CMD ["/bin/bash"]
