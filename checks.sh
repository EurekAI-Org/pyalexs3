#!/bin/bash
uv run pytest -q
uv run ruff check . --fix --select I
uv run ruff check . --fix
uv run ruff check . --fix --unsafe-fixes
uv run black . 
uv run mypy src
uv run black --check .