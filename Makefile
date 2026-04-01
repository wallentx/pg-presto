.PHONY: test lint format typecheck all

all: lint typecheck test

test:
	uv run pytest

lint:
	ruff check .

format:
	ruff format .

typecheck:
	uv run mypy src
