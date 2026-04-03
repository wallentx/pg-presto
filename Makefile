.PHONY: test lint format typecheck all build clean

all: lint typecheck test build

test:
	uv run pytest

lint:
	ruff check .

format:
	ruff format .

typecheck:
	uv run mypy src

build: clean
	mkdir -p dist
	uv run shiv -c pg-aegis -o dist/pg-aegis .

clean:
	rm -rf dist/
