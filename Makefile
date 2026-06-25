.PHONY: lint typecheck test run

UV := $(shell command -v uv 2>/dev/null || echo "python3 -m uv")

lint:
	$(UV) run ruff check .

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest

run:
	$(UV) run pmbot --help
