.PHONY: install test lint deploy setup compile sync sync-obsidian clean

# ──────────────────────────────────────────────
# Development
# ──────────────────────────────────────────────

install:
	uv pip install -e ".[dev]"

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff format src/ tests/

# ──────────────────────────────────────────────
# Databricks deployment
# ──────────────────────────────────────────────

deploy:
	databricks bundle deploy -t dev

deploy-prod:
	databricks bundle deploy -t prod

setup: deploy
	databricks bundle run llm_wiki_setup -t dev

# ──────────────────────────────────────────────
# Pipeline operations
# ──────────────────────────────────────────────

pipeline:
	databricks bundle run llm_wiki_etl -t dev

compile:
	databricks bundle run llm_wiki_compile -t dev

# ──────────────────────────────────────────────
# Obsidian sync
# ──────────────────────────────────────────────

VAULT_DIR ?= ./vault

sync-obsidian:
	@mkdir -p $(VAULT_DIR)
	databricks fs cp /Volumes/llm_wiki/wiki/obsidian/ $(VAULT_DIR)/ --recursive --overwrite
	@echo "Synced to $(VAULT_DIR)/ — open as Obsidian vault"

# ──────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────

clean:
	rm -rf dist/ build/ *.egg-info .pytest_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
