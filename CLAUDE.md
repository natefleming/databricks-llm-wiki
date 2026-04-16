# LLM Wiki on Databricks

## What this project is

An enterprise implementation of Karpathy's LLM Wiki pattern on Databricks. Instead of
traditional RAG (re-deriving answers per query), an LLM compiles knowledge once during
ingestion into a persistent wiki of interlinked markdown pages.

Three-layer architecture mapped to Databricks:
- **Raw Sources** → UC Volumes (`incoming/`), ingested via SDP Auto Loader
- **Wiki** → Delta tables (source of truth), optional Lakebase (fast serving), UC Volumes (Obsidian)
- **Schema** → `wiki_config.yaml` governs page types, freshness tiers, prompts

## Tech stack

- Python 3.11, Pydantic v2, FastAPI, loguru, httpx
- Databricks SDK, databricks-sql-connector, psycopg3 (optional)
- Spark Declarative Pipelines (SDP) with `ai_parse_document` for document parsing
- Databricks Asset Bundles (`engine: direct`)
- Foundation Model API: `databricks-claude-sonnet-4-5` (compilation), `databricks-gte-large-en` (embeddings)
- Optional: Lakebase (Postgres) with pg_trgm, Databricks Vector Search

## Deployment (verified working)

```bash
# Install locally
uv pip install -e ".[dev]"

# Configure profile in databricks.yaml targets, set catalog in variables
databricks bundle validate -t dev

# Deploy + setup
databricks bundle deploy -t dev
databricks bundle run llm_wiki_setup -t dev

# Upload sources + run pipeline
databricks fs cp sample_data/*.md dbfs:/Volumes/<catalog>/<raw_schema>/incoming/ --profile <profile>
databricks bundle run llm_wiki_etl -t dev

# Seed compilation queue (SQL: INSERT INTO compilation_queue SELECT FROM silver_content)
# Then compile + sync
databricks bundle run llm_wiki_compile -t dev

# Sync to local Obsidian vault
mkdir -p vault && databricks fs cp dbfs:/Volumes/<catalog>/<wiki_schema>/obsidian/ vault/ --recursive
```

## Code conventions

- **Logging**: `from llm_wiki.log import logger`. All output to stderr, no log files.
- **Type hints**: All function signatures must have type annotations.
- **Docstrings**: All public functions and classes need docstrings.
- **Models**: Pydantic v2 models in `models.py` for all data structures.
- **Config**: `from llm_wiki.config import get_config`. Backed by `wiki_config.yaml`.
- **Storage**: Three backends (`DeltaStore`, `LakebaseStore`, `VolumeStore`). Delta is source of truth. Lakebase is optional.
- **SQL safety**: Notebooks use Spark SQL directly (no llm_wiki package import on job clusters). Parameterized queries for Lakebase.
- **FMAPI calls**: Notebooks use REST API directly (not SDK `serving_endpoints.query()`) to avoid serialization issues.

## Key design decisions

1. **Schemas/volumes created by setup job, not DABs resources** — DABs dev mode adds `dev_<user>_` prefix to schema names, breaking volume path references in SDP pipelines.

2. **Compile notebook uses Spark SQL + REST API** — Serverless job clusters don't have the `llm_wiki` Python package. Notebooks are self-contained using Spark SQL for Delta operations and REST API for FMAPI calls.

3. **Lakebase is fully optional** — The app/tools/routes fall back to DeltaStore when Lakebase is unavailable. Search falls back to Delta SQL LIKE queries.

4. **`ai_parse_document` in SDP silver layer** — Handles PDF, DOCX, PPTX, images. Text files are decoded directly. Both paths unify into the same silver_content table.

## Testing

```bash
pytest tests/ -v    # 38 unit tests
```

Tests cover: models, config loading, frontmatter roundtrip, wikilink extraction, RRF search merge.
Integration tests require a Databricks workspace.
