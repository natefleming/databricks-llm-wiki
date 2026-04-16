# LLM Wiki on Databricks

## What this project is

An enterprise implementation of Karpathy's LLM Wiki pattern on Databricks. Instead of
traditional RAG (re-deriving answers from raw documents per query), an LLM compiles
knowledge once during ingestion into a persistent wiki of interlinked markdown pages.
Queries read from the wiki rather than re-processing raw sources.

The three-layer architecture maps to Databricks as:
- **Raw Sources** → UC Volumes (`incoming/`), ingested via SDP Auto Loader
- **Wiki** → Delta tables (source of truth), Lakebase (fast serving), UC Volumes (Obsidian)
- **Schema** → `wiki_config.yaml` governs page types, freshness tiers, prompts, models

## Tech stack

- Python 3.11, Pydantic v2, FastAPI, loguru, httpx
- Databricks SDK, databricks-sql-connector, psycopg3
- Spark Declarative Pipelines (SDP / DLT) for ETL
- Databricks Asset Bundles (`engine: direct`) for deployment
- Lakebase (managed Postgres) with pg_trgm full-text search
- Databricks Vector Search for semantic retrieval
- Foundation Model API (FMAPI) for LLM compilation

## How to run

```bash
# Local install
uv pip install -e ".[dev]"

# Deploy everything to Databricks (dev target)
databricks bundle deploy -t dev
databricks bundle run llm_wiki_setup -t dev   # one-time Lakebase + Vector Search

# Ingest → process → compile → sync
databricks fs cp ./sources/ /Volumes/llm_wiki/raw_sources_<user>/incoming/ --recursive
databricks bundle run llm_wiki_etl -t dev
databricks bundle run llm_wiki_compile -t dev

# Sync to local Obsidian vault
make sync-obsidian VAULT_DIR=./vault
```

## Code conventions

- **Logging**: `from llm_wiki.log import logger`. All output to stderr, no log files.
- **Type hints**: All function signatures must have type annotations.
- **Docstrings**: All public functions and classes need Google-style docstrings.
- **Models**: Use Pydantic v2 models for all data structures. Models live in `models.py`.
- **Config**: Access via `from llm_wiki.config import get_config`. Backed by `wiki_config.yaml`.
- **Storage**: Three backends behind dedicated classes (`DeltaStore`, `LakebaseStore`, `VolumeStore`).
  Delta is the source of truth. Lakebase and Volumes are materialized projections.
- **Errors**: Let exceptions propagate. The compilation engine has its own retry logic.
  Do not add catch-all handlers.
- **SQL**: Delta operations use `databricks-sql-connector`. Lakebase uses `psycopg` with
  parameterized queries (never string interpolation for user-provided values).

## Key modules

| Module | Purpose |
|---|---|
| `config.py` | Pydantic settings loaded from `wiki_config.yaml` |
| `models.py` | Domain models: `Page`, `Source`, `SourceChunk`, `BackLink`, `Frontmatter`, enums |
| `storage/delta.py` | Delta table CRUD (pages, backlinks, queue, activity log) |
| `storage/lakebase.py` | Lakebase/Postgres CRUD + pg_trgm search, connection pooling |
| `storage/volumes.py` | UC Volume file upload + Obsidian markdown export |
| `pipeline/bronze.py` | SDP: Auto Loader streaming table from `incoming/` volume |
| `pipeline/silver.py` | SDP: text extraction, cleaning, chunking (1000 tokens, 200 overlap) |
| `pipeline/gold.py` | SDP: change detection via content_hash, compilation queue population |
| `compiler/engine.py` | `WikiCompiler`: parallel compilation via ThreadPoolExecutor + FMAPI |
| `compiler/prompts.py` | Prompt templates per page type (concept, entity, source, analysis, index) |
| `compiler/context.py` | `ContextAssembler`: gathers source chunks + related pages for compilation |
| `compiler/frontmatter.py` | YAML frontmatter parse/render, `[[wikilink]]` extraction |
| `search.py` | `WikiSearch`: hybrid search (Lakebase FTS + Vector Search + reciprocal rank fusion) |
| `sync.py` | `WikiSync`: Delta → Lakebase, Delta → UC Volume (Obsidian), Vector Search trigger |
| `operations/ingest.py` | Upload source (URL/text/file) to volume + enqueue compilation |
| `operations/query.py` | `QueryEngine`: search → context assembly → LLM synthesis with citations |
| `operations/lint.py` | `WikiLinter`: stale pages, broken links, orphans, thin content |
| `app/server.py` | FastAPI entry point: MCP server, REST API, Web UI |
| `app/tools.py` | 7 MCP tools: search, read, ingest, query, lint, list, stats |
| `app/routes.py` | Web UI routes: home, page view, search, graph, editor |

## DABs resources

All infrastructure is declarative in `resources/`:
- `catalog.yaml` → UC schemas (`raw_sources`, `wiki`) + volumes (`incoming`, `obsidian`)
- `pipeline.yaml` → SDP pipeline (bronze/silver/gold, serverless)
- `jobs.yaml` → `llm_wiki_setup` (one-time) + `llm_wiki_compile` (compile + sync)
- `app.yaml` → Databricks App (MCP + Web UI, with serving endpoint + vector search access)

Dev target uses per-user schema suffixes (`wiki_<user>`). Prod uses shared schemas.

## Testing

```bash
pytest tests/ -v                    # all tests
pytest tests/test_frontmatter.py    # specific file
```

Unit tests cover models, config loading, frontmatter roundtrip, wikilink extraction,
and reciprocal rank fusion. Integration tests require a Databricks workspace connection.
