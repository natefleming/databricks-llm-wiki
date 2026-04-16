# LLM Wiki on Databricks

An enterprise implementation of [Andrej Karpathy's LLM Wiki pattern](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f) built entirely on Databricks.

## Purpose

Traditional RAG re-derives answers from raw document chunks on every query. The LLM Wiki inverts this: an LLM **compiles knowledge once during ingestion**, producing a persistent wiki of interlinked markdown pages. Subsequent queries read from the wiki rather than re-processing raw sources. Cross-references, contradictions, and syntheses accumulate over time instead of being reconstructed per request.

This project maps Karpathy's three-layer architecture onto Databricks primitives:

| Layer | What it holds | Databricks service |
|---|---|---|
| **Raw Sources** | Immutable documents (articles, PDFs, markdown, text) | UC Volumes (`incoming/`) |
| **Wiki** | LLM-compiled markdown pages with YAML frontmatter | Delta tables (source of truth), Lakebase (fast serving), UC Volumes (Obsidian) |
| **Schema** | Page types, freshness tiers, prompt templates, rules | `wiki_config.yaml` |

The system is designed for agent/MCP integration, Obsidian-based browsing, and enterprise-scale ingestion through Spark Declarative Pipelines.

## Architecture

```
                     wiki_config.yaml (schema layer)
                              |
         ┌────────────────────┼────────────────────┐
         |                    |                    |
   UC Volumes           Delta Tables          Databricks App
   (incoming/)     ─SDP─▶ pages, backlinks      (MCP + Web UI)
         |            compilation_queue             |
         |                    |                    |
         |           LLM Compilation Job           |
         |                    |                    |
         |          ┌────────┬┼────────┐           |
         |          |        ||        |           |
         |     Lakebase   UC Volume  Vector     queries
         |     (pg_trgm)  (obsidian) Search       |
         |          |        |         |           |
         |     fast FTS   Obsidian   semantic ◀───┘
         |                 vault     retrieval
```

**Data flow:** Raw files land in the `incoming/` volume. A Spark Declarative Pipeline (SDP) extracts text, chunks it, and detects changes. A separate Databricks Job calls the Foundation Model API to compile pages, then syncs the results to Lakebase (fast serving), a UC Volume (Obsidian markdown), and a Vector Search index (semantic retrieval). A Databricks App exposes an MCP server plus a Wikipedia-style web UI.

## Prerequisites

- **Python 3.11+** with [uv](https://docs.astral.sh/uv/) (recommended) or pip
- **Databricks CLI** (`databricks` >= 0.230) authenticated to your workspace
- **Databricks workspace** with:
  - Unity Catalog enabled (catalog `llm_wiki` must exist or you must have `CREATE CATALOG` privileges)
  - A SQL warehouse (serverless preferred)
  - Access to Foundation Model API endpoints (default: `databricks-claude-sonnet-4`)
  - Lakebase provisioning permissions
  - Vector Search endpoint creation permissions

## Installation

### 1. Clone and install the Python package

```bash
git clone <repo-url> llm-wiki
cd llm-wiki

# Create virtual environment and install (uv)
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Or with pip
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Authenticate the Databricks CLI

```bash
databricks auth login --host https://<workspace-url>

# Verify
databricks auth env
```

### 3. Create the Unity Catalog catalog

If the `llm_wiki` catalog does not yet exist in your workspace:

```sql
-- Run in a Databricks SQL editor or notebook
CREATE CATALOG IF NOT EXISTS llm_wiki;
```

Or change the catalog name in `databricks.yaml` by setting the `catalog` variable.

### 4. Deploy all infrastructure

```bash
# Deploy DABs resources (schemas, volumes, pipeline, jobs, app)
databricks bundle deploy -t dev

# Run the one-time setup job (creates Lakebase instance,
# Lakebase tables, Vector Search endpoint, and Vector Search index)
databricks bundle run llm_wiki_setup -t dev
```

The setup job is idempotent. It checks for existing resources before creating them.

### 5. Verify the deployment

```bash
# List deployed resources
databricks bundle summary -t dev
```

You should see:

- Schemas: `llm_wiki.raw_sources_<user>`, `llm_wiki.wiki_<user>`
- Volumes: `incoming`, `obsidian`
- Pipeline: `llm-wiki-etl-dev`
- Jobs: `llm-wiki-setup-dev`, `llm-wiki-compile-dev`
- App: `llm-wiki-dev`

## Usage

### Ingest sources

Drop raw files (markdown, text, HTML) into the incoming UC Volume:

```bash
# Upload a single file
databricks fs cp my-article.md /Volumes/llm_wiki/raw_sources_<user>/incoming/

# Upload a directory of files
databricks fs cp ./sources/ /Volumes/llm_wiki/raw_sources_<user>/incoming/ --recursive
```

Or use the MCP `wiki_ingest` tool from Claude Code:

```
Use wiki_ingest to add this URL: https://example.com/interesting-article
```

### Process sources through the pipeline

```bash
# Run the SDP pipeline (Auto Loader → extraction → chunking → change detection)
make pipeline
# or: databricks bundle run llm_wiki_etl -t dev
```

### Compile wiki pages

```bash
# Run LLM compilation + sync to Lakebase, Obsidian volume, and Vector Search
make compile
# or: databricks bundle run llm_wiki_compile -t dev
```

The compile job:
1. Reads pending items from the `compilation_queue` table
2. Assembles context (source chunks + related existing pages)
3. Calls the Foundation Model API to compile each page
4. Writes compiled pages to `wiki.pages` Delta table
5. Extracts `[[wikilinks]]` and writes to the `backlinks` table
6. Syncs pages to Lakebase, the Obsidian volume, and triggers a Vector Search re-index

### Browse in Obsidian

```bash
# Sync compiled pages to a local Obsidian vault
make sync-obsidian VAULT_DIR=./my-vault
```

Open `./my-vault/` as an Obsidian vault. Pages include YAML frontmatter and `[[wikilinks]]` that Obsidian renders natively with graph view, backlinks, and search.

### Browse via the Web UI

The Databricks App (`llm-wiki-dev`) serves a Wikipedia-style interface:

- **Home** (`/`): recent pages, stats, search bar
- **Page view** (`/page/<slug>`): rendered markdown, backlinks sidebar, related pages
- **Search** (`/search?q=...`): combined full-text and semantic results
- **Knowledge graph** (`/graph`): interactive Cytoscape.js visualization
- **Editor** (`/edit/<slug>`): split-pane markdown editor with live preview

### Use MCP tools

The app exposes an MCP server (Streamable HTTP transport at `/mcp`) with these tools:

| Tool | Description |
|---|---|
| `wiki_search` | Full-text + semantic search across pages |
| `wiki_read` | Read a specific page by its slug |
| `wiki_ingest` | Upload a source (URL, text, or file) and enqueue compilation |
| `wiki_query` | Ask a question and get a cited answer synthesized from wiki pages |
| `wiki_lint` | Run health checks: stale pages, broken links, orphans, thin content |
| `wiki_list` | List pages with optional type/tag filters |
| `wiki_stats` | Page counts, type distribution, confidence breakdown |

To connect from Claude Code or Claude Desktop, add the app URL to your MCP client configuration.

## Configuration

All wiki behavior is governed by `wiki_config.yaml`:

### Page types

| Type | Description | Default freshness |
|---|---|---|
| `concept` | Explains a concept, idea, or technique | monthly |
| `entity` | Describes a person, organization, or project | weekly |
| `source` | Summary of an ingested source document | permanent |
| `analysis` | Cross-cutting synthesis across multiple sources | weekly |
| `index` | Auto-generated listing of related pages | daily |

### Freshness tiers

| Tier | Max age | Use case |
|---|---|---|
| `live` | 15 min | Real-time data, live metrics |
| `hourly` | 1 hour | Rapidly evolving topics |
| `daily` | 1 day | News, current events |
| `weekly` | 1 week | Software releases, API changes |
| `monthly` | 30 days | General knowledge, how-to guides |
| `permanent` | never | Personal notes, historical facts |

### Confidence levels

| Level | Criteria |
|---|---|
| `high` | Official docs, peer-reviewed papers, 2+ corroborating sources |
| `medium` | Reputable blogs, conference talks, 1 established source |
| `low` | Single unverified source, community posts |

### DABs variables

Override defaults per target in `databricks.yaml`:

| Variable | Default | Description |
|---|---|---|
| `catalog` | `llm_wiki` | Unity Catalog catalog name |
| `wiki_schema` | `wiki` | Schema for compiled wiki tables |
| `raw_schema` | `raw_sources` | Schema for raw ingestion tables |
| `llm_endpoint` | `databricks-claude-sonnet-4` | Foundation Model API endpoint |
| `embedding_endpoint` | `databricks-gte-large-en` | Embedding model endpoint |
| `lakebase_instance` | `llm-wiki-db` | Lakebase instance name |

## Data Model

### Delta tables

**`llm_wiki.<wiki_schema>.pages`** - Compiled wiki pages (source of truth)

| Column | Type | Description |
|---|---|---|
| `page_id` | STRING | URL-safe slug (primary key) |
| `title` | STRING | Display title |
| `page_type` | STRING | concept, entity, source, analysis, index |
| `content_markdown` | STRING | Compiled page body (markdown) |
| `frontmatter` | STRING | YAML frontmatter as JSON string |
| `confidence` | STRING | high, medium, low |
| `sources` | ARRAY\<STRING\> | Source IDs used for compilation |
| `related` | ARRAY\<STRING\> | Related page IDs (from wikilinks) |
| `tags` | ARRAY\<STRING\> | Extracted tags |
| `freshness_tier` | STRING | Staleness tier |
| `content_hash` | STRING | SHA-256 hash of compiled content |
| `created_at` | TIMESTAMP | First compilation time |
| `updated_at` | TIMESTAMP | Last compilation time |
| `compiled_by` | STRING | Model name used |

**`llm_wiki.<wiki_schema>.backlinks`** - Cross-reference graph

**`llm_wiki.<wiki_schema>.compilation_queue`** - Pending compilation requests

**`llm_wiki.<wiki_schema>.activity_log`** - Append-only operation log

**`llm_wiki.<raw_schema>.sources`** - Raw ingested documents (streaming table via SDP)

**`llm_wiki.<raw_schema>.source_chunks`** - Chunked source content (materialized view via SDP)

### Lakebase tables

The Lakebase instance mirrors the wiki schema for fast serving:

- `pages` with `pg_trgm` trigram indexes and `tsvector` full-text search
- `backlinks` with foreign key references
- `activity_log`

### UC Volumes

- `llm_wiki.<raw_schema>.incoming` - Drop zone for new source files
- `llm_wiki.<wiki_schema>.obsidian` - Exported markdown files for Obsidian

## Project Structure

```
llm-wiki/
├── databricks.yaml                 # DABs config (engine: direct)
├── resources/
│   ├── catalog.yaml                # UC schemas + volumes
│   ├── pipeline.yaml               # SDP pipeline (bronze/silver/gold)
│   ├── jobs.yaml                   # Setup + compile/sync jobs
│   └── app.yaml                    # Databricks App (MCP + Web UI)
├── wiki_config.yaml                # Schema layer: page types, freshness, models
├── src/llm_wiki/
│   ├── config.py                   # Pydantic settings loader
│   ├── models.py                   # Domain models (Page, Source, Frontmatter, ...)
│   ├── log.py                      # Loguru logging (stderr only)
│   ├── search.py                   # Unified search (Lakebase FTS + Vector Search + RRF)
│   ├── sync.py                     # Delta → Lakebase / Obsidian / Vector Search sync
│   ├── storage/
│   │   ├── delta.py                # Delta table CRUD (databricks-sql-connector)
│   │   ├── lakebase.py             # Lakebase/Postgres CRUD (psycopg pool)
│   │   └── volumes.py              # UC Volume file operations (databricks-sdk)
│   ├── pipeline/
│   │   ├── bronze.py               # SDP: Auto Loader ingestion from incoming volume
│   │   ├── silver.py               # SDP: text extraction, chunking, metadata
│   │   └── gold.py                 # SDP: change detection, compilation queue
│   ├── compiler/
│   │   ├── engine.py               # LLM compilation orchestrator (ThreadPoolExecutor)
│   │   ├── prompts.py              # Prompt templates per page type
│   │   ├── context.py              # Context assembly (sources + related pages)
│   │   └── frontmatter.py          # YAML frontmatter parse/render, wikilink extraction
│   ├── operations/
│   │   ├── ingest.py               # Ingest: URL fetch, file upload, queue enqueue
│   │   ├── query.py                # Query: search + LLM-synthesized cited answer
│   │   └── lint.py                 # Lint: stale pages, broken links, orphans
│   └── app/
│       ├── server.py               # FastAPI entry point (MCP + REST + Web UI)
│       ├── tools.py                # MCP tool definitions (7 tools)
│       ├── routes.py               # Web UI routes (home, page, search, graph, edit)
│       ├── templates/              # Jinja2 HTML templates
│       └── static/                 # CSS + JS (Cytoscape.js graph, theme toggle)
├── notebooks/
│   ├── setup_infrastructure.py     # Provisions Lakebase + Vector Search (idempotent)
│   ├── compile_pages.py            # Runs LLM compilation on pending queue
│   └── sync.py                     # Syncs Delta → Lakebase / Obsidian / Vector Search
├── tests/                          # Unit tests (pytest)
├── Makefile                        # Build, deploy, sync shortcuts
├── CLAUDE.md                       # Claude Code project instructions
└── pyproject.toml                  # Package definition (hatchling)
```

## Development

```bash
make install        # install package + dev dependencies
make test           # run unit tests
make lint           # check code style (ruff)
make format         # auto-format code (ruff)
make clean          # remove build artifacts
```

### Running tests

```bash
# All tests
pytest tests/ -v

# Specific test file
pytest tests/test_frontmatter.py -v
```

### Production deployment

```bash
# Set the prod_user variable in databricks.yaml or pass it:
databricks bundle deploy -t prod --var prod_user=<service-principal>
databricks bundle run llm_wiki_setup -t prod
```

The `prod` target uses shared schema names (`wiki`, `raw_sources`) instead of per-user suffixes.

## License

MIT
