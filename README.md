# LLM Wiki on Databricks

An enterprise implementation of [Andrej Karpathy's LLM Wiki pattern](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f) built entirely on Databricks.

## Purpose

Traditional RAG re-derives answers from raw document chunks on every query. The LLM Wiki inverts this: an LLM **compiles knowledge once during ingestion**, producing a persistent wiki of interlinked markdown pages. Subsequent queries read from the wiki rather than re-processing raw sources. Cross-references, contradictions, and syntheses accumulate over time instead of being reconstructed per request.

This project maps Karpathy's three-layer architecture onto Databricks primitives:

| Layer | What it holds | Databricks service |
|---|---|---|
| **Raw Sources** | Immutable documents (articles, PDFs, markdown, text) | UC Volumes (`incoming/`) |
| **Wiki** | LLM-compiled markdown pages with YAML frontmatter | Delta tables (source of truth), optional Lakebase (fast serving), UC Volumes (Obsidian) |
| **Schema** | Page types, freshness tiers, prompt templates, rules | `wiki_config.yaml` |

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
         |    (optional)  (obsidian) Search       |
         |          |        |         |           |
         |     fast FTS   Obsidian   semantic ◀───┘
         |                 vault     retrieval
```

**Key components:**

- **Spark Declarative Pipelines (SDP)**: Auto Loader ingestion, `ai_parse_document` for PDF/DOCX/PPTX parsing, chunking, change detection
- **LLM Compiler**: Compiles source chunks into wiki pages via Foundation Model API (`databricks-claude-sonnet-4-5`)
- **Embeddings**: `databricks-gte-large-en` for Vector Search semantic retrieval
- **Lakebase**: Optional Postgres-based fast serving layer with `pg_trgm` full-text search (falls back to Delta when unavailable)
- **MCP Server**: Databricks App exposing 7 wiki tools for Claude Code / Claude Desktop
- **Web UI**: Wikipedia-style browseable interface with Cytoscape.js knowledge graph
- **Obsidian Sync**: Pages exported as markdown to UC Volumes, synced locally

## Prerequisites

- **Python 3.11+** with [uv](https://docs.astral.sh/uv/) (recommended) or pip
- **Databricks CLI** (`databricks` >= 0.230) authenticated to your workspace
- **Databricks workspace** with:
  - Unity Catalog enabled with a catalog you can write to (default: `nfleming`)
  - Serverless compute enabled (for SDP pipeline)
  - Access to Foundation Model API endpoints (`databricks-claude-sonnet-4-5`, `databricks-gte-large-en`)
  - Vector Search endpoint creation permissions

## Installation & Deployment (Verified Step-by-Step)

These steps have been tested end-to-end on the `e2-demo-field-eng` workspace.

### 1. Clone and install locally

```bash
git clone https://github.com/natefleming/databricks-llm-wiki.git
cd databricks-llm-wiki

uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

### 2. Configure Databricks CLI profile

```bash
# Authenticate (if not already)
databricks auth login --host https://<workspace-url> --profile <profile-name>

# Verify
databricks auth env --profile <profile-name>
```

Then set the profile in `databricks.yaml` under each target:

```yaml
targets:
  dev:
    workspace:
      profile: <your-profile-name>
```

### 3. Configure your catalog

Edit `databricks.yaml` and `wiki_config.yaml` to set your catalog name:

```yaml
# databricks.yaml
variables:
  catalog:
    default: <your-catalog>   # e.g., nfleming

# wiki_config.yaml
wiki:
  catalog: <your-catalog>
```

### 4. Validate the bundle

```bash
databricks bundle validate -t dev
# Expected: "Validation OK!" with no warnings
```

### 5. Deploy

```bash
databricks bundle deploy -t dev
```

This creates:
- SDP pipeline: `llm-wiki-etl-dev`
- Jobs: `llm-wiki-setup-dev`, `llm-wiki-compile-dev`
- App: `llm-wiki-dev`

**Note:** Schemas, volumes, and Delta tables are created by the setup job (not DABs resources) to avoid dev-mode schema name prefixing issues.

### 6. Run one-time setup

```bash
databricks bundle run llm_wiki_setup -t dev
```

This creates:
- Catalog (if not exists), schemas, and UC Volumes
- Delta tables: `pages`, `backlinks`, `compilation_queue`, `activity_log`
- Vector Search endpoint and sync index (may fail on first run if pages table is empty - that's OK)

### 7. Upload source files

```bash
# Upload included sample data (7 interlinked markdown files)
for f in sample_data/*.md; do
  databricks fs cp "$f" "dbfs:/Volumes/<catalog>/<raw_schema>/incoming/$(basename $f)" --profile <profile>
done

# Or upload your own documents (supports .md, .txt, .html, .pdf, .docx, .pptx)
databricks fs cp your-docs/ "dbfs:/Volumes/<catalog>/<raw_schema>/incoming/" --recursive --profile <profile>
```

### 8. Run the SDP pipeline

```bash
databricks bundle run llm_wiki_etl -t dev
```

This processes source files through:
- **Bronze**: Auto Loader reads files as binary from the incoming volume
- **Silver**: `ai_parse_document` parses PDFs/DOCX/PPTX; text files decoded directly; content chunked
- **Gold**: Change detection populates the compilation queue

### 9. Seed the compilation queue

The SDP gold layer creates a temporary `compilation_queue_pending` view. To copy items into the actual compilation queue:

```sql
-- Run in a Databricks SQL editor or notebook
INSERT INTO <catalog>.<wiki_schema>.compilation_queue
SELECT uuid() as queue_id, slug as page_id, 'new_source' as trigger_type,
       array(source_id) as trigger_source_ids, 10 as priority, 'pending' as status,
       current_timestamp() as created_at, null as completed_at, '' as error_message
FROM <catalog>.<raw_schema>.silver_content
```

### 10. Run LLM compilation + sync

```bash
databricks bundle run llm_wiki_compile -t dev
```

This runs 4 tasks:
1. **compile_pages**: Calls `databricks-claude-sonnet-4-5` to compile each pending page (~1-2 min per page)
2. **sync_to_lakebase**: Syncs to Lakebase (skipped if unavailable)
3. **sync_to_obsidian**: Exports pages as markdown to the `obsidian` UC Volume
4. **trigger_vector_search_sync**: Triggers Vector Search index re-sync

### 11. Sync to local Obsidian vault

```bash
mkdir -p vault
for f in $(databricks fs ls dbfs:/Volumes/<catalog>/<wiki_schema>/obsidian/ --profile <profile> | awk '{print $1}'); do
  databricks fs cp "dbfs:/Volumes/<catalog>/<wiki_schema>/obsidian/$f" "vault/$f" --profile <profile>
done
```

Open `vault/` as an Obsidian vault. Pages include YAML frontmatter and `[[wikilinks]]` that Obsidian renders natively.

### 12. Verify results

```bash
# Check compiled pages
databricks api post /api/2.0/sql/statements --profile <profile> --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "SELECT page_id, title, confidence, length(content_markdown) FROM <catalog>.<wiki_schema>.pages",
  "wait_timeout": "30s"
}'

# Check backlinks
databricks api post /api/2.0/sql/statements --profile <profile> --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "SELECT count(*) FROM <catalog>.<wiki_schema>.backlinks",
  "wait_timeout": "30s"
}'
```

## LLM Inference

| Purpose | Model | Endpoint | Called from |
|---|---|---|---|
| Page compilation | Claude Sonnet 4.5 | `databricks-claude-sonnet-4-5` | `notebooks/compile_pages.py` via REST API |
| Query synthesis | Claude Sonnet 4.5 | `databricks-claude-sonnet-4-5` | `operations/query.py` via SDK |
| Embeddings | GTE Large (En) | `databricks-gte-large-en` | Vector Search delta sync (automatic) |

All models are accessed via Databricks Foundation Model API (FMAPI) - pay-per-token serverless, no provisioned infrastructure. The compile notebook calls the REST API directly to avoid SDK serialization issues with the chat completions response format.

## Document Parsing

The SDP silver layer uses `ai_parse_document()` for binary document formats:

| Format | Method |
|---|---|
| `.md`, `.txt`, `.html` | Direct text decode from bytes |
| `.pdf`, `.docx`, `.pptx`, `.jpg`, `.png` | `ai_parse_document(content, MAP('version', '2.0'))` |

`ai_parse_document` extracts structured elements (paragraphs, tables, figures, headers) that are reconstructed into markdown for compilation.

## Configuration

### DABs variables (`databricks.yaml`)

| Variable | Default | Description |
|---|---|---|
| `catalog` | `nfleming` | Unity Catalog catalog name |
| `wiki_schema` | `wiki` | Schema for compiled wiki tables |
| `raw_schema` | `raw_sources` | Schema for raw ingestion tables |
| `llm_endpoint` | `databricks-claude-sonnet-4-5` | FMAPI endpoint for compilation |
| `embedding_endpoint` | `databricks-gte-large-en` | Embedding model endpoint |
| `lakebase_instance` | `llm-wiki-db` | Lakebase instance name (optional) |

### Wiki config (`wiki_config.yaml`)

Defines page types (concept, entity, source, analysis, index), freshness tiers (live through permanent), and confidence levels (high, medium, low).

## MCP Tools

| Tool | Description |
|---|---|
| `wiki_search` | Full-text + semantic search across pages |
| `wiki_read` | Read a specific page by slug |
| `wiki_ingest` | Upload a source and enqueue compilation |
| `wiki_query` | Ask a question, get a cited answer |
| `wiki_lint` | Health checks: stale pages, broken links |
| `wiki_list` | List pages with type/tag filters |
| `wiki_stats` | Page counts, type distribution |

## Issues Found During Testing

These issues were discovered and fixed during deployment testing:

1. **DABs `type: boolean` invalid** — Variable types only accept `complex` or omit. Fixed by removing `type: boolean` and quoting the default.

2. **DABs dev-mode schema prefix** — In development mode, DABs prepends `dev_<user>_` to schema resource names, causing volume path mismatches. Fixed by creating schemas/volumes via the setup job instead of DABs resources.

3. **SDK `serving_endpoints.query()` serialization** — The Databricks SDK's query method throws `dict object has no attribute as_dict` when the endpoint returns a dict. Fixed by using REST API directly in the compile notebook.

4. **Lakebase unavailability** — Lakebase API returns 404 on many workspaces. Fixed by making Lakebase optional throughout with Delta table fallback for all reads.

5. **Notebook package imports** — Serverless job clusters don't have `llm_wiki` installed. Fixed by rewriting notebooks to use Spark SQL and REST API directly.

## Project Structure

```
databricks-llm-wiki/
├── databricks.yaml                 # DABs config (engine: direct)
├── resources/
│   ├── catalog.yaml                # Documentation only (setup job handles creation)
│   ├── pipeline.yaml               # SDP pipeline (bronze/silver/gold)
│   ├── jobs.yaml                   # Setup + compile/sync jobs
│   └── app.yaml                    # Databricks App (MCP + Web UI)
├── wiki_config.yaml                # Schema layer: page types, freshness, models
├── sample_data/                    # 7 interlinked sample markdown files
├── src/llm_wiki/                   # Python package
│   ├── config.py, models.py, log.py
│   ├── search.py, sync.py
│   ├── storage/                    # Delta, Lakebase, UC Volumes
│   ├── pipeline/                   # SDP bronze/silver/gold
│   ├── compiler/                   # LLM compilation engine
│   ├── operations/                 # Ingest, query, lint
│   └── app/                        # FastAPI + MCP + Web UI
├── notebooks/                      # Setup, compile, sync (Spark SQL based)
├── tests/                          # Unit tests (38 passing)
└── Makefile
```

## Development

```bash
make install        # install package + dev deps
make test           # run unit tests (38 tests)
make lint           # check code style
make format         # auto-format code
```

## License

MIT
