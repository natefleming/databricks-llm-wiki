"""FastAPI application serving MCP tools, web UI, and REST API.

Entry point for the Databricks App. Serves:
- MCP endpoint at /mcp (Streamable HTTP transport)
- Web UI at / (Wikipedia-style browseable interface)
- REST API at /api/* (programmatic access)

Usage:
    python -m llm_wiki.app.server
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from llm_wiki.config import get_config, WikiConfig
from llm_wiki.log import logger

# Resolve paths relative to this file
_APP_DIR = Path(__file__).parent
_TEMPLATE_DIR = _APP_DIR / "templates"
_STATIC_DIR = _APP_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize stores and MCP on startup."""
    config = get_config(os.environ.get("WIKI_CONFIG_PATH"))

    catalog = os.environ.get("WIKI_CATALOG", config.wiki.catalog)
    wiki_schema = os.environ.get("WIKI_SCHEMA", config.wiki.wiki_schema if hasattr(config.wiki, "wiki_schema") else "wiki")
    raw_schema = os.environ.get("RAW_SCHEMA", "raw_sources")
    lakebase_instance = os.environ.get("LAKEBASE_INSTANCE", config.lakebase.instance_name)

    # Initialize stores
    from llm_wiki.storage.delta import DeltaStore
    from llm_wiki.storage.lakebase import LakebaseStore
    from llm_wiki.storage.volumes import VolumeStore

    delta_store = DeltaStore(catalog=catalog, wiki_schema=wiki_schema, raw_schema=raw_schema)
    volume_store = VolumeStore(catalog=catalog, raw_schema=raw_schema, wiki_schema=wiki_schema)

    # Initialize Lakebase with credentials from environment or SDK
    lakebase_store = None
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        lb_info = w.lakebase.get_database_instance(lakebase_instance)
        lakebase_store = LakebaseStore(
            host=lb_info.host,
            port=lb_info.port or 5432,
            database="wiki",
        )
    except Exception as e:
        logger.warning("Could not connect to Lakebase, using fallback", error=str(e))

    # Initialize search and query engine
    from llm_wiki.search import WikiSearch
    from llm_wiki.operations.query import QueryEngine

    search = WikiSearch(
        lakebase_store=lakebase_store,
        vs_index_name=config.vector_search.index_name,
    ) if lakebase_store else None

    query_engine = QueryEngine(
        search=search,
        delta_store=delta_store,
        config=config,
    ) if search else None

    # Store in app state
    app.state.config = config
    app.state.delta_store = delta_store
    app.state.lakebase_store = lakebase_store
    app.state.volume_store = volume_store
    app.state.search = search
    app.state.query_engine = query_engine

    # Register MCP tools
    if lakebase_store and search and query_engine:
        from mcp.server import Server
        from llm_wiki.app.tools import register_tools

        mcp = Server("llm-wiki")
        register_tools(mcp, lakebase_store, delta_store, volume_store, search, query_engine, config)
        app.state.mcp = mcp

    logger.info("LLM Wiki server started", catalog=catalog, schema=wiki_schema)

    yield

    # Cleanup
    if lakebase_store:
        lakebase_store.close()
    logger.info("LLM Wiki server stopped")


# Create FastAPI app
app = FastAPI(
    title="LLM Wiki",
    description="Karpathy's LLM Wiki pattern on Databricks",
    version="0.1.0",
    lifespan=lifespan,
)

# Mount static files
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

# Templates
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR)) if _TEMPLATE_DIR.exists() else None


# ──────────────────────────────────────────────
# Dependency injection
# ──────────────────────────────────────────────

def get_lakebase(request: Request) -> Any:
    """Get Lakebase store from app state."""
    return request.app.state.lakebase_store


def get_search(request: Request) -> Any:
    """Get search engine from app state."""
    return request.app.state.search


def get_config_dep(request: Request) -> WikiConfig:
    """Get wiki config from app state."""
    return request.app.state.config


# ──────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────

@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "service": "llm-wiki"}


# ──────────────────────────────────────────────
# REST API
# ──────────────────────────────────────────────

@app.get("/api/search")
async def api_search(q: str, limit: int = 20, request: Request = None) -> JSONResponse:
    """Search the wiki via REST API."""
    search = request.app.state.search
    if not search:
        return JSONResponse({"error": "Search not available"}, status_code=503)

    results = search.search(q, limit=limit)
    return JSONResponse([r.model_dump() for r in results])


@app.get("/api/pages/{page_id}")
async def api_get_page(page_id: str, request: Request = None) -> JSONResponse:
    """Get a specific page via REST API."""
    store = request.app.state.lakebase_store
    if not store:
        return JSONResponse({"error": "Store not available"}, status_code=503)

    page = store.get_page(page_id)
    if not page:
        return JSONResponse({"error": "Page not found"}, status_code=404)

    return JSONResponse(page.model_dump(mode="json"))


@app.get("/api/stats")
async def api_stats(request: Request = None) -> JSONResponse:
    """Get wiki statistics via REST API."""
    store = request.app.state.lakebase_store
    if not store:
        return JSONResponse({"error": "Store not available"}, status_code=503)

    return JSONResponse(store.get_stats())


@app.get("/api/graph")
async def api_graph(center: str | None = None, request: Request = None) -> JSONResponse:
    """Get knowledge graph data for Cytoscape.js."""
    store = request.app.state.lakebase_store
    if not store:
        return JSONResponse({"error": "Store not available"}, status_code=503)

    data = store.get_graph_data(center_page_id=center)
    return JSONResponse(data)


# ──────────────────────────────────────────────
# Web UI routes (imported from routes.py)
# ──────────────────────────────────────────────

# Import and include web UI routes
try:
    from llm_wiki.app.routes import router as web_router

    app.include_router(web_router)
except ImportError:
    logger.debug("Web UI routes not available")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main() -> None:
    """Run the server."""
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
