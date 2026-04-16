"""FastAPI application serving MCP tools, web UI, and REST API.

Entry point for the Databricks App. Serves:
- MCP endpoint at /mcp (Streamable HTTP transport)
- Web UI at / (Wikipedia-style browseable interface)
- REST API at /api/* (programmatic access)

When Lakebase is unavailable, falls back to DeltaStore for all reads.

Usage:
    python -m llm_wiki.app.server
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from llm_wiki.config import get_config, WikiConfig
from llm_wiki.log import logger

# Resolve paths relative to this file
_APP_DIR = Path(__file__).parent
_TEMPLATE_DIR = _APP_DIR / "templates"
_STATIC_DIR = _APP_DIR / "static"


def _get_store(request: Request) -> Any:
    """Get the best available store (Lakebase or Delta fallback)."""
    return request.app.state.lakebase_store or request.app.state.delta_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize stores and MCP on startup."""
    config = get_config(os.environ.get("WIKI_CONFIG_PATH"))

    catalog = os.environ.get("WIKI_CATALOG", config.wiki.catalog)
    wiki_schema = os.environ.get("WIKI_SCHEMA", "wiki")
    raw_schema = os.environ.get("RAW_SCHEMA", "raw_sources")
    lakebase_instance = os.environ.get("LAKEBASE_INSTANCE", config.lakebase.instance_name)

    # Initialize stores
    from llm_wiki.storage.delta import DeltaStore
    from llm_wiki.storage.volumes import VolumeStore

    delta_store = DeltaStore(catalog=catalog, wiki_schema=wiki_schema, raw_schema=raw_schema)
    volume_store = VolumeStore(catalog=catalog, raw_schema=raw_schema, wiki_schema=wiki_schema)

    # Try Lakebase - optional, falls back to Delta
    lakebase_store = None
    try:
        from llm_wiki.storage.lakebase import LakebaseStore
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        lb_info = w.lakebase.get_database_instance(lakebase_instance)
        lakebase_store = LakebaseStore(
            host=lb_info.host,
            port=lb_info.port or 5432,
            database="wiki",
        )
        logger.info("Lakebase connected", instance=lakebase_instance)
    except Exception as e:
        logger.warning("Lakebase unavailable, using Delta tables for serving", error=str(e))

    # Initialize search - works with Lakebase or Delta fallback
    from llm_wiki.search import WikiSearch
    from llm_wiki.operations.query import QueryEngine

    search = WikiSearch(
        lakebase_store=lakebase_store,
        delta_store=delta_store,
        vs_index_name=config.vector_search.index_name,
    )

    query_engine = QueryEngine(
        search=search,
        delta_store=delta_store,
        config=config,
    )

    # Store in app state
    app.state.config = config
    app.state.delta_store = delta_store
    app.state.lakebase_store = lakebase_store
    app.state.volume_store = volume_store
    app.state.search = search
    app.state.query_engine = query_engine

    # Register MCP tools
    try:
        from mcp.server import Server
        from llm_wiki.app.tools import register_tools

        mcp = Server("llm-wiki")
        store = lakebase_store or delta_store
        register_tools(mcp, store, delta_store, volume_store, search, query_engine, config)
        app.state.mcp = mcp
        logger.info("MCP tools registered")
    except Exception as e:
        logger.warning("MCP registration failed", error=str(e))

    backend = "Lakebase" if lakebase_store else "Delta"
    logger.info("LLM Wiki server started", catalog=catalog, schema=wiki_schema, backend=backend)

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


# ──────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────

@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    """Health check endpoint."""
    backend = "lakebase" if request.app.state.lakebase_store else "delta"
    return {"status": "healthy", "service": "llm-wiki", "backend": backend}


# ──────────────────────────────────────────────
# REST API
# ──────────────────────────────────────────────

@app.get("/api/search")
async def api_search(q: str, limit: int = 20, request: Request = None) -> JSONResponse:
    """Search the wiki via REST API."""
    search = request.app.state.search
    results = search.search(q, limit=limit)
    return JSONResponse([r.model_dump() for r in results])


@app.get("/api/pages/{page_id}")
async def api_get_page(page_id: str, request: Request = None) -> JSONResponse:
    """Get a specific page via REST API."""
    store = _get_store(request)
    page = store.get_page(page_id)
    if not page:
        return JSONResponse({"error": "Page not found"}, status_code=404)
    return JSONResponse(page.model_dump(mode="json"))


@app.get("/api/stats")
async def api_stats(request: Request = None) -> JSONResponse:
    """Get wiki statistics via REST API."""
    store = _get_store(request)
    return JSONResponse(store.get_stats())


@app.get("/api/graph")
async def api_graph(center: str | None = None, request: Request = None) -> JSONResponse:
    """Get knowledge graph data for Cytoscape.js."""
    store = request.app.state.lakebase_store
    if not store:
        # Delta doesn't have graph_data method - return basic node list
        delta = request.app.state.delta_store
        pages = delta.list_pages(limit=200)
        nodes = [{"data": {"id": p.page_id, "label": p.title, "type": p.page_type.value}} for p in pages]
        edges = []
        for p in pages:
            for link in p.wikilinks:
                edges.append({"data": {"source": p.page_id, "target": link}})
        return JSONResponse({"nodes": nodes, "edges": edges})
    return JSONResponse(store.get_graph_data(center_page_id=center))


# ──────────────────────────────────────────────
# Web UI routes (imported from routes.py)
# ──────────────────────────────────────────────

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
