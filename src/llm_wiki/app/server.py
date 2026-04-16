"""FastAPI application serving MCP tools, web UI, and REST API.

Entry point for the Databricks App. Serves:
- Web UI at / (Wikipedia-style browseable interface)
- REST API at /api/* (programmatic access)
- Health check at /health

Uses DeltaStore (Databricks SDK Statement Execution API) for all reads.
Lakebase is optional and only used when explicitly available.

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

from llm_wiki.config import get_config
from llm_wiki.log import logger

_APP_DIR = Path(__file__).parent
_STATIC_DIR = _APP_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: lightweight init, no blocking calls."""
    config = get_config(os.environ.get("WIKI_CONFIG_PATH"))

    catalog = os.environ.get("WIKI_CATALOG", config.wiki.catalog)
    wiki_schema = os.environ.get("WIKI_SCHEMA", "wiki")
    raw_schema = os.environ.get("RAW_SCHEMA", "raw_sources")

    # DeltaStore uses Statement Execution API - lightweight init, no warehouse listing
    from llm_wiki.storage.delta import DeltaStore
    from llm_wiki.storage.volumes import VolumeStore

    delta_store = DeltaStore(catalog=catalog, wiki_schema=wiki_schema, raw_schema=raw_schema)
    volume_store = VolumeStore(catalog=catalog, raw_schema=raw_schema, wiki_schema=wiki_schema)

    # Search uses DeltaStore as fallback (no Lakebase)
    from llm_wiki.search import WikiSearch

    search = WikiSearch(delta_store=delta_store, vs_index_name=config.vector_search.index_name)

    app.state.config = config
    app.state.delta_store = delta_store
    app.state.lakebase_store = None
    app.state.volume_store = volume_store
    app.state.search = search

    logger.info("LLM Wiki server started", catalog=catalog, schema=wiki_schema)
    yield
    logger.info("LLM Wiki server stopped")


app = FastAPI(
    title="LLM Wiki",
    description="Karpathy's LLM Wiki pattern on Databricks",
    version="0.1.0",
    lifespan=lifespan,
)

if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


def _get_store(request: Request) -> Any:
    """Get the best available store."""
    return request.app.state.lakebase_store or request.app.state.delta_store


# ──────────────────────────────────────────────
# Health check (fast - no DB calls)
# ──────────────────────────────────────────────

@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "service": "llm-wiki", "backend": "delta"}


# ──────────────────────────────────────────────
# REST API
# ──────────────────────────────────────────────

@app.get("/api/search")
async def api_search(q: str, limit: int = 20, request: Request = None) -> JSONResponse:
    """Search the wiki."""
    search = request.app.state.search
    results = search.search(q, limit=limit, mode="fulltext")
    return JSONResponse([r.model_dump() for r in results])


@app.get("/api/pages/{page_id}")
async def api_get_page(page_id: str, request: Request = None) -> JSONResponse:
    """Get a specific page."""
    store = _get_store(request)
    page = store.get_page(page_id)
    if not page:
        return JSONResponse({"error": "Page not found"}, status_code=404)
    return JSONResponse(page.model_dump(mode="json"))


@app.get("/api/stats")
async def api_stats(request: Request = None) -> JSONResponse:
    """Get wiki statistics."""
    store = _get_store(request)
    return JSONResponse(store.get_stats())


@app.get("/api/graph")
async def api_graph(center: str | None = None, request: Request = None) -> JSONResponse:
    """Get knowledge graph data for Cytoscape.js."""
    delta = request.app.state.delta_store
    pages = delta.list_pages(limit=200)
    nodes = [{"data": {"id": p.page_id, "label": p.title, "type": p.page_type.value}} for p in pages]
    edges = []
    for p in pages:
        for link in p.wikilinks:
            edges.append({"data": {"source": p.page_id, "target": link}})
    return JSONResponse({"nodes": nodes, "edges": edges})


# ──────────────────────────────────────────────
# Web UI routes
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
