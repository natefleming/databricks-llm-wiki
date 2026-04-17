"""FastAPI application serving MCP tools, web UI, and REST API.

Entry point for the Databricks App. Serves:
- MCP endpoint at /mcp (Streamable HTTP transport for Claude Code / Desktop)
- Web UI at / (Wikipedia-style browseable interface)
- REST API at /api/* (programmatic access)
- Health check at /health

Uses DeltaStore (Databricks SDK Statement Execution API) for all reads.

Usage:
    python -m llm_wiki.app.server
"""

from __future__ import annotations

import contextlib
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


def _build_state() -> dict[str, Any]:
    """Construct wiki stores, search, query engine, and MCP server."""
    config = get_config(os.environ.get("WIKI_CONFIG_PATH"))

    catalog = os.environ.get("WIKI_CATALOG", config.wiki.catalog)
    wiki_schema = os.environ.get("WIKI_SCHEMA", "wiki")
    raw_schema = os.environ.get("RAW_SCHEMA", "raw_sources")

    from llm_wiki.storage.delta import DeltaStore
    from llm_wiki.storage.volumes import VolumeStore

    delta_store = DeltaStore(catalog=catalog, wiki_schema=wiki_schema, raw_schema=raw_schema)
    volume_store = VolumeStore(catalog=catalog, raw_schema=raw_schema, wiki_schema=wiki_schema)

    from llm_wiki.search import WikiSearch

    search = WikiSearch(delta_store=delta_store, vs_index_name=config.vector_search.index_name)

    # QueryEngine is optional (requires FMAPI access)
    query_engine = None
    try:
        from llm_wiki.operations.query import QueryEngine
        query_engine = QueryEngine(search=search, delta_store=delta_store, config=config)
    except Exception as e:
        logger.warning("QueryEngine unavailable", error=str(e))

    # Build MCP server
    from llm_wiki.app.tools import build_mcp_server

    mcp = build_mcp_server(
        delta_store=delta_store,
        volume_store=volume_store,
        search=search,
        query_engine=query_engine,
        config=config,
    )

    return {
        "config": config,
        "delta_store": delta_store,
        "volume_store": volume_store,
        "search": search,
        "query_engine": query_engine,
        "mcp": mcp,
    }


# Build state at module load so MCP's streamable_http_app is available for mounting
_state = _build_state()
_mcp_app = _state["mcp"].streamable_http_app()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run MCP session manager alongside FastAPI lifespan."""
    # Attach state to the app
    app.state.config = _state["config"]
    app.state.delta_store = _state["delta_store"]
    app.state.lakebase_store = None
    app.state.volume_store = _state["volume_store"]
    app.state.search = _state["search"]
    app.state.query_engine = _state["query_engine"]
    app.state.mcp = _state["mcp"]

    catalog = app.state.config.wiki.catalog
    logger.info("LLM Wiki server starting", catalog=catalog)

    # Run the MCP session manager inside the FastAPI lifespan
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(_state["mcp"].session_manager.run())
        logger.info("MCP session manager running")
        yield
        logger.info("LLM Wiki server stopping")


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
    return {
        "status": "healthy",
        "service": "llm-wiki",
        "backend": "delta",
        "mcp": "enabled",
    }


# ──────────────────────────────────────────────
# REST API
# ──────────────────────────────────────────────

@app.get("/api/search")
async def api_search(
    q: str,
    limit: int = 20,
    mode: str = "hybrid",
    request: Request = None,
) -> JSONResponse:
    """Search the wiki. Modes: fulltext, semantic, hybrid (default)."""
    search = request.app.state.search
    results = search.search(q, limit=limit, mode=mode)
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
    try:
        delta = request.app.state.delta_store
        pages = delta.list_pages(limit=200)
        page_ids = {p.page_id for p in pages}
        nodes = [{"data": {"id": p.page_id, "label": p.title, "type": p.page_type.value}} for p in pages]
        edges = []
        for p in pages:
            for link in p.wikilinks:
                if link in page_ids:
                    edges.append({"data": {"source": p.page_id, "target": link}})
        return JSONResponse({"nodes": nodes, "edges": edges})
    except Exception as e:
        logger.error("Graph API failed", error=str(e))
        return JSONResponse({"nodes": [], "edges": [], "error": str(e)})


# ──────────────────────────────────────────────
# Web UI routes
# ──────────────────────────────────────────────

try:
    from llm_wiki.app.routes import router as web_router
    app.include_router(web_router)
except ImportError:
    logger.debug("Web UI routes not available")


# ──────────────────────────────────────────────
# Mount MCP Streamable HTTP transport at /mcp
# ──────────────────────────────────────────────

# FastMCP's streamable_http_app already serves at its own /mcp path,
# so mount at root.
app.mount("/", _mcp_app)


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
