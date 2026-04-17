"""Web UI routes for the LLM Wiki browser interface.

Provides Wikipedia-style page browsing, search, knowledge graph,
and a page editor.
"""

from __future__ import annotations

from pathlib import Path

import markdown
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from llm_wiki.log import logger

_TEMPLATE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))

router = APIRouter()


def _get_store(request: Request):
    """Get the best available store (Lakebase or Delta fallback)."""
    return request.app.state.lakebase_store or request.app.state.delta_store


def _render_markdown(content: str, existing_page_ids: set[str] | None = None) -> str:
    """Render markdown to HTML, converting [[wikilinks]] to <a> tags.

    Links to pages that exist get the `wikilink` CSS class (blue).
    Links to missing pages get the `redlink` class (styled differently)
    and a `title` tooltip explaining the page doesn't exist.

    Args:
        content: Markdown content.
        existing_page_ids: Set of known page IDs. If None, all links render as wikilinks.

    Returns:
        HTML string.
    """
    import re

    existing = existing_page_ids or set()

    def wikilink_replace(match: re.Match) -> str:
        slug = match.group(1)
        display = match.group(3) if match.group(3) else slug.replace("-", " ").title()
        if existing_page_ids is not None and slug not in existing:
            return (
                f'<a href="/page/{slug}" class="redlink" '
                f'title="Red link: page &quot;{slug}&quot; does not exist yet">{display}</a>'
            )
        return f'<a href="/page/{slug}" class="wikilink">{display}</a>'

    content = re.sub(
        r"\[\[([a-z0-9][a-z0-9-]*)(\|([^\]]+))?\]\]",
        wikilink_replace,
        content,
    )

    return markdown.markdown(
        content,
        extensions=["fenced_code", "tables", "toc"],
    )


def _fetch_page_ids(store) -> set[str]:
    """Fetch the set of all existing page_ids (cached per-request via store)."""
    try:
        pages = store.list_pages(limit=5000)
        return {p.page_id for p in pages}
    except Exception:
        return set()


@router.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    """Homepage: recent pages, search bar, stats."""
    store = _get_store(request)
    context: dict = {"request": request, "pages": [], "stats": {}}

    if store:
        try:
            context["pages"] = store.list_pages(limit=20)
            context["stats"] = store.get_stats()
        except Exception as e:
            logger.warning("Could not load homepage data", error=str(e))

    return templates.TemplateResponse("home.html", context)


@router.get("/page/{page_id}", response_class=HTMLResponse)
async def page_view(page_id: str, request: Request) -> HTMLResponse:
    """View a wiki page with rendered markdown and backlinks."""
    store = _get_store(request)
    page = store.get_page(page_id) if store else None
    if not page:
        return templates.TemplateResponse("page.html", {
            "request": request,
            "page": None,
            "page_id": page_id,
            "content_html": "",
            "backlinks": [],
        })

    existing_ids = _fetch_page_ids(store)
    content_html = _render_markdown(page.content_markdown, existing_page_ids=existing_ids)
    try:
        backlinks = store.get_backlinks(page_id)
    except Exception:
        backlinks = []

    return templates.TemplateResponse("page.html", {
        "request": request,
        "page": page,
        "page_id": page_id,
        "content_html": content_html,
        "backlinks": backlinks,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, q: str = "") -> HTMLResponse:
    """Search results page."""
    search_engine = request.app.state.search
    results = []

    if q and search_engine:
        try:
            results = search_engine.search(q, limit=20)
        except Exception as e:
            logger.warning("Search failed", error=str(e))

    return templates.TemplateResponse("search.html", {
        "request": request,
        "query": q,
        "results": results,
    })


@router.get("/graph", response_class=HTMLResponse)
async def graph_view(request: Request, center: str | None = None) -> HTMLResponse:
    """Interactive knowledge graph visualization."""
    return templates.TemplateResponse("graph.html", {
        "request": request,
        "center": center,
    })


@router.get("/edit/{page_id}", response_class=HTMLResponse)
async def edit_page(page_id: str, request: Request) -> HTMLResponse:
    """Page editor with live markdown preview."""
    store = _get_store(request)
    page = store.get_page(page_id) if store else None

    return templates.TemplateResponse("edit.html", {
        "request": request,
        "page": page,
        "page_id": page_id,
    })


@router.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request) -> HTMLResponse:
    """Wiki statistics dashboard."""
    store = _get_store(request)
    stats = store.get_stats() if store else {}

    return templates.TemplateResponse("home.html", {
        "request": request,
        "pages": [],
        "stats": stats,
    })
