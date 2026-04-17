"""MCP tool definitions for the LLM Wiki server.

Exposes wiki operations as MCP tools that can be used by Claude Code,
Claude Desktop, and other MCP-compatible clients via Streamable HTTP
transport at /mcp.

Usage:
    from llm_wiki.app.tools import build_mcp_server

    mcp = build_mcp_server(delta_store, volume_store, search, query_engine, config)
    app.mount("/", mcp.streamable_http_app())
"""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from llm_wiki.config import WikiConfig
from llm_wiki.operations.lint import WikiLinter
from llm_wiki.search import WikiSearch
from llm_wiki.storage.delta import DeltaStore
from llm_wiki.storage.volumes import VolumeStore


def build_mcp_server(
    delta_store: DeltaStore,
    volume_store: VolumeStore,
    search: WikiSearch,
    query_engine: Any,
    config: WikiConfig,
    name: str = "llm-wiki",
) -> FastMCP:
    """Build and return a FastMCP server with all wiki tools registered.

    The server exposes 7 tools that wrap the wiki's core operations.
    Mount via `app.mount("/", mcp.streamable_http_app())`.

    Args:
        delta_store: Delta store for reads and writes.
        volume_store: Volume store for source uploads.
        search: WikiSearch instance.
        query_engine: QueryEngine instance (may be None if LLM unavailable).
        config: Wiki configuration.
        name: Name advertised by the MCP server.

    Returns:
        Configured FastMCP instance (not yet mounted).
    """
    mcp = FastMCP(name, stateless_http=True, json_response=True)

    # ──────────────────────────────────────────────
    # wiki_search
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_search(query: str, limit: int = 10, mode: str = "hybrid") -> str:
        """Search the wiki for pages matching a query.

        Args:
            query: Search query string.
            limit: Maximum number of results (default 10).
            mode: Search mode - 'fulltext', 'semantic', or 'hybrid' (default).

        Returns:
            Formatted search results with page IDs, titles, and snippets.
        """
        results = search.search(query, limit=limit, mode=mode)

        if not results:
            return f"No results found for '{query}'."

        lines = [f"Found {len(results)} results for '{query}':\n"]
        for r in results:
            lines.append(f"- **[[{r.page_id}]]** {r.title}")
            if r.snippet:
                lines.append(f"  {r.snippet[:150]}...")
            lines.append(f"  (type: {r.page_type}, score: {r.score:.3f})")
        return "\n".join(lines)

    # ──────────────────────────────────────────────
    # wiki_read
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_read(page_id: str) -> str:
        """Read a specific wiki page by its slug.

        Args:
            page_id: The page slug (e.g., 'gandalf' or 'the-one-ring').

        Returns:
            Full page content with metadata and backlinks.
        """
        page = delta_store.get_page(page_id)
        if not page:
            return f"Page '{page_id}' not found."

        try:
            backlinks = delta_store.get_backlinks(page_id)
        except Exception:
            backlinks = []

        backlink_text = ""
        if backlinks:
            bl_list = ", ".join(f"[[{bl.source_page_id}]]" for bl in backlinks[:10])
            backlink_text = f"\n\n---\n**Backlinks**: {bl_list}"

        return (
            f"# {page.title}\n\n"
            f"**Type**: {page.page_type.value} | "
            f"**Confidence**: {page.confidence.value} | "
            f"**Freshness**: {page.freshness_tier.value}\n\n"
            f"{page.content_markdown}"
            f"{backlink_text}"
        )

    # ──────────────────────────────────────────────
    # wiki_ingest
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_ingest(
        text: str | None = None,
        url: str | None = None,
        title: str | None = None,
    ) -> str:
        """Ingest a new source into the wiki.

        Provide either text content or a URL. The source is uploaded to the
        incoming volume and enqueued for LLM compilation. Run the compile
        job afterwards to produce a wiki page.

        Args:
            text: Raw text content to ingest.
            url: URL to fetch and ingest.
            title: Optional title for the source.

        Returns:
            Status message with the created page slug.
        """
        from llm_wiki.operations.ingest import ingest_source

        result = ingest_source(
            volume_store=volume_store,
            delta_store=delta_store,
            text=text,
            url=url,
            title=title,
        )

        if result["status"] == "ingested":
            return (
                f"Source ingested successfully!\n"
                f"- Slug: {result['slug']}\n"
                f"- Title: {result.get('title', 'untitled')}\n"
                f"- Path: {result['source_path']}\n"
                f"- Queue ID: {result['queue_id']}\n\n"
                f"Run the compilation job to compile this into a wiki page."
            )
        return f"Ingestion failed: {result['status']}"

    # ──────────────────────────────────────────────
    # wiki_query
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_query(question: str) -> str:
        """Ask a question and get a cited answer synthesized from wiki pages.

        Searches the wiki, assembles relevant context, and uses the LLM to
        synthesize an answer with [[page-slug]] citations.

        Args:
            question: The question to answer.

        Returns:
            Answer with [[page-slug]] citations.
        """
        if query_engine is None:
            return "Query engine not available on this deployment."
        result = query_engine.query(question)
        return result["answer"]

    # ──────────────────────────────────────────────
    # wiki_lint
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_lint() -> str:
        """Run wiki health checks and return a report.

        Checks for: stale pages, broken links, orphan pages, low confidence,
        and thin content.

        Returns:
            Formatted lint report with issues and summary.
        """
        linter = WikiLinter(delta_store, config)
        report = linter.run()

        lines = [
            f"## Wiki Lint Report\n",
            f"Pages checked: {report['pages_checked']}",
            f"Issues found: {report['total_issues']}\n",
        ]

        if report["issues"]:
            lines.append("### Issues\n")
            for issue in report["issues"][:20]:
                icon = {"error": "X", "warning": "!", "info": "i"}.get(issue["severity"], "?")
                lines.append(
                    f"- [{icon}] **{issue['page_id']}** ({issue['category']}): {issue['message']}"
                )
            if report["total_issues"] > 20:
                lines.append(f"\n... and {report['total_issues'] - 20} more issues")
        else:
            lines.append("No issues found. Wiki is healthy!")

        return "\n".join(lines)

    # ──────────────────────────────────────────────
    # wiki_list
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_list(
        page_type: str | None = None,
        tag: str | None = None,
        limit: int = 50,
    ) -> str:
        """List wiki pages with optional filtering.

        Args:
            page_type: Filter by type (concept, entity, source, analysis, index).
            tag: Filter by tag.
            limit: Maximum results (default 50).

        Returns:
            Formatted list of pages.
        """
        pages = delta_store.list_pages(page_type=page_type, tag=tag, limit=limit)

        if not pages:
            return "No pages found matching the criteria."

        lines = [f"## Wiki Pages ({len(pages)} results)\n"]
        for p in pages:
            updated = p.updated_at.strftime("%Y-%m-%d") if p.updated_at else "unknown"
            lines.append(
                f"- **[[{p.page_id}]]** {p.title} "
                f"({p.page_type.value}, {p.confidence.value}, updated {updated})"
            )

        return "\n".join(lines)

    # ──────────────────────────────────────────────
    # wiki_stats
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_stats() -> str:
        """Get wiki statistics: page counts, type distribution, confidence breakdown.

        Returns:
            Formatted statistics summary.
        """
        stats = delta_store.get_stats()

        lines = [
            "## Wiki Statistics\n",
            f"**Total pages**: {stats.get('total_pages', 0)}",
            f"**Total backlinks**: {stats.get('total_backlinks', 0)}",
            f"**Pending compilations**: {stats.get('pending_compilations', 0)}",
            "",
            "### By Type",
        ]
        for ptype, count in sorted(stats.get("by_type", {}).items()):
            lines.append(f"- {ptype}: {count}")

        lines.append("\n### By Confidence")
        for conf, count in sorted(stats.get("by_confidence", {}).items()):
            lines.append(f"- {conf}: {count}")

        return "\n".join(lines)

    return mcp
