"""MCP tool definitions for the LLM Wiki server.

Exposes wiki operations as MCP tools that can be used by
Claude Code, Claude Desktop, and other MCP-compatible clients.

Usage:
    from llm_wiki.app.tools import register_tools

    register_tools(mcp, lakebase_store, delta_store, ...)
"""

from __future__ import annotations

from typing import Any

from mcp.server import Server

from llm_wiki.config import WikiConfig
from llm_wiki.log import logger
from llm_wiki.operations.lint import WikiLinter
from llm_wiki.operations.query import QueryEngine
from llm_wiki.search import WikiSearch
from llm_wiki.storage.delta import DeltaStore
from llm_wiki.storage.volumes import VolumeStore


def register_tools(
    mcp: Server,
    read_store: Any,
    delta_store: DeltaStore,
    volume_store: VolumeStore,
    search: WikiSearch,
    query_engine: QueryEngine,
    config: WikiConfig,
) -> None:
    """Register all MCP tools on the server.

    Args:
        mcp: The MCP server instance.
        read_store: Store for reads (LakebaseStore or DeltaStore).
        delta_store: Delta store for writes.
        volume_store: Volume store for source uploads.
        search: WikiSearch instance.
        query_engine: QueryEngine instance.
        config: Wiki configuration.
    """

    @mcp.tool()
    async def wiki_search(query: str, limit: int = 20, mode: str = "hybrid") -> str:
        """Search the wiki for pages matching a query.

        Uses hybrid search (full-text + semantic) by default.

        Args:
            query: Search query string.
            limit: Maximum number of results (default 20).
            mode: Search mode - 'fulltext', 'semantic', or 'hybrid'.

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

    @mcp.tool()
    async def wiki_read(page_id: str) -> str:
        """Read a specific wiki page by its slug.

        Args:
            page_id: The page slug (e.g., 'kubernetes-scheduling').

        Returns:
            Full page content with frontmatter, or error message if not found.
        """
        page = read_store.get_page(page_id)
        if not page:
            return f"Page '{page_id}' not found."

        # Get backlinks
        backlinks = read_store.get_backlinks(page_id)
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

    @mcp.tool()
    async def wiki_ingest(
        text: str | None = None,
        url: str | None = None,
        title: str | None = None,
    ) -> str:
        """Ingest a new source into the wiki.

        Provide either text content or a URL to fetch. The source will be
        uploaded to the incoming volume and enqueued for compilation.

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
                f"- **Slug**: {result['slug']}\n"
                f"- **Title**: {result.get('title', 'untitled')}\n"
                f"- **Path**: {result['source_path']}\n"
                f"- **Queue ID**: {result['queue_id']}\n\n"
                f"Run the compilation job to compile this into a wiki page."
            )
        return f"Ingestion failed: {result['status']}"

    @mcp.tool()
    async def wiki_query(question: str) -> str:
        """Ask a question and get an answer synthesized from wiki pages.

        Searches the wiki, assembles relevant context, and uses the LLM
        to synthesize a cited answer.

        Args:
            question: The question to answer.

        Returns:
            Answer with [[page-slug]] citations.
        """
        result = query_engine.query(question)
        return result["answer"]

    @mcp.tool()
    async def wiki_lint() -> str:
        """Run wiki health checks and return a report.

        Checks for: stale pages, broken links, orphans, low confidence,
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
                severity_icon = {"error": "X", "warning": "!", "info": "i"}.get(issue["severity"], "?")
                lines.append(
                    f"- [{severity_icon}] **{issue['page_id']}** ({issue['category']}): {issue['message']}"
                )
            if report["total_issues"] > 20:
                lines.append(f"\n... and {report['total_issues'] - 20} more issues")
        else:
            lines.append("No issues found. Wiki is healthy!")

        return "\n".join(lines)

    @mcp.tool()
    async def wiki_list(page_type: str | None = None, tag: str | None = None, limit: int = 50) -> str:
        """List wiki pages with optional filtering.

        Args:
            page_type: Filter by type (concept, entity, source, analysis, index).
            tag: Filter by tag.
            limit: Maximum results (default 50).

        Returns:
            Formatted list of pages.
        """
        pages = read_store.list_pages(page_type=page_type, tag=tag, limit=limit)

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

    @mcp.tool()
    async def wiki_stats() -> str:
        """Get wiki statistics: page counts, type distribution, health.

        Returns:
            Formatted statistics summary.
        """
        stats = read_store.get_stats()

        lines = [
            "## Wiki Statistics\n",
            f"**Total pages**: {stats.get('total_pages', 0)}",
            f"**Total backlinks**: {stats.get('total_backlinks', 0)}",
            "",
            "### By Type",
        ]
        for ptype, count in sorted(stats.get("by_type", {}).items()):
            lines.append(f"- {ptype}: {count}")

        lines.append("\n### By Confidence")
        for conf, count in sorted(stats.get("by_confidence", {}).items()):
            lines.append(f"- {conf}: {count}")

        return "\n".join(lines)
