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
    delta_store: Any,
    volume_store: VolumeStore,
    search: WikiSearch,
    query_engine: Any,
    config: WikiConfig,
    write_store: DeltaStore | None = None,
    name: str = "llm-wiki",
) -> FastMCP:
    """Build and return a FastMCP server with all wiki tools registered.

    The server exposes tools that wrap the wiki's core operations.
    Mount via `app.mount("/", mcp.streamable_http_app())`.

    Args:
        delta_store: Read store (LakebaseStore or DeltaStore). Used for
                     get_page, list_pages, search, stats, index.
        volume_store: Volume store for source uploads.
        search: WikiSearch instance.
        query_engine: QueryEngine instance (may be None if LLM unavailable).
        config: Wiki configuration.
        write_store: DeltaStore for operations that modify data (ingest).
                     Defaults to `delta_store` when not provided.
        name: Name advertised by the MCP server.

    Returns:
        Configured FastMCP instance (not yet mounted).
    """
    write_store = write_store or delta_store
    mcp = FastMCP(name, stateless_http=True, json_response=True)

    # ──────────────────────────────────────────────
    # wiki_index  (Karpathy's "first stop" for the agent)
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_index(page_type: str | None = None) -> str:
        """Return the wiki index - the compact catalog of all pages.

        **Start here before answering any question.** The index lists every
        page in the wiki with a one-line summary, grouped by page type. Use
        it to identify which pages are most relevant to the user's question,
        then call `wiki_read` on the specific ones you need.

        This is more reliable than `wiki_search` for broad or multi-topic
        questions because you see the complete landscape rather than a
        top-K vector match.

        Args:
            page_type: Optional filter (concept, entity, source, analysis, index).

        Returns:
            Markdown-formatted index, grouped by page_type.
        """
        if not hasattr(delta_store, "get_index"):
            return "wiki_index: not supported by current store backend"

        entries = delta_store.get_index()
        if page_type:
            entries = [e for e in entries if e["page_type"] == page_type]

        if not entries:
            return "Wiki is empty."

        # Group by type
        by_type: dict[str, list[dict]] = {}
        for e in entries:
            by_type.setdefault(e["page_type"], []).append(e)

        lines = [f"# Wiki Index ({len(entries)} pages)\n"]
        for ptype in sorted(by_type.keys()):
            lines.append(f"\n## {ptype.title()}\n")
            for e in sorted(by_type[ptype], key=lambda x: x["title"]):
                summary = e["summary"] or "(no summary available)"
                lines.append(f"- **[[{e['page_id']}]]** {e['title']} — {summary}")

        return "\n".join(lines)

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
            delta_store=write_store,
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
    def wiki_query(question: str, use_index: bool = True) -> str:
        """Ask a question and get a cited answer synthesized from wiki pages.

        Follows Karpathy's index-first pattern:
          1. LLM reads the wiki index and picks relevant pages.
          2. Those pages are loaded in full.
          3. Answer synthesized with [[page-slug]] citations.

        Falls back to vector search if the index path yields nothing.

        Args:
            question: The natural-language question.
            use_index: If True (default), use index-first retrieval. If False,
                       skip directly to vector search.

        Returns:
            Cited answer. Includes a footer with retrieval path + pages consulted.
        """
        if query_engine is None:
            return "Query engine not available on this deployment."
        result = query_engine.query(question, use_index=use_index)

        footer = (
            f"\n\n---\n"
            f"_Retrieval: **{result['retrieval_path']}** | "
            f"Pages consulted: {', '.join(f'[[{p}]]' for p in result['pages_used'][:8])}_"
        )
        return result["answer"] + footer

    # ──────────────────────────────────────────────
    # wiki_file_answer  (self-reinforcing loop)
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_file_answer(question: str, answer: str, page_type: str = "analysis") -> str:
        """File a valuable query answer back into the wiki as a new page.

        Karpathy's principle: "good answers shouldn't vanish into chat history -
        they should be filed back as new wiki pages." Use this after answering
        a substantive question that isn't already covered by existing pages, so
        the knowledge compounds over time.

        This ingests the answer as a source document (with the question as
        title). It will be processed by the SDP pipeline and compiled into a
        full wiki page on the next compile run.

        Args:
            question: The original question (becomes the page title).
            answer: The synthesized answer to file.
            page_type: Default "analysis" since these are cross-cutting syntheses.

        Returns:
            Status with the new page slug.
        """
        from llm_wiki.operations.ingest import ingest_source

        body = (
            f"# {question}\n\n"
            f"*This page was filed from a wiki_query answer. It represents "
            f"synthesized knowledge, not raw source material.*\n\n"
            f"{answer}"
        )
        result = ingest_source(
            volume_store=volume_store,
            delta_store=write_store,
            text=body,
            title=question,
        )
        if result["status"] == "ingested":
            return (
                f"Answer filed as future wiki page.\n"
                f"- Slug: **{result['slug']}**\n"
                f"- Queue ID: {result['queue_id']}\n"
                f"- Page type (requested): {page_type}\n\n"
                f"Run the compile job to materialize the page."
            )
        return f"Failed to file answer: {result['status']}"

    # ──────────────────────────────────────────────
    # wiki_lint
    # ──────────────────────────────────────────────
    @mcp.tool()
    def wiki_lint(check_contradictions: bool = False) -> str:
        """Run wiki health checks and return a report.

        Fast checks (always run): stale pages, broken links, orphans,
        low confidence, thin content.

        Optional: `check_contradictions=true` runs an LLM-based check on up to
        20 linked page pairs to identify factual conflicts. Costs LLM tokens.

        Args:
            check_contradictions: Enable LLM-based contradiction detection.

        Returns:
            Formatted lint report.
        """
        # Use write_store (real DeltaStore) for lint so log_activity works
        linter = WikiLinter(write_store, config)
        report = linter.run(check_contradictions=check_contradictions)

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
