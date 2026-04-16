"""Lakebase (Postgres) storage for fast wiki serving.

Provides sub-10ms point lookups and full-text search via pg_trgm/tsvector
for the MCP server and web UI.

Usage:
    from llm_wiki.storage.lakebase import LakebaseStore

    store = LakebaseStore(host="...", database="wiki")
    results = store.search_pages("kubernetes scheduling")
"""

from __future__ import annotations

import json
import os
from typing import Any

from psycopg import sql
from psycopg_pool import ConnectionPool

from llm_wiki.log import logger
from llm_wiki.models import BackLink, Page, SearchResult


class LakebaseStore:
    """Lakebase/Postgres storage backend for fast wiki serving.

    Provides full-text search via pg_trgm and tsvector indexes,
    and sub-10ms point lookups for the MCP server and web UI.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int = 5432,
        database: str = "wiki",
        user: str | None = None,
        password: str | None = None,
        min_size: int = 2,
        max_size: int = 10,
    ) -> None:
        """Initialize the Lakebase store with a connection pool.

        Args:
            host: Lakebase host. Defaults to LAKEBASE_HOST env var.
            port: Lakebase port.
            database: Database name.
            user: Database user. Defaults to LAKEBASE_USER env var.
            password: Database password. Defaults to LAKEBASE_PASSWORD env var.
            min_size: Minimum pool connections.
            max_size: Maximum pool connections.
        """
        self._host = host or os.environ.get("LAKEBASE_HOST", "localhost")
        self._port = port
        self._database = database
        self._user = user or os.environ.get("LAKEBASE_USER", "")
        self._password = password or os.environ.get("LAKEBASE_PASSWORD", "")

        conninfo = (
            f"host={self._host} port={self._port} dbname={self._database} "
            f"user={self._user} password={self._password}"
        )
        self._pool = ConnectionPool(conninfo=conninfo, min_size=min_size, max_size=max_size)
        logger.info("Lakebase pool initialized", host=self._host, database=self._database)

    def close(self) -> None:
        """Close the connection pool."""
        self._pool.close()

    # ──────────────────────────────────────────────
    # Page operations
    # ──────────────────────────────────────────────

    def get_page(self, page_id: str) -> Page | None:
        """Retrieve a page by its slug.

        Args:
            page_id: The page slug identifier.

        Returns:
            Page instance or None if not found.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT page_id, title, page_type, content_markdown, frontmatter,
                           confidence, sources, related, tags, freshness_tier,
                           created_at, updated_at
                    FROM pages
                    WHERE page_id = %s
                    """,
                    (page_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return self._row_to_page(row)

    def list_pages(
        self,
        page_type: str | None = None,
        tag: str | None = None,
        limit: int = 100,
    ) -> list[Page]:
        """List pages with optional filtering.

        Args:
            page_type: Filter by page type.
            tag: Filter by tag.
            limit: Maximum results.

        Returns:
            List of Page instances.
        """
        conditions: list[str] = []
        params: list[Any] = []

        if page_type:
            conditions.append("page_type = %s")
            params.append(page_type)
        if tag:
            conditions.append("%s = ANY(tags)")
            params.append(tag)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT page_id, title, page_type, content_markdown, frontmatter,
                           confidence, sources, related, tags, freshness_tier,
                           created_at, updated_at
                    FROM pages
                    {where}
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return [self._row_to_page(row) for row in cur.fetchall()]

    def search_pages(self, query_text: str, limit: int = 20) -> list[SearchResult]:
        """Full-text search using tsvector/tsquery and trigram similarity.

        Combines tsvector ranking with trigram title matching for best results.

        Args:
            query_text: Search query string.
            limit: Maximum results.

        Returns:
            List of SearchResult instances ordered by relevance.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT page_id, title, page_type,
                           ts_headline('english', content_markdown,
                                       plainto_tsquery('english', %s),
                                       'MaxWords=40, MinWords=20') AS snippet,
                           ts_rank_cd(to_tsvector('english', content_markdown),
                                      plainto_tsquery('english', %s)) +
                           similarity(title, %s) AS score
                    FROM pages
                    WHERE to_tsvector('english', content_markdown) @@ plainto_tsquery('english', %s)
                       OR similarity(title, %s) > 0.2
                    ORDER BY score DESC
                    LIMIT %s
                    """,
                    (query_text, query_text, query_text, query_text, query_text, limit),
                )
                return [
                    SearchResult(
                        page_id=row[0],
                        title=row[1],
                        page_type=row[2] or "",
                        snippet=row[3] or "",
                        score=float(row[4] or 0),
                        source="fulltext",
                    )
                    for row in cur.fetchall()
                ]

    def upsert_page(self, page: Page) -> None:
        """Insert or update a page.

        Args:
            page: The Page to upsert.
        """
        frontmatter_json = page.frontmatter.model_dump() if page.frontmatter else {}

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pages (
                        page_id, title, page_type, content_markdown, frontmatter,
                        confidence, sources, related, tags, freshness_tier,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (page_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        page_type = EXCLUDED.page_type,
                        content_markdown = EXCLUDED.content_markdown,
                        frontmatter = EXCLUDED.frontmatter,
                        confidence = EXCLUDED.confidence,
                        sources = EXCLUDED.sources,
                        related = EXCLUDED.related,
                        tags = EXCLUDED.tags,
                        freshness_tier = EXCLUDED.freshness_tier,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        page.page_id,
                        page.title,
                        page.page_type.value,
                        page.content_markdown,
                        json.dumps(frontmatter_json),
                        page.confidence.value,
                        page.sources,
                        page.related,
                        page.tags,
                        page.freshness_tier.value,
                        page.created_at,
                        page.updated_at,
                    ),
                )

    # ──────────────────────────────────────────────
    # Backlink operations
    # ──────────────────────────────────────────────

    def get_backlinks(self, page_id: str) -> list[BackLink]:
        """Get all pages linking to the given page.

        Args:
            page_id: Target page slug.

        Returns:
            List of BackLink instances.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_page_id, target_page_id, link_text, context_snippet
                    FROM backlinks
                    WHERE target_page_id = %s
                    """,
                    (page_id,),
                )
                return [
                    BackLink(
                        source_page_id=row[0],
                        target_page_id=row[1],
                        link_text=row[2] or "",
                        context_snippet=row[3] or "",
                    )
                    for row in cur.fetchall()
                ]

    def upsert_backlinks(self, links: list[BackLink]) -> None:
        """Insert or update backlinks.

        Args:
            links: List of BackLink instances to upsert.
        """
        if not links:
            return

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                for link in links:
                    cur.execute(
                        """
                        INSERT INTO backlinks (source_page_id, target_page_id, link_text, context_snippet)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (source_page_id, target_page_id) DO UPDATE SET
                            link_text = EXCLUDED.link_text,
                            context_snippet = EXCLUDED.context_snippet
                        """,
                        (link.source_page_id, link.target_page_id, link.link_text, link.context_snippet),
                    )

    # ──────────────────────────────────────────────
    # Statistics
    # ──────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        """Get wiki statistics.

        Returns:
            Dictionary with page counts, type distribution, etc.
        """
        stats: dict[str, Any] = {}
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM pages")
                stats["total_pages"] = cur.fetchone()[0]

                cur.execute("SELECT page_type, COUNT(*) FROM pages GROUP BY page_type")
                stats["by_type"] = {row[0]: row[1] for row in cur.fetchall()}

                cur.execute("SELECT confidence, COUNT(*) FROM pages GROUP BY confidence")
                stats["by_confidence"] = {row[0]: row[1] for row in cur.fetchall()}

                cur.execute("SELECT COUNT(*) FROM backlinks")
                stats["total_backlinks"] = cur.fetchone()[0]

        return stats

    # ──────────────────────────────────────────────
    # Graph data (for Cytoscape.js)
    # ──────────────────────────────────────────────

    def get_graph_data(self, center_page_id: str | None = None, hops: int = 2) -> dict[str, Any]:
        """Get graph data for knowledge graph visualization.

        Args:
            center_page_id: Optional center page for neighborhood graph.
            hops: Number of hops from center (only used when center_page_id is set).

        Returns:
            Dictionary with 'nodes' and 'edges' lists for Cytoscape.js.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                if center_page_id:
                    # Neighborhood graph: get pages within N hops
                    cur.execute(
                        """
                        WITH RECURSIVE neighborhood AS (
                            SELECT %s AS page_id, 0 AS depth
                            UNION
                            SELECT CASE
                                WHEN b.source_page_id = n.page_id THEN b.target_page_id
                                ELSE b.source_page_id
                            END, n.depth + 1
                            FROM neighborhood n
                            JOIN backlinks b ON b.source_page_id = n.page_id
                                             OR b.target_page_id = n.page_id
                            WHERE n.depth < %s
                        )
                        SELECT DISTINCT p.page_id, p.title, p.page_type
                        FROM neighborhood n
                        JOIN pages p ON p.page_id = n.page_id
                        """,
                        (center_page_id, hops),
                    )
                else:
                    cur.execute("SELECT page_id, title, page_type FROM pages")

                nodes = [
                    {"data": {"id": row[0], "label": row[1], "type": row[2]}}
                    for row in cur.fetchall()
                ]
                node_ids = {n["data"]["id"] for n in nodes}

                cur.execute("SELECT source_page_id, target_page_id FROM backlinks")
                edges = [
                    {"data": {"source": row[0], "target": row[1]}}
                    for row in cur.fetchall()
                    if row[0] in node_ids and row[1] in node_ids
                ]

        return {"nodes": nodes, "edges": edges}

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _row_to_page(row: tuple) -> Page:
        """Convert a database row to a Page model."""
        frontmatter_raw = row[4]
        if isinstance(frontmatter_raw, str):
            frontmatter_raw = json.loads(frontmatter_raw)

        return Page(
            page_id=row[0],
            title=row[1],
            page_type=row[2] or "concept",
            content_markdown=row[3] or "",
            confidence=row[5] or "low",
            sources=row[6] or [],
            related=row[7] or [],
            tags=row[8] or [],
            freshness_tier=row[9] or "monthly",
            created_at=row[10],
            updated_at=row[11],
        )
