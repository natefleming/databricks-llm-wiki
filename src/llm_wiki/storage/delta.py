"""Delta table storage operations for LLM Wiki.

Provides CRUD operations on wiki Delta tables via databricks-sql-connector.
This is the source-of-truth layer - all data originates here.

Usage:
    from llm_wiki.storage.delta import DeltaStore

    store = DeltaStore(catalog="llm_wiki", schema="wiki")
    page = store.get_page("kubernetes-scheduling")
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks import sql as dbsql

from llm_wiki.log import logger
from llm_wiki.models import (
    ActivityLogEntry,
    BackLink,
    CompilationQueueItem,
    CompilationStatus,
    Page,
    Source,
    SourceChunk,
)


class DeltaStore:
    """Delta table storage backend for LLM Wiki.

    Manages pages, sources, backlinks, compilation queue, and activity log
    in Unity Catalog Delta tables.
    """

    def __init__(
        self,
        catalog: str = "llm_wiki",
        wiki_schema: str = "wiki",
        raw_schema: str = "raw_sources",
        server_hostname: str | None = None,
        http_path: str | None = None,
        access_token: str | None = None,
    ) -> None:
        """Initialize the Delta store.

        Args:
            catalog: Unity Catalog catalog name.
            wiki_schema: Schema for wiki tables (pages, backlinks, etc.).
            raw_schema: Schema for raw source tables.
            server_hostname: Databricks workspace hostname. Defaults to env var.
            http_path: SQL warehouse HTTP path. Defaults to env var.
            access_token: Databricks access token. Defaults to env var.
        """
        self.catalog = catalog
        self.wiki_schema = wiki_schema
        self.raw_schema = raw_schema
        self._server_hostname = server_hostname or os.environ.get("DATABRICKS_SERVER_HOSTNAME", "")
        self._http_path = http_path or os.environ.get("DATABRICKS_HTTP_PATH", "")
        self._access_token = access_token or os.environ.get("DATABRICKS_TOKEN", "")

    def _connect(self) -> dbsql.client.Connection:
        """Create a new database connection."""
        return dbsql.connect(
            server_hostname=self._server_hostname,
            http_path=self._http_path,
            access_token=self._access_token,
            catalog=self.catalog,
        )

    def _wiki_table(self, table: str) -> str:
        """Return fully qualified wiki table name."""
        return f"{self.catalog}.{self.wiki_schema}.{table}"

    def _raw_table(self, table: str) -> str:
        """Return fully qualified raw source table name."""
        return f"{self.catalog}.{self.raw_schema}.{table}"

    # ──────────────────────────────────────────────
    # Page operations
    # ──────────────────────────────────────────────

    def get_page(self, page_id: str) -> Page | None:
        """Retrieve a single page by its slug.

        Args:
            page_id: The page slug identifier.

        Returns:
            Page instance or None if not found.
        """
        query = f"""
            SELECT page_id, title, page_type, content_markdown, frontmatter,
                   confidence, sources, related, tags, freshness_tier,
                   content_hash, created_at, updated_at, compiled_by
            FROM {self._wiki_table("pages")}
            WHERE page_id = %s
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [page_id])
                row = cursor.fetchone()
                if row is None:
                    return None
                return self._row_to_page(row, cursor.description)

    def list_pages(
        self,
        page_type: str | None = None,
        tag: str | None = None,
        limit: int = 100,
    ) -> list[Page]:
        """List pages with optional filtering.

        Args:
            page_type: Filter by page type (concept, entity, etc.).
            tag: Filter by tag.
            limit: Maximum number of pages to return.

        Returns:
            List of Page instances.
        """
        conditions: list[str] = []
        params: list[Any] = []

        if page_type:
            conditions.append("page_type = %s")
            params.append(page_type)
        if tag:
            conditions.append("array_contains(tags, %s)")
            params.append(tag)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT page_id, title, page_type, content_markdown, frontmatter,
                   confidence, sources, related, tags, freshness_tier,
                   content_hash, created_at, updated_at, compiled_by
            FROM {self._wiki_table("pages")}
            {where}
            ORDER BY updated_at DESC
            LIMIT %s
        """
        params.append(limit)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return [self._row_to_page(row, cursor.description) for row in cursor.fetchall()]

    def upsert_page(self, page: Page) -> None:
        """Insert or update a page in the pages table.

        Args:
            page: The Page to upsert.
        """
        now = datetime.now(timezone.utc)
        content_hash = hashlib.sha256(page.content_markdown.encode()).hexdigest()[:16]
        frontmatter_json = page.frontmatter.model_dump_json() if page.frontmatter else "{}"

        query = f"""
            MERGE INTO {self._wiki_table("pages")} AS target
            USING (SELECT %s AS page_id) AS source
            ON target.page_id = source.page_id
            WHEN MATCHED THEN UPDATE SET
                title = %s, page_type = %s, content_markdown = %s, frontmatter = %s,
                confidence = %s, sources = %s, related = %s, tags = %s,
                freshness_tier = %s, content_hash = %s, updated_at = %s, compiled_by = %s
            WHEN NOT MATCHED THEN INSERT (
                page_id, title, page_type, content_markdown, frontmatter,
                confidence, sources, related, tags, freshness_tier,
                content_hash, created_at, updated_at, compiled_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        sources_arr = f"ARRAY({','.join(repr(s) for s in page.sources)})" if page.sources else "ARRAY()"
        related_arr = f"ARRAY({','.join(repr(r) for r in page.related)})" if page.related else "ARRAY()"
        tags_arr = f"ARRAY({','.join(repr(t) for t in page.tags)})" if page.tags else "ARRAY()"

        # Use SQL directly for array types since connector doesn't support array params well
        insert_sql = f"""
            MERGE INTO {self._wiki_table("pages")} AS target
            USING (SELECT '{page.page_id}' AS page_id) AS source
            ON target.page_id = source.page_id
            WHEN MATCHED THEN UPDATE SET
                title = '{_esc(page.title)}',
                page_type = '{page.page_type.value}',
                content_markdown = '{_esc(page.content_markdown)}',
                frontmatter = '{_esc(frontmatter_json)}',
                confidence = '{page.confidence.value}',
                sources = {sources_arr},
                related = {related_arr},
                tags = {tags_arr},
                freshness_tier = '{page.freshness_tier.value}',
                content_hash = '{content_hash}',
                updated_at = '{now.isoformat()}',
                compiled_by = '{_esc(page.compiled_by)}'
            WHEN NOT MATCHED THEN INSERT (
                page_id, title, page_type, content_markdown, frontmatter,
                confidence, sources, related, tags, freshness_tier,
                content_hash, created_at, updated_at, compiled_by
            ) VALUES (
                '{page.page_id}', '{_esc(page.title)}', '{page.page_type.value}',
                '{_esc(page.content_markdown)}', '{_esc(frontmatter_json)}',
                '{page.confidence.value}', {sources_arr}, {related_arr}, {tags_arr},
                '{page.freshness_tier.value}', '{content_hash}',
                '{now.isoformat()}', '{now.isoformat()}', '{_esc(page.compiled_by)}'
            )
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_sql)
        logger.info("Upserted page", page_id=page.page_id)

    def search_pages(self, query_text: str, limit: int = 20) -> list[Page]:
        """Search pages by title and content using SQL LIKE.

        Args:
            query_text: Search query string.
            limit: Maximum results.

        Returns:
            List of matching Pages.
        """
        pattern = f"%{_esc(query_text)}%"
        query = f"""
            SELECT page_id, title, page_type, content_markdown, frontmatter,
                   confidence, sources, related, tags, freshness_tier,
                   content_hash, created_at, updated_at, compiled_by
            FROM {self._wiki_table("pages")}
            WHERE title LIKE '{pattern}' OR content_markdown LIKE '{pattern}'
            ORDER BY updated_at DESC
            LIMIT {limit}
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [self._row_to_page(row, cursor.description) for row in cursor.fetchall()]

    # ──────────────────────────────────────────────
    # Backlink operations
    # ──────────────────────────────────────────────

    def get_backlinks(self, page_id: str) -> list[BackLink]:
        """Get all pages that link TO the given page.

        Args:
            page_id: Target page slug.

        Returns:
            List of BackLink instances.
        """
        query = f"""
            SELECT source_page_id, target_page_id, link_text, context_snippet
            FROM {self._wiki_table("backlinks")}
            WHERE target_page_id = %s
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [page_id])
                return [
                    BackLink(
                        source_page_id=row[0],
                        target_page_id=row[1],
                        link_text=row[2] or "",
                        context_snippet=row[3] or "",
                    )
                    for row in cursor.fetchall()
                ]

    def upsert_backlinks(self, links: list[BackLink]) -> None:
        """Insert or update backlinks.

        Args:
            links: List of BackLink instances to upsert.
        """
        if not links:
            return

        values = ", ".join(
            f"('{_esc(l.source_page_id)}', '{_esc(l.target_page_id)}', "
            f"'{_esc(l.link_text)}', '{_esc(l.context_snippet)}')"
            for l in links
        )
        query = f"""
            MERGE INTO {self._wiki_table("backlinks")} AS target
            USING (
                SELECT * FROM VALUES {values}
                AS s(source_page_id, target_page_id, link_text, context_snippet)
            ) AS source
            ON target.source_page_id = source.source_page_id
               AND target.target_page_id = source.target_page_id
            WHEN MATCHED THEN UPDATE SET
                link_text = source.link_text,
                context_snippet = source.context_snippet
            WHEN NOT MATCHED THEN INSERT *
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
        logger.info("Upserted backlinks", count=len(links))

    # ──────────────────────────────────────────────
    # Compilation queue operations
    # ──────────────────────────────────────────────

    def get_pending_compilations(self, limit: int = 50) -> list[CompilationQueueItem]:
        """Get pending compilation queue items ordered by priority.

        Args:
            limit: Maximum items to return.

        Returns:
            List of pending CompilationQueueItem instances.
        """
        query = f"""
            SELECT queue_id, page_id, trigger_type, trigger_source_ids,
                   priority, status, created_at, completed_at, error_message
            FROM {self._wiki_table("compilation_queue")}
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT {limit}
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [
                    CompilationQueueItem(
                        queue_id=row[0],
                        page_id=row[1],
                        trigger_type=row[2],
                        trigger_source_ids=row[3] or [],
                        priority=row[4] or 0,
                        status=row[5],
                        created_at=row[6],
                        completed_at=row[7],
                        error_message=row[8] or "",
                    )
                    for row in cursor.fetchall()
                ]

    def update_compilation_status(
        self,
        queue_id: str,
        status: CompilationStatus,
        error_message: str = "",
    ) -> None:
        """Update the status of a compilation queue item.

        Args:
            queue_id: The queue item ID.
            status: New status.
            error_message: Error message if status is FAILED.
        """
        now = datetime.now(timezone.utc).isoformat()
        completed = f"'{now}'" if status in (CompilationStatus.COMPLETED, CompilationStatus.FAILED) else "NULL"
        query = f"""
            UPDATE {self._wiki_table("compilation_queue")}
            SET status = '{status.value}',
                completed_at = {completed},
                error_message = '{_esc(error_message)}'
            WHERE queue_id = '{_esc(queue_id)}'
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

    def enqueue_compilation(
        self,
        page_id: str,
        trigger_type: str = "manual",
        trigger_source_ids: list[str] | None = None,
        priority: int = 0,
    ) -> str:
        """Add a new item to the compilation queue.

        Args:
            page_id: Target page to compile.
            trigger_type: What triggered this compilation.
            trigger_source_ids: Source IDs that triggered this.
            priority: Priority (higher = more urgent).

        Returns:
            The queue_id of the new item.
        """
        queue_id = str(uuid4())
        source_ids = trigger_source_ids or []
        sources_arr = f"ARRAY({','.join(repr(s) for s in source_ids)})" if source_ids else "ARRAY()"
        now = datetime.now(timezone.utc).isoformat()

        query = f"""
            INSERT INTO {self._wiki_table("compilation_queue")}
            (queue_id, page_id, trigger_type, trigger_source_ids, priority, status, created_at)
            VALUES ('{queue_id}', '{_esc(page_id)}', '{_esc(trigger_type)}',
                    {sources_arr}, {priority}, 'pending', '{now}')
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
        logger.info("Enqueued compilation", queue_id=queue_id, page_id=page_id)
        return queue_id

    # ──────────────────────────────────────────────
    # Source operations
    # ──────────────────────────────────────────────

    def get_source_chunks(self, source_id: str) -> list[SourceChunk]:
        """Get all chunks for a source document.

        Args:
            source_id: The source identifier.

        Returns:
            List of SourceChunk instances ordered by chunk_index.
        """
        query = f"""
            SELECT chunk_id, source_id, chunk_index, chunk_text, token_count
            FROM {self._raw_table("source_chunks")}
            WHERE source_id = %s
            ORDER BY chunk_index
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [source_id])
                return [
                    SourceChunk(
                        chunk_id=row[0],
                        source_id=row[1],
                        chunk_index=row[2],
                        chunk_text=row[3],
                        token_count=row[4] or 0,
                    )
                    for row in cursor.fetchall()
                ]

    # ──────────────────────────────────────────────
    # Activity log
    # ──────────────────────────────────────────────

    def log_activity(self, operation: str, details: str, page_ids: list[str] | None = None) -> None:
        """Write an entry to the activity log.

        Args:
            operation: Operation name (ingest, compile, query, lint).
            details: Human-readable description.
            page_ids: List of affected page IDs.
        """
        log_id = str(uuid4())
        ids = page_ids or []
        ids_arr = f"ARRAY({','.join(repr(p) for p in ids)})" if ids else "ARRAY()"
        now = datetime.now(timezone.utc).isoformat()

        query = f"""
            INSERT INTO {self._wiki_table("activity_log")}
            (log_id, operation, details, page_ids, timestamp)
            VALUES ('{log_id}', '{_esc(operation)}', '{_esc(details)}', {ids_arr}, '{now}')
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

    # ──────────────────────────────────────────────
    # Statistics
    # ──────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        """Get wiki statistics: page counts, type distribution, freshness.

        Returns:
            Dictionary with statistics.
        """
        queries = {
            "total_pages": f"SELECT COUNT(*) FROM {self._wiki_table('pages')}",
            "by_type": f"""
                SELECT page_type, COUNT(*) as cnt
                FROM {self._wiki_table("pages")}
                GROUP BY page_type
            """,
            "by_confidence": f"""
                SELECT confidence, COUNT(*) as cnt
                FROM {self._wiki_table("pages")}
                GROUP BY confidence
            """,
            "total_backlinks": f"SELECT COUNT(*) FROM {self._wiki_table('backlinks')}",
            "pending_compilations": f"""
                SELECT COUNT(*) FROM {self._wiki_table("compilation_queue")}
                WHERE status = 'pending'
            """,
        }

        stats: dict[str, Any] = {}
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queries["total_pages"])
                stats["total_pages"] = cursor.fetchone()[0]

                cursor.execute(queries["by_type"])
                stats["by_type"] = {row[0]: row[1] for row in cursor.fetchall()}

                cursor.execute(queries["by_confidence"])
                stats["by_confidence"] = {row[0]: row[1] for row in cursor.fetchall()}

                cursor.execute(queries["total_backlinks"])
                stats["total_backlinks"] = cursor.fetchone()[0]

                cursor.execute(queries["pending_compilations"])
                stats["pending_compilations"] = cursor.fetchone()[0]

        return stats

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _row_to_page(row: tuple, description: list) -> Page:
        """Convert a database row to a Page model."""
        cols = [d[0] for d in description]
        data = dict(zip(cols, row))
        return Page(
            page_id=data.get("page_id", ""),
            title=data.get("title", ""),
            page_type=data.get("page_type", "concept"),
            content_markdown=data.get("content_markdown", ""),
            confidence=data.get("confidence", "low"),
            sources=data.get("sources") or [],
            related=data.get("related") or [],
            tags=data.get("tags") or [],
            freshness_tier=data.get("freshness_tier", "monthly"),
            content_hash=data.get("content_hash", ""),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            compiled_by=data.get("compiled_by", ""),
        )


def _esc(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")
