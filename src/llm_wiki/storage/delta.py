"""Delta table storage operations for LLM Wiki.

Provides CRUD operations on wiki Delta tables using the Databricks SDK
Statement Execution API. This avoids the need for a SQL warehouse HTTP path.

Usage:
    from llm_wiki.storage.delta import DeltaStore

    store = DeltaStore(catalog="nfleming", wiki_schema="wiki_nate_fleming")
    page = store.get_page("kubernetes-scheduling")
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks.sdk import WorkspaceClient

from llm_wiki.log import logger
from llm_wiki.models import (
    BackLink,
    CompilationQueueItem,
    CompilationStatus,
    Page,
    SearchResult,
)


class DeltaStore:
    """Delta table storage backend using Databricks SDK Statement Execution API.

    Uses the SDK's SQL statement execution which auto-discovers a warehouse,
    avoiding manual warehouse configuration.
    """

    def __init__(
        self,
        catalog: str = "nfleming",
        wiki_schema: str = "wiki",
        raw_schema: str = "raw_sources",
        warehouse_id: str | None = None,
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the Delta store.

        Args:
            catalog: Unity Catalog catalog name.
            wiki_schema: Schema for wiki tables (pages, backlinks, etc.).
            raw_schema: Schema for raw source tables.
            warehouse_id: Optional SQL warehouse ID. Auto-detected if not set.
            client: Optional pre-configured WorkspaceClient.
        """
        self.catalog = catalog
        self.wiki_schema = wiki_schema
        self.raw_schema = raw_schema
        self._warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
        self._client = client or WorkspaceClient()
        logger.info(
            "DeltaStore initialized",
            catalog=catalog,
            wiki_schema=wiki_schema,
            warehouse_id=self._warehouse_id or "(auto)",
        )

    def _execute(self, sql: str) -> list[list[Any]]:
        """Execute a SQL statement and return rows.

        Uses the Statement Execution API which auto-discovers a warehouse
        when warehouse_id is not set.

        Args:
            sql: SQL statement to execute.

        Returns:
            List of rows, each row is a list of column values.
        """
        try:
            kwargs: dict[str, Any] = {"statement": sql, "catalog": self.catalog}
            if self._warehouse_id:
                kwargs["warehouse_id"] = self._warehouse_id

            response = self._client.statement_execution.execute_statement(**kwargs)

            if response.result and response.result.data_array:
                return response.result.data_array
            return []
        except Exception as e:
            logger.error("SQL execution failed", sql=sql[:200], error=str(e))
            raise

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
        rows = self._execute(f"""
            SELECT page_id, title, page_type, content_markdown,
                   confidence, sources, related, tags, freshness_tier,
                   content_hash, created_at, updated_at, compiled_by
            FROM {self._wiki_table("pages")}
            WHERE page_id = '{_esc(page_id)}'
        """)
        if not rows:
            return None
        return self._row_to_page(rows[0])

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
        if page_type:
            conditions.append(f"page_type = '{_esc(page_type)}'")
        if tag:
            conditions.append(f"array_contains(tags, '{_esc(tag)}')")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self._execute(f"""
            SELECT page_id, title, page_type, content_markdown,
                   confidence, sources, related, tags, freshness_tier,
                   content_hash, created_at, updated_at, compiled_by
            FROM {self._wiki_table("pages")}
            {where}
            ORDER BY updated_at DESC
            LIMIT {limit}
        """)
        return [self._row_to_page(row) for row in rows]

    def search_pages(self, query_text: str, limit: int = 20) -> list[Page]:
        """Search pages by title and content using SQL LIKE.

        Args:
            query_text: Search query string.
            limit: Maximum results.

        Returns:
            List of matching Pages.
        """
        pattern = f"%{_esc(query_text)}%"
        rows = self._execute(f"""
            SELECT page_id, title, page_type, content_markdown,
                   confidence, sources, related, tags, freshness_tier,
                   content_hash, created_at, updated_at, compiled_by
            FROM {self._wiki_table("pages")}
            WHERE title LIKE '{pattern}' OR content_markdown LIKE '{pattern}'
            ORDER BY updated_at DESC
            LIMIT {limit}
        """)
        return [self._row_to_page(row) for row in rows]

    def upsert_page(self, page: Page) -> None:
        """Insert or update a page in the pages table.

        Args:
            page: The Page to upsert.
        """
        now = datetime.now(timezone.utc)
        content_hash = hashlib.sha256(page.content_markdown.encode()).hexdigest()[:16]
        frontmatter_json = page.frontmatter.model_dump_json() if page.frontmatter else "{}"

        sources_arr = f"ARRAY({','.join(repr(s) for s in page.sources)})" if page.sources else "ARRAY()"
        related_arr = f"ARRAY({','.join(repr(r) for r in page.related)})" if page.related else "ARRAY()"
        tags_arr = f"ARRAY({','.join(repr(t) for t in page.tags)})" if page.tags else "ARRAY()"

        self._execute(f"""
            MERGE INTO {self._wiki_table("pages")} AS target
            USING (SELECT '{_esc(page.page_id)}' AS page_id) AS source
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
                '{_esc(page.page_id)}', '{_esc(page.title)}', '{page.page_type.value}',
                '{_esc(page.content_markdown)}', '{_esc(frontmatter_json)}',
                '{page.confidence.value}', {sources_arr}, {related_arr}, {tags_arr},
                '{page.freshness_tier.value}', '{content_hash}',
                '{now.isoformat()}', '{now.isoformat()}', '{_esc(page.compiled_by)}'
            )
        """)
        logger.info("Upserted page", page_id=page.page_id)

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
        rows = self._execute(f"""
            SELECT source_page_id, target_page_id, link_text, context_snippet
            FROM {self._wiki_table("backlinks")}
            WHERE target_page_id = '{_esc(page_id)}'
        """)
        return [
            BackLink(
                source_page_id=row[0] or "",
                target_page_id=row[1] or "",
                link_text=row[2] or "",
                context_snippet=row[3] or "",
            )
            for row in rows
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
        self._execute(f"""
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
        """)
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
        rows = self._execute(f"""
            SELECT queue_id, page_id, trigger_type, trigger_source_ids,
                   priority, status, created_at, completed_at, error_message
            FROM {self._wiki_table("compilation_queue")}
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT {limit}
        """)
        return [
            CompilationQueueItem(
                queue_id=row[0] or "",
                page_id=row[1] or "",
                trigger_type=row[2] or "manual",
                trigger_source_ids=row[3] or [],
                priority=row[4] or 0,
                status=row[5] or "pending",
                created_at=row[6],
                completed_at=row[7],
                error_message=row[8] or "",
            )
            for row in rows
        ]

    def update_compilation_status(
        self,
        queue_id: str,
        status: CompilationStatus,
        error_message: str = "",
    ) -> None:
        """Update the status of a compilation queue item."""
        now = datetime.now(timezone.utc).isoformat()
        completed = f"'{now}'" if status in (CompilationStatus.COMPLETED, CompilationStatus.FAILED) else "NULL"
        self._execute(f"""
            UPDATE {self._wiki_table("compilation_queue")}
            SET status = '{status.value}',
                completed_at = {completed},
                error_message = '{_esc(error_message)}'
            WHERE queue_id = '{_esc(queue_id)}'
        """)

    def enqueue_compilation(
        self,
        page_id: str,
        trigger_type: str = "manual",
        trigger_source_ids: list[str] | None = None,
        priority: int = 0,
    ) -> str:
        """Add a new item to the compilation queue."""
        queue_id = str(uuid4())
        source_ids = trigger_source_ids or []
        sources_arr = f"ARRAY({','.join(repr(s) for s in source_ids)})" if source_ids else "ARRAY()"
        now = datetime.now(timezone.utc).isoformat()

        self._execute(f"""
            INSERT INTO {self._wiki_table("compilation_queue")}
            (queue_id, page_id, trigger_type, trigger_source_ids, priority, status, created_at)
            VALUES ('{queue_id}', '{_esc(page_id)}', '{_esc(trigger_type)}',
                    {sources_arr}, {priority}, 'pending', '{now}')
        """)
        logger.info("Enqueued compilation", queue_id=queue_id, page_id=page_id)
        return queue_id

    # ──────────────────────────────────────────────
    # Activity log
    # ──────────────────────────────────────────────

    def log_activity(self, operation: str, details: str, page_ids: list[str] | None = None) -> None:
        """Write an entry to the activity log."""
        log_id = str(uuid4())
        ids = page_ids or []
        ids_arr = f"ARRAY({','.join(repr(p) for p in ids)})" if ids else "ARRAY()"
        now = datetime.now(timezone.utc).isoformat()

        self._execute(f"""
            INSERT INTO {self._wiki_table("activity_log")}
            (log_id, operation, details, page_ids, timestamp)
            VALUES ('{log_id}', '{_esc(operation)}', '{_esc(details)}', {ids_arr}, '{now}')
        """)

    # ──────────────────────────────────────────────
    # Statistics
    # ──────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        """Get wiki statistics: page counts, type distribution, freshness."""
        stats: dict[str, Any] = {}

        rows = self._execute(f"SELECT COUNT(*) FROM {self._wiki_table('pages')}")
        stats["total_pages"] = rows[0][0] if rows else 0

        rows = self._execute(f"""
            SELECT page_type, COUNT(*) as cnt
            FROM {self._wiki_table("pages")}
            GROUP BY page_type
        """)
        stats["by_type"] = {row[0]: row[1] for row in rows}

        rows = self._execute(f"""
            SELECT confidence, COUNT(*) as cnt
            FROM {self._wiki_table("pages")}
            GROUP BY confidence
        """)
        stats["by_confidence"] = {row[0]: row[1] for row in rows}

        rows = self._execute(f"SELECT COUNT(*) FROM {self._wiki_table('backlinks')}")
        stats["total_backlinks"] = rows[0][0] if rows else 0

        rows = self._execute(f"""
            SELECT COUNT(*) FROM {self._wiki_table("compilation_queue")}
            WHERE status = 'pending'
        """)
        stats["pending_compilations"] = rows[0][0] if rows else 0

        return stats

    # ──────────────────────────────────────────────
    # Source operations
    # ──────────────────────────────────────────────

    def get_source_chunks(self, source_id: str) -> list:
        """Get all chunks for a source document."""
        from llm_wiki.models import SourceChunk

        rows = self._execute(f"""
            SELECT chunk_id, source_id, chunk_index, chunk_text, token_count
            FROM {self._raw_table("source_chunks")}
            WHERE source_id = '{_esc(source_id)}'
            ORDER BY chunk_index
        """)
        return [
            SourceChunk(
                chunk_id=row[0] or "",
                source_id=row[1] or "",
                chunk_index=row[2] or 0,
                chunk_text=row[3] or "",
                token_count=row[4] or 0,
            )
            for row in rows
        ]

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _row_to_page(row: list) -> Page:
        """Convert a result row to a Page model."""
        return Page(
            page_id=row[0] or "",
            title=row[1] or "",
            page_type=row[2] or "concept",
            content_markdown=row[3] or "",
            confidence=row[4] or "low",
            sources=row[5] or [],
            related=row[6] or [],
            tags=row[7] or [],
            freshness_tier=row[8] or "monthly",
            content_hash=row[9] or "",
            created_at=row[10],
            updated_at=row[11],
            compiled_by=row[12] or "",
        )


def _esc(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")
