"""Lakebase (Postgres + pgvector) storage for LLM Wiki serving.

Single backend for both full-text (tsvector + pg_trgm) and semantic (pgvector)
search. Hybrid queries merge lexical + vector scores in one SQL statement so
no RRF layer is needed in Python.

Usage:
    from llm_wiki.storage.lakebase import LakebaseStore

    store = LakebaseStore.from_instance("llm-wiki-db", database="llm_wiki")
    results = store.search_pages("immortal wizard", query_embedding=[...], limit=10)
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from psycopg_pool import ConnectionPool

from llm_wiki.log import logger
from llm_wiki.models import BackLink, Page, SearchResult


# gte-large-en produces 1024-dim vectors
EMBEDDING_DIM = 1024


class LakebaseStore:
    """Lakebase/Postgres backend with pgvector and tsvector for hybrid search."""

    def __init__(
        self,
        host: str,
        database: str = "llm_wiki",
        user: str = "",
        password_fn: Any = None,
        port: int = 5432,
        min_size: int = 1,
        max_size: int = 5,
    ) -> None:
        """Initialize with an OAuth-token password provider.

        Args:
            host: Lakebase read-write DNS.
            database: PG database name.
            user: Postgres role (user email for personal auth).
            password_fn: Zero-arg callable returning a fresh OAuth token.
            port: PG port (5432).
            min_size: Pool min connections.
            max_size: Pool max connections.
        """
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password_fn = password_fn
        self._token = password_fn() if password_fn else ""
        self._token_issued_at = time.time()
        self._pool = self._build_pool(min_size, max_size)
        logger.info("LakebaseStore connected", host=host, database=database)

    def _build_pool(self, min_size: int, max_size: int) -> ConnectionPool:
        conninfo = (
            f"host={self._host} port={self._port} dbname={self._database} "
            f"user={self._user} password={self._token} sslmode=require"
        )
        return ConnectionPool(conninfo=conninfo, min_size=min_size, max_size=max_size, open=True)

    @classmethod
    def from_instance(
        cls,
        instance_name: str,
        database: str = "llm_wiki",
        profile: str | None = None,
        user: str | None = None,
    ) -> LakebaseStore:
        """Build a store from a Databricks Lakebase instance name.

        Fetches host + generates a fresh OAuth token via the Databricks SDK.
        """
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        inst = w.database.get_database_instance(name=instance_name)

        if user:
            role = user
        else:
            # Determine role name from current identity:
            # - User: email address
            # - Service principal (Databricks App): application_id UUID
            import os
            sp_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
            if sp_client_id:
                role = sp_client_id
            else:
                me = w.current_user.me()
                role = me.emails[0].value if me.emails else me.user_name

        def mint_token() -> str:
            cred = w.database.generate_database_credential(instance_names=[instance_name])
            return cred.token

        return cls(
            host=inst.read_write_dns,
            database=database,
            user=role,
            password_fn=mint_token,
        )

    def close(self) -> None:
        """Close the connection pool."""
        self._pool.close()

    # ──────────────────────────────────────────────
    # Page CRUD
    # ──────────────────────────────────────────────

    def get_page(self, page_id: str) -> Page | None:
        """Fetch a page by slug."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT page_id, title, page_type, content_markdown, frontmatter,
                       confidence, sources, related, tags, freshness_tier,
                       content_hash, compiled_by, created_at, updated_at
                FROM public.pages_v WHERE page_id = %s
                """,
                (page_id,),
            )
            row = cur.fetchone()
            return self._row_to_page(row) if row else None

    def list_pages(
        self,
        page_type: str | None = None,
        tag: str | None = None,
        limit: int = 100,
    ) -> list[Page]:
        """List pages with optional filters."""
        conds: list[str] = []
        params: list[Any] = []
        if page_type:
            conds.append("page_type = %s")
            params.append(page_type)
        if tag:
            conds.append("%s = ANY(tags)")
            params.append(tag)

        where = f"WHERE {' AND '.join(conds)}" if conds else ""
        params.append(limit)

        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT page_id, title, page_type, content_markdown, frontmatter,
                       confidence, sources, related, tags, freshness_tier,
                       content_hash, compiled_by, created_at, updated_at
                FROM public.pages_v {where}
                ORDER BY updated_at DESC NULLS LAST
                LIMIT %s
                """,
                params,
            )
            return [self._row_to_page(r) for r in cur.fetchall()]

    def upsert_page(
        self,
        page: Page,
        embedding: list[float] | None = None,
    ) -> None:
        """DEPRECATED: writes now go through Delta + reverse ETL (synced table).

        This store reads from public.pages_v, a view over the synced Delta
        mirror. Call DeltaStore.upsert_page() instead; changes propagate
        automatically to Lakebase via the synced table pipeline.
        """
        raise NotImplementedError(
            "Writes go through Delta + reverse ETL. "
            "Use DeltaStore.upsert_page() - changes sync to Lakebase automatically."
        )

    # ──────────────────────────────────────────────
    # Hybrid search (single SQL)
    # ──────────────────────────────────────────────

    def search_pages(
        self,
        query: str,
        query_embedding: list[float] | None = None,
        limit: int = 20,
        fts_weight: float = 0.4,
        vector_weight: float = 0.6,
    ) -> list[SearchResult]:
        """Hybrid search: tsvector rank + pgvector cosine similarity in one SQL.

        Returns results scored as weighted blend. If query_embedding is None,
        falls back to pure full-text.

        Args:
            query: Natural-language query string.
            query_embedding: Pre-computed 1024-dim embedding for semantic part.
            limit: Max results.
            fts_weight: Weight for lexical match score (0..1).
            vector_weight: Weight for cosine-similarity score (0..1).

        Returns:
            SearchResult list ordered by blended score descending.
        """
        if query_embedding is None:
            return self._fulltext_only(query, limit)

        vec = _to_pgvector(query_embedding)
        sql = """
        WITH fts AS (
            SELECT page_id, title, page_type, content_markdown,
                   ts_rank_cd(fts, plainto_tsquery('english', %(q)s)) AS fts_score
            FROM public.pages_v
            WHERE fts @@ plainto_tsquery('english', %(q)s)
            ORDER BY fts_score DESC
            LIMIT 50
        ),
        vec AS (
            SELECT page_id, title, page_type, content_markdown,
                   1 - (embedding <=> %(v)s::vector) AS vec_score
            FROM public.pages_v
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> %(v)s::vector
            LIMIT 50
        ),
        merged AS (
            SELECT
                COALESCE(f.page_id, v.page_id)         AS page_id,
                COALESCE(f.title, v.title)             AS title,
                COALESCE(f.page_type, v.page_type)     AS page_type,
                COALESCE(f.content_markdown, v.content_markdown) AS content_markdown,
                COALESCE(f.fts_score, 0)               AS fts_score,
                COALESCE(v.vec_score, 0)               AS vec_score
            FROM fts f
            FULL OUTER JOIN vec v ON f.page_id = v.page_id
        )
        SELECT
            page_id,
            title,
            page_type,
            ts_headline('english', content_markdown,
                        plainto_tsquery('english', %(q)s),
                        'MaxWords=25, MinWords=10, MaxFragments=1') AS snippet,
            (%(wf)s * fts_score + %(wv)s * vec_score) AS score
        FROM merged
        ORDER BY score DESC
        LIMIT %(lim)s
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, {
                "q": query, "v": vec,
                "wf": fts_weight, "wv": vector_weight,
                "lim": limit,
            })
            rows = cur.fetchall()

        return [
            SearchResult(
                page_id=r[0],
                title=r[1] or "",
                page_type=r[2] or "concept",
                snippet=r[3] or "",
                score=float(r[4] or 0),
                source="hybrid",
            )
            for r in rows
        ]

    def _fulltext_only(self, query: str, limit: int) -> list[SearchResult]:
        """Full-text only path (used when no query embedding supplied)."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT page_id, title, page_type,
                       ts_headline('english', content_markdown,
                                   plainto_tsquery('english', %s),
                                   'MaxWords=25, MinWords=10, MaxFragments=1') AS snippet,
                       ts_rank_cd(fts, plainto_tsquery('english', %s))
                         + 0.3 * similarity(title, %s) AS score
                FROM public.pages_v
                WHERE fts @@ plainto_tsquery('english', %s) OR similarity(title, %s) > 0.15
                ORDER BY score DESC
                LIMIT %s
                """,
                (query, query, query, query, query, limit),
            )
            return [
                SearchResult(
                    page_id=r[0],
                    title=r[1] or "",
                    page_type=r[2] or "concept",
                    snippet=r[3] or "",
                    score=float(r[4] or 0),
                    source="fulltext",
                )
                for r in cur.fetchall()
            ]

    # ──────────────────────────────────────────────
    # Backlinks
    # ──────────────────────────────────────────────

    def get_backlinks(self, page_id: str) -> list[BackLink]:
        """Read backlinks from the synced table."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_page_id, target_page_id, link_text, context_snippet "
                "FROM wiki_nate_fleming.backlinks_synced WHERE target_page_id = %s",
                (page_id,),
            )
            return [
                BackLink(
                    source_page_id=r[0], target_page_id=r[1],
                    link_text=r[2] or "", context_snippet=r[3] or "",
                )
                for r in cur.fetchall()
            ]

    def upsert_backlinks(self, links: list[BackLink]) -> None:
        """DEPRECATED: writes go through Delta + reverse ETL."""
        raise NotImplementedError(
            "Writes go through Delta + reverse ETL. "
            "Use DeltaStore.upsert_backlinks() - changes sync to Lakebase automatically."
        )

    # ──────────────────────────────────────────────
    # Stats + graph
    # ──────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        stats: dict[str, Any] = {}
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.pages_v")
            stats["total_pages"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.pages_v WHERE embedding IS NOT NULL")
            stats["pages_with_embeddings"] = cur.fetchone()[0]
            cur.execute("SELECT page_type, COUNT(*) FROM public.pages_v GROUP BY page_type")
            stats["by_type"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT confidence, COUNT(*) FROM public.pages_v GROUP BY confidence")
            stats["by_confidence"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT COUNT(*) FROM wiki_nate_fleming.backlinks_synced")
            stats["total_backlinks"] = cur.fetchone()[0]
        return stats

    def get_graph_data(self, center_page_id: str | None = None) -> dict[str, Any]:
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT page_id, title, page_type FROM public.pages_v")
            nodes = [
                {"data": {"id": r[0], "label": r[1], "type": r[2] or "concept"}}
                for r in cur.fetchall()
            ]
            node_ids = {n["data"]["id"] for n in nodes}

            cur.execute("SELECT source_page_id, target_page_id FROM wiki_nate_fleming.backlinks_synced")
            edges = [
                {"data": {"source": r[0], "target": r[1]}}
                for r in cur.fetchall()
                if r[0] in node_ids and r[1] in node_ids
            ]
        return {"nodes": nodes, "edges": edges}

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _row_to_page(row: tuple) -> Page:
        fm_raw = row[4]
        if isinstance(fm_raw, str):
            try:
                fm_raw = json.loads(fm_raw)
            except Exception:
                fm_raw = {}
        return Page(
            page_id=row[0],
            title=row[1] or "",
            page_type=row[2] or "concept",
            content_markdown=row[3] or "",
            confidence=row[5] or "low",
            sources=row[6] or [],
            related=row[7] or [],
            tags=row[8] or [],
            freshness_tier=row[9] or "monthly",
            content_hash=row[10] or "",
            compiled_by=row[11] or "",
            created_at=row[12],
            updated_at=row[13],
        )


def _to_pgvector(values: list[float]) -> str:
    """Convert a float list to the pgvector string literal format."""
    return "[" + ",".join(f"{float(x):.8f}" for x in values) + "]"
