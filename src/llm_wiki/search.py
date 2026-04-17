"""Unified wiki search using Lakebase with pgvector + tsvector.

Hybrid search is a single SQL query against Lakebase; no Python-side RRF
merge is needed. Embeddings for queries are computed via the Databricks
Foundation Model API (gte-large-en by default). If Lakebase is unavailable,
falls back to DeltaStore's SQL LIKE search (fulltext only, no embeddings).

Usage:
    from llm_wiki.search import WikiSearch

    search = WikiSearch(lakebase_store=lb, delta_store=delta)
    results = search.search("immortal wizard", mode="hybrid")
"""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient

from llm_wiki.log import logger
from llm_wiki.models import SearchResult


class WikiSearch:
    """Hybrid search backed by Lakebase (pgvector + tsvector)."""

    def __init__(
        self,
        lakebase_store: Any | None = None,
        delta_store: Any | None = None,
        embedding_endpoint: str = "databricks-gte-large-en",
        client: WorkspaceClient | None = None,
        **_legacy,
    ) -> None:
        """Initialize the search engine.

        Args:
            lakebase_store: LakebaseStore for hybrid search. Primary backend.
            delta_store: DeltaStore fallback for fulltext-only when Lakebase is down.
            embedding_endpoint: FMAPI endpoint name for query embedding.
            client: Optional pre-configured WorkspaceClient.
        """
        self._lakebase = lakebase_store
        self._delta = delta_store
        self._embedding_endpoint = embedding_endpoint
        self._client = client or WorkspaceClient()

    def search(
        self,
        query: str,
        limit: int = 20,
        mode: str = "hybrid",
    ) -> list[SearchResult]:
        """Search the wiki.

        Args:
            query: Query string.
            limit: Max results.
            mode: 'fulltext' (no embeddings), 'semantic' (vector only),
                  or 'hybrid' (both, blended in SQL). Default hybrid.
        """
        if self._lakebase is None:
            return self._delta_fallback(query, limit)

        if mode == "fulltext":
            return self._lakebase.search_pages(query, query_embedding=None, limit=limit)

        # Semantic or hybrid -> need query embedding
        try:
            qvec = self._embed_query(query)
        except Exception as e:
            logger.warning("Query embedding failed, falling back to fulltext", error=str(e))
            return self._lakebase.search_pages(query, query_embedding=None, limit=limit)

        if mode == "semantic":
            return self._lakebase.search_pages(
                query, query_embedding=qvec, limit=limit,
                fts_weight=0.0, vector_weight=1.0,
            )
        # hybrid default
        return self._lakebase.search_pages(
            query, query_embedding=qvec, limit=limit,
            fts_weight=0.4, vector_weight=0.6,
        )

    def _embed_query(self, query: str) -> list[float]:
        """Embed a query string via FMAPI."""
        response = self._client.serving_endpoints.query(
            name=self._embedding_endpoint,
            input=[query],
        )
        item = response.data[0]
        vec = item.embedding if hasattr(item, "embedding") else item["embedding"]
        return list(vec)

    def _delta_fallback(self, query: str, limit: int) -> list[SearchResult]:
        """Fallback to Delta SQL LIKE when Lakebase is not configured."""
        if self._delta is None:
            return []
        try:
            pages = self._delta.search_pages(query, limit=limit)
            return [
                SearchResult(
                    page_id=p.page_id, title=p.title,
                    page_type=p.page_type.value,
                    snippet=(p.content_markdown[:200] + "...") if p.content_markdown else "",
                    score=1.0, source="fulltext",
                )
                for p in pages
            ]
        except Exception as e:
            logger.warning("Delta fallback search failed", error=str(e))
            return []


# Legacy reciprocal_rank_fusion kept for any remaining callers (e.g. tests)
def reciprocal_rank_fusion(
    *result_lists: list[SearchResult],
    k: int = 60,
    limit: int = 20,
) -> list[SearchResult]:
    """Merge ranked lists via Reciprocal Rank Fusion.

    Retained for unit tests and edge cases where multiple ranked lists must
    be merged outside a SQL hybrid query.
    """
    scores: dict[str, float] = {}
    best: dict[str, SearchResult] = {}

    for results in result_lists:
        for rank, result in enumerate(results):
            rrf = 1.0 / (k + rank + 1)
            scores[result.page_id] = scores.get(result.page_id, 0.0) + rrf
            if result.page_id not in best or len(result.snippet) > len(best[result.page_id].snippet):
                best[result.page_id] = result

    ordered = sorted(scores.keys(), key=lambda pid: scores[pid], reverse=True)
    merged: list[SearchResult] = []
    for pid in ordered[:limit]:
        r = best[pid]
        r.score = scores[pid]
        r.source = "hybrid"
        merged.append(r)
    return merged
