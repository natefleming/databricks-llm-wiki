"""Unified search combining full-text and Vector Search.

Supports Lakebase pg_trgm search when available, with Delta SQL LIKE
as a fallback. Merges results using reciprocal rank fusion.

Usage:
    from llm_wiki.search import WikiSearch

    search = WikiSearch(lakebase_store=lb, delta_store=ds)
    results = search.search("kubernetes scheduling", limit=10)
"""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient

from llm_wiki.log import logger
from llm_wiki.models import SearchResult


class WikiSearch:
    """Unified wiki search combining full-text and semantic search."""

    def __init__(
        self,
        lakebase_store: Any | None = None,
        delta_store: Any | None = None,
        vs_endpoint_name: str = "llm-wiki-vs-endpoint",
        vs_index_name: str = "llm_wiki.wiki.pages_vs_index",
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the search engine.

        Args:
            lakebase_store: Optional LakebaseStore for fast full-text search.
            delta_store: Optional DeltaStore as fallback for full-text search.
            vs_endpoint_name: Vector Search endpoint name.
            vs_index_name: Vector Search index name.
            client: Optional pre-configured WorkspaceClient.
        """
        self._lakebase = lakebase_store
        self._delta = delta_store
        self._vs_endpoint = vs_endpoint_name
        self._vs_index = vs_index_name
        self._client = client or WorkspaceClient()

    def search(
        self,
        query: str,
        limit: int = 20,
        mode: str = "hybrid",
    ) -> list[SearchResult]:
        """Search the wiki using the specified mode.

        Args:
            query: Search query string.
            limit: Maximum results.
            mode: Search mode - 'fulltext', 'semantic', or 'hybrid'.

        Returns:
            List of SearchResult instances ordered by relevance.
        """
        if mode == "fulltext":
            return self._fulltext_search(query, limit)
        elif mode == "semantic":
            return self._semantic_search(query, limit)
        else:
            return self._hybrid_search(query, limit)

    def _fulltext_search(self, query: str, limit: int) -> list[SearchResult]:
        """Full-text search via Lakebase pg_trgm or Delta SQL LIKE fallback.

        Args:
            query: Search query.
            limit: Maximum results.

        Returns:
            List of SearchResult from full-text search.
        """
        # Try Lakebase first (fast pg_trgm search)
        if self._lakebase:
            try:
                return self._lakebase.search_pages(query, limit=limit)
            except Exception as e:
                logger.warning("Lakebase search failed, trying Delta", error=str(e))

        # Fallback to Delta SQL LIKE search
        if self._delta:
            try:
                pages = self._delta.search_pages(query, limit=limit)
                return [
                    SearchResult(
                        page_id=p.page_id,
                        title=p.title,
                        page_type=p.page_type.value,
                        snippet=p.content_markdown[:200] + "..." if p.content_markdown else "",
                        score=1.0,
                        source="fulltext",
                    )
                    for p in pages
                ]
            except Exception as e:
                logger.warning("Delta search failed", error=str(e))

        return []

    def _semantic_search(self, query: str, limit: int) -> list[SearchResult]:
        """Semantic search via Databricks Vector Search.

        Args:
            query: Search query.
            limit: Maximum results.

        Returns:
            List of SearchResult from vector search.
        """
        try:
            response = self._client.vector_search_indexes.query(
                index_name=self._vs_index,
                columns=["page_id", "title", "page_type", "content_markdown"],
                query_text=query,
                num_results=limit,
            )

            results: list[SearchResult] = []
            if response.result and response.result.data_array:
                for row in response.result.data_array:
                    results.append(SearchResult.from_vs_result(row))

            return results

        except Exception as e:
            logger.warning("Semantic search failed", error=str(e))
            return []

    def _hybrid_search(self, query: str, limit: int) -> list[SearchResult]:
        """Combine full-text and semantic search using reciprocal rank fusion.

        Args:
            query: Search query.
            limit: Maximum results.

        Returns:
            Merged and re-ranked list of SearchResult instances.
        """
        ft_results = self._fulltext_search(query, limit=limit)
        vs_results = self._semantic_search(query, limit=limit)

        return reciprocal_rank_fusion(ft_results, vs_results, limit=limit)


def reciprocal_rank_fusion(
    *result_lists: list[SearchResult],
    k: int = 60,
    limit: int = 20,
) -> list[SearchResult]:
    """Merge multiple ranked result lists using Reciprocal Rank Fusion.

    RRF score = sum(1 / (k + rank_i)) across all lists.

    Args:
        result_lists: Variable number of ranked result lists.
        k: RRF constant (default 60, standard in literature).
        limit: Maximum results to return.

    Returns:
        Merged and re-ranked list of SearchResult instances.
    """
    scores: dict[str, float] = {}
    best_result: dict[str, SearchResult] = {}

    for results in result_lists:
        for rank, result in enumerate(results):
            rrf_score = 1.0 / (k + rank + 1)
            scores[result.page_id] = scores.get(result.page_id, 0.0) + rrf_score

            if result.page_id not in best_result or len(result.snippet) > len(best_result[result.page_id].snippet):
                best_result[result.page_id] = result

    sorted_ids = sorted(scores.keys(), key=lambda pid: scores[pid], reverse=True)

    merged: list[SearchResult] = []
    for pid in sorted_ids[:limit]:
        result = best_result[pid]
        result.score = scores[pid]
        result.source = "hybrid"
        merged.append(result)

    return merged
