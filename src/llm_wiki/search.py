"""Unified search combining Lakebase full-text search and Vector Search.

Provides a single search interface that merges results from both backends
using reciprocal rank fusion for optimal relevance.

Usage:
    from llm_wiki.search import WikiSearch

    search = WikiSearch(lakebase_store, vs_index_name="llm_wiki.wiki.pages_vs_index")
    results = search.search("kubernetes scheduling", limit=10)
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

from llm_wiki.log import logger
from llm_wiki.models import SearchResult
from llm_wiki.storage.lakebase import LakebaseStore


class WikiSearch:
    """Unified wiki search combining full-text and semantic search."""

    def __init__(
        self,
        lakebase_store: LakebaseStore,
        vs_endpoint_name: str = "llm-wiki-vs-endpoint",
        vs_index_name: str = "llm_wiki.wiki.pages_vs_index",
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the search engine.

        Args:
            lakebase_store: LakebaseStore for full-text search.
            vs_endpoint_name: Vector Search endpoint name.
            vs_index_name: Vector Search index name.
            client: Optional pre-configured WorkspaceClient.
        """
        self._lakebase = lakebase_store
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
        """Perform full-text search via Lakebase pg_trgm/tsvector.

        Args:
            query: Search query.
            limit: Maximum results.

        Returns:
            List of SearchResult from full-text search.
        """
        try:
            return self._lakebase.search_pages(query, limit=limit)
        except Exception as e:
            logger.warning("Full-text search failed", error=str(e))
            return []

    def _semantic_search(self, query: str, limit: int) -> list[SearchResult]:
        """Perform semantic search via Databricks Vector Search.

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

            # Keep the result with the longest snippet
            if result.page_id not in best_result or len(result.snippet) > len(best_result[result.page_id].snippet):
                best_result[result.page_id] = result

    # Sort by RRF score descending
    sorted_ids = sorted(scores.keys(), key=lambda pid: scores[pid], reverse=True)

    merged: list[SearchResult] = []
    for pid in sorted_ids[:limit]:
        result = best_result[pid]
        result.score = scores[pid]
        result.source = "hybrid"
        merged.append(result)

    return merged
