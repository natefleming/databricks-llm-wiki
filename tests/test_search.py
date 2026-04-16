"""Tests for search utilities."""

from llm_wiki.models import SearchResult
from llm_wiki.search import reciprocal_rank_fusion


class TestReciprocalRankFusion:
    """Tests for the RRF merge function."""

    def test_single_list(self) -> None:
        results = [
            SearchResult(page_id="a", title="A", score=1.0),
            SearchResult(page_id="b", title="B", score=0.5),
        ]
        merged = reciprocal_rank_fusion(results)
        assert len(merged) == 2
        assert merged[0].page_id == "a"

    def test_two_lists_overlap(self) -> None:
        list1 = [
            SearchResult(page_id="a", title="A", score=1.0, source="fulltext"),
            SearchResult(page_id="b", title="B", score=0.5, source="fulltext"),
        ]
        list2 = [
            SearchResult(page_id="b", title="B", score=1.0, source="semantic"),
            SearchResult(page_id="c", title="C", score=0.5, source="semantic"),
        ]
        merged = reciprocal_rank_fusion(list1, list2)

        # b appears in both lists so should score highest
        assert merged[0].page_id == "b"
        assert merged[0].source == "hybrid"
        assert len(merged) == 3

    def test_empty_lists(self) -> None:
        merged = reciprocal_rank_fusion([], [])
        assert merged == []

    def test_limit(self) -> None:
        results = [
            SearchResult(page_id=f"page-{i}", title=f"Page {i}", score=float(i))
            for i in range(10)
        ]
        merged = reciprocal_rank_fusion(results, limit=3)
        assert len(merged) == 3

    def test_deduplication(self) -> None:
        list1 = [SearchResult(page_id="a", title="A", snippet="short")]
        list2 = [SearchResult(page_id="a", title="A", snippet="longer snippet text")]
        merged = reciprocal_rank_fusion(list1, list2)

        assert len(merged) == 1
        # Should keep the result with the longer snippet
        assert "longer" in merged[0].snippet
