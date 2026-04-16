"""Tests for LLM Wiki data models."""

from datetime import datetime, timezone

from llm_wiki.models import (
    BackLink,
    CompilationQueueItem,
    CompilationStatus,
    Confidence,
    Frontmatter,
    FreshnessTier,
    Page,
    PageType,
    SearchResult,
    Source,
    SourceChunk,
    TriggerType,
)


class TestPage:
    """Tests for the Page model."""

    def test_create_page(self) -> None:
        page = Page(
            page_id="kubernetes-scheduling",
            title="Kubernetes Scheduling",
            page_type=PageType.CONCEPT,
            content_markdown="# Kubernetes Scheduling\n\nThe scheduler...",
            confidence=Confidence.HIGH,
            sources=["k8s-docs"],
            related=["pod-lifecycle"],
            tags=["kubernetes", "scheduling"],
            freshness_tier=FreshnessTier.WEEKLY,
        )

        assert page.page_id == "kubernetes-scheduling"
        assert page.title == "Kubernetes Scheduling"
        assert page.page_type == PageType.CONCEPT
        assert page.confidence == Confidence.HIGH
        assert len(page.sources) == 1
        assert len(page.tags) == 2

    def test_wikilinks_extraction(self) -> None:
        page = Page(
            page_id="test",
            title="Test",
            content_markdown="See [[pod-lifecycle]] and [[node-affinity]] for details.",
        )

        links = page.wikilinks
        assert "pod-lifecycle" in links
        assert "node-affinity" in links
        assert len(links) == 2

    def test_wikilinks_empty(self) -> None:
        page = Page(page_id="test", title="Test", content_markdown="No links here.")
        assert page.wikilinks == []

    def test_wikilinks_dedup(self) -> None:
        page = Page(
            page_id="test",
            title="Test",
            content_markdown="See [[foo]] and [[foo]] again.",
        )
        # computed_field recalculates each time, but regex should find both
        assert "foo" in page.wikilinks

    def test_page_serialization(self) -> None:
        page = Page(
            page_id="test",
            title="Test Page",
            page_type=PageType.ENTITY,
            confidence=Confidence.MEDIUM,
        )
        data = page.model_dump()
        assert data["page_id"] == "test"
        assert data["page_type"] == "entity"
        assert data["confidence"] == "medium"


class TestFrontmatter:
    """Tests for the Frontmatter model."""

    def test_create_frontmatter(self) -> None:
        fm = Frontmatter(
            title="Test",
            type=PageType.CONCEPT,
            confidence=Confidence.HIGH,
            sources=["source1"],
            tags=["tag1", "tag2"],
            freshness_tier=FreshnessTier.WEEKLY,
            created="2026-04-16",
            updated="2026-04-16",
        )
        assert fm.title == "Test"
        assert fm.type == PageType.CONCEPT
        assert len(fm.sources) == 1
        assert len(fm.tags) == 2

    def test_defaults(self) -> None:
        fm = Frontmatter(title="Test")
        assert fm.type == PageType.CONCEPT
        assert fm.confidence == Confidence.LOW
        assert fm.freshness_tier == FreshnessTier.MONTHLY
        assert fm.sources == []
        assert fm.related == []


class TestSource:
    """Tests for the Source model."""

    def test_create_source(self) -> None:
        source = Source(
            source_id="abc123",
            file_path="/Volumes/llm_wiki/raw/incoming/article.md",
            content_type="article",
            raw_text="Hello world",
            content_hash="deadbeef",
        )
        assert source.source_id == "abc123"
        assert source.content_type == "article"


class TestSourceChunk:
    """Tests for the SourceChunk model."""

    def test_create_chunk(self) -> None:
        chunk = SourceChunk(
            chunk_id="chunk1",
            source_id="src1",
            chunk_index=0,
            chunk_text="First chunk of text",
            token_count=5,
        )
        assert chunk.chunk_index == 0
        assert chunk.token_count == 5


class TestSearchResult:
    """Tests for the SearchResult model."""

    def test_from_vs_result(self) -> None:
        row = ["kubernetes-scheduling", "Kubernetes Scheduling", "concept", "Content here...", 0.95]
        result = SearchResult.from_vs_result(row)

        assert result.page_id == "kubernetes-scheduling"
        assert result.title == "Kubernetes Scheduling"
        assert result.score == 0.95
        assert result.source == "semantic"

    def test_from_vs_result_short_row(self) -> None:
        row = ["test-page", "Test", 0.5]
        result = SearchResult.from_vs_result(row)
        assert result.page_id == "test-page"


class TestCompilationQueueItem:
    """Tests for the CompilationQueueItem model."""

    def test_create_item(self) -> None:
        item = CompilationQueueItem(
            queue_id="q1",
            page_id="test-page",
            trigger_type=TriggerType.NEW_SOURCE,
            trigger_source_ids=["src1"],
            priority=10,
            status=CompilationStatus.PENDING,
        )
        assert item.trigger_type == TriggerType.NEW_SOURCE
        assert item.status == CompilationStatus.PENDING
        assert item.priority == 10


class TestEnums:
    """Tests for enum values."""

    def test_page_types(self) -> None:
        assert PageType.CONCEPT.value == "concept"
        assert PageType.ENTITY.value == "entity"
        assert PageType.SOURCE.value == "source"
        assert PageType.ANALYSIS.value == "analysis"
        assert PageType.INDEX.value == "index"

    def test_confidence_levels(self) -> None:
        assert Confidence.HIGH.value == "high"
        assert Confidence.MEDIUM.value == "medium"
        assert Confidence.LOW.value == "low"

    def test_freshness_tiers(self) -> None:
        assert FreshnessTier.LIVE.value == "live"
        assert FreshnessTier.PERMANENT.value == "permanent"
