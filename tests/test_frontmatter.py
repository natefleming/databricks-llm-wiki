"""Tests for YAML frontmatter parsing and generation."""

from llm_wiki.compiler.frontmatter import (
    extract_wikilinks,
    parse_frontmatter,
    render_frontmatter,
)
from llm_wiki.models import Confidence, Frontmatter, FreshnessTier, PageType


class TestParseFrontmatter:
    """Tests for parse_frontmatter()."""

    def test_parse_valid_frontmatter(self) -> None:
        markdown = """---
title: "Kubernetes Scheduling"
type: concept
confidence: high
sources:
  - k8s-docs
  - distributed-paper
tags:
  - kubernetes
freshness_tier: weekly
created: "2026-04-16"
updated: "2026-04-16"
---

# Content here
"""
        fm, body = parse_frontmatter(markdown)

        assert fm is not None
        assert fm.title == "Kubernetes Scheduling"
        assert fm.type == PageType.CONCEPT
        assert fm.confidence == Confidence.HIGH
        assert len(fm.sources) == 2
        assert fm.freshness_tier == FreshnessTier.WEEKLY
        assert body.strip().startswith("# Content here")

    def test_parse_no_frontmatter(self) -> None:
        markdown = "# Just Content\n\nNo frontmatter here."
        fm, body = parse_frontmatter(markdown)

        assert fm is None
        assert body == markdown

    def test_parse_minimal_frontmatter(self) -> None:
        markdown = "---\ntitle: Test\n---\nBody"
        fm, body = parse_frontmatter(markdown)

        assert fm is not None
        assert fm.title == "Test"
        assert fm.type == PageType.CONCEPT  # default
        assert body.strip() == "Body"

    def test_parse_invalid_yaml(self) -> None:
        markdown = "---\n: invalid yaml [[\n---\nBody"
        fm, body = parse_frontmatter(markdown)
        # Should gracefully handle invalid YAML
        assert fm is None or body is not None


class TestRenderFrontmatter:
    """Tests for render_frontmatter()."""

    def test_render_full(self) -> None:
        fm = Frontmatter(
            title="Test Page",
            type=PageType.ENTITY,
            confidence=Confidence.MEDIUM,
            sources=["src1", "src2"],
            related=["related1"],
            tags=["tag1"],
            freshness_tier=FreshnessTier.DAILY,
            created="2026-04-16",
            updated="2026-04-16",
        )

        result = render_frontmatter(fm)

        assert result.startswith("---\n")
        assert result.endswith("---\n")
        assert "title: Test Page" in result
        assert "type: entity" in result
        assert "confidence: medium" in result
        assert "src1" in result
        assert "freshness_tier: daily" in result

    def test_render_minimal(self) -> None:
        fm = Frontmatter(title="Minimal")
        result = render_frontmatter(fm)

        assert "title: Minimal" in result
        assert "type: concept" in result

    def test_roundtrip(self) -> None:
        original = Frontmatter(
            title="Roundtrip Test",
            type=PageType.ANALYSIS,
            confidence=Confidence.HIGH,
            sources=["s1"],
            tags=["t1", "t2"],
            freshness_tier=FreshnessTier.MONTHLY,
            created="2026-01-01",
            updated="2026-04-16",
        )

        rendered = render_frontmatter(original)
        parsed, _ = parse_frontmatter(rendered + "\nBody content")

        assert parsed is not None
        assert parsed.title == original.title
        assert parsed.type == original.type
        assert parsed.confidence == original.confidence
        assert parsed.sources == original.sources
        assert parsed.freshness_tier == original.freshness_tier


class TestExtractWikilinks:
    """Tests for extract_wikilinks()."""

    def test_basic_links(self) -> None:
        content = "See [[kubernetes]] and [[docker]] for more."
        links = extract_wikilinks(content)
        assert links == ["kubernetes", "docker"]

    def test_link_with_display_text(self) -> None:
        content = "See [[kubernetes|K8s]] for more."
        links = extract_wikilinks(content)
        assert links == ["kubernetes"]

    def test_link_with_section(self) -> None:
        content = "See [[kubernetes#scheduling]] for more."
        links = extract_wikilinks(content)
        assert links == ["kubernetes"]

    def test_deduplicated(self) -> None:
        content = "See [[foo]] and then [[foo]] again."
        links = extract_wikilinks(content)
        assert links == ["foo"]

    def test_no_links(self) -> None:
        content = "No links here at all."
        links = extract_wikilinks(content)
        assert links == []

    def test_mixed_formats(self) -> None:
        content = "[[simple]] and [[with-section#heading]] and [[aliased|display]]"
        links = extract_wikilinks(content)
        assert "simple" in links
        assert "with-section" in links
        assert "aliased" in links
