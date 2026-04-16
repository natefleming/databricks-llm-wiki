"""Pydantic data models for LLM Wiki.

Defines the core domain objects: pages, sources, chunks, backlinks,
compilation queue items, and activity log entries.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, computed_field


class PageType(str, Enum):
    """Supported wiki page types."""

    CONCEPT = "concept"
    ENTITY = "entity"
    SOURCE = "source"
    ANALYSIS = "analysis"
    INDEX = "index"


class Confidence(str, Enum):
    """Page confidence levels."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class FreshnessTier(str, Enum):
    """Content freshness tiers controlling staleness detection."""

    LIVE = "live"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    PERMANENT = "permanent"


class CompilationStatus(str, Enum):
    """Status of a compilation queue item."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class TriggerType(str, Enum):
    """What triggered a compilation request."""

    NEW_SOURCE = "new_source"
    SOURCE_UPDATED = "source_updated"
    MANUAL = "manual"
    STALE = "stale"


class Frontmatter(BaseModel):
    """YAML frontmatter for a wiki page (Obsidian-compatible)."""

    title: str
    type: PageType = PageType.CONCEPT
    confidence: Confidence = Confidence.LOW
    sources: list[str] = Field(default_factory=list)
    related: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    freshness_tier: FreshnessTier = FreshnessTier.MONTHLY
    created: str = ""
    updated: str = ""


class Page(BaseModel):
    """A compiled wiki page."""

    page_id: str = Field(description="URL-safe slug, e.g. 'kubernetes-scheduling'")
    title: str
    page_type: PageType = PageType.CONCEPT
    content_markdown: str = ""
    frontmatter: Frontmatter | None = None
    confidence: Confidence = Confidence.LOW
    sources: list[str] = Field(default_factory=list)
    related: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    freshness_tier: FreshnessTier = FreshnessTier.MONTHLY
    content_hash: str = ""
    created_at: datetime | None = None
    updated_at: datetime | None = None
    compiled_by: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def wikilinks(self) -> list[str]:
        """Extract all [[wikilink]] references from the page content."""
        return re.findall(r"\[\[([a-z0-9][a-z0-9-]*)\]\]", self.content_markdown)


class Source(BaseModel):
    """A raw source document ingested into the wiki."""

    source_id: str
    file_path: str
    content_type: str = "article"
    raw_text: str = ""
    content_hash: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)
    ingested_at: datetime | None = None


class SourceChunk(BaseModel):
    """A chunk of text from a source document."""

    chunk_id: str
    source_id: str
    chunk_index: int = 0
    chunk_text: str = ""
    token_count: int = 0


class BackLink(BaseModel):
    """A cross-reference link between two wiki pages."""

    source_page_id: str
    target_page_id: str
    link_text: str = ""
    context_snippet: str = ""


class CompilationQueueItem(BaseModel):
    """An item in the compilation queue waiting to be processed."""

    queue_id: str
    page_id: str
    trigger_type: TriggerType = TriggerType.NEW_SOURCE
    trigger_source_ids: list[str] = Field(default_factory=list)
    priority: int = 0
    status: CompilationStatus = CompilationStatus.PENDING
    created_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str = ""


class ActivityLogEntry(BaseModel):
    """A log entry recording a wiki operation."""

    log_id: str
    operation: str
    details: str = ""
    page_ids: list[str] = Field(default_factory=list)
    timestamp: datetime | None = None


class SearchResult(BaseModel):
    """A search result combining full-text and semantic scores."""

    page_id: str
    title: str
    page_type: str = ""
    snippet: str = ""
    score: float = 0.0
    source: str = "fulltext"

    @classmethod
    def from_vs_result(cls, row: list[Any]) -> SearchResult:
        """Create a SearchResult from a Vector Search result row.

        Args:
            row: Row from Vector Search query [page_id, title, page_type, content, score].

        Returns:
            A SearchResult instance.
        """
        page_type = ""
        if len(row) > 2 and isinstance(row[2], str):
            page_type = row[2]

        snippet = ""
        if len(row) > 3 and isinstance(row[3], str) and row[3]:
            snippet = row[3][:200] + "..."

        score = row[-1] if isinstance(row[-1], (int, float)) else 0.0

        return cls(
            page_id=row[0],
            title=row[1],
            page_type=page_type,
            snippet=snippet,
            score=score,
            source="semantic",
        )
