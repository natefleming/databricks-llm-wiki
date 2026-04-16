"""Context assembly for wiki page compilation.

Gathers relevant source chunks and related existing pages to provide
rich context for LLM compilation.

Usage:
    from llm_wiki.compiler.context import ContextAssembler

    assembler = ContextAssembler(delta_store)
    source_text, related_text = assembler.assemble("kubernetes-scheduling", source_ids)
"""

from __future__ import annotations

from llm_wiki.log import logger
from llm_wiki.models import Page, SourceChunk
from llm_wiki.storage.delta import DeltaStore


class ContextAssembler:
    """Assembles context from sources and related pages for compilation."""

    def __init__(self, store: DeltaStore, max_source_tokens: int = 8000, max_related_tokens: int = 4000) -> None:
        """Initialize the context assembler.

        Args:
            store: DeltaStore instance for reading sources and pages.
            max_source_tokens: Maximum token budget for source chunks.
            max_related_tokens: Maximum token budget for related pages.
        """
        self.store = store
        self.max_source_tokens = max_source_tokens
        self.max_related_tokens = max_related_tokens

    def assemble(
        self,
        page_id: str,
        source_ids: list[str],
    ) -> tuple[str, str]:
        """Assemble context for compiling a page.

        Gathers source chunks from the given source IDs and finds
        related existing pages via backlinks and tag overlap.

        Args:
            page_id: The target page slug being compiled.
            source_ids: Source document IDs to pull chunks from.

        Returns:
            Tuple of (formatted_source_chunks, formatted_related_pages).
        """
        source_text = self._gather_source_chunks(source_ids)
        related_text = self._gather_related_pages(page_id)

        logger.debug(
            "Assembled context",
            page_id=page_id,
            source_ids=len(source_ids),
            source_chars=len(source_text),
            related_chars=len(related_text),
        )

        return source_text, related_text

    def _gather_source_chunks(self, source_ids: list[str]) -> str:
        """Gather and format source chunks within the token budget.

        Args:
            source_ids: Source document IDs.

        Returns:
            Formatted string of source chunks.
        """
        if not source_ids:
            return ""

        all_chunks: list[SourceChunk] = []
        for sid in source_ids:
            chunks = self.store.get_source_chunks(sid)
            all_chunks.extend(chunks)

        # Sort by chunk_index within each source, then trim to budget
        all_chunks.sort(key=lambda c: (c.source_id, c.chunk_index))

        lines: list[str] = []
        token_count = 0

        for chunk in all_chunks:
            if token_count + chunk.token_count > self.max_source_tokens:
                break
            lines.append(f"[Source: {chunk.source_id}, chunk {chunk.chunk_index}]")
            lines.append(chunk.chunk_text)
            lines.append("")
            token_count += chunk.token_count

        return "\n".join(lines)

    def _gather_related_pages(self, page_id: str) -> str:
        """Find and format related existing pages.

        Looks for pages that link to or from the target page,
        and pages with overlapping tags.

        Args:
            page_id: The target page slug.

        Returns:
            Formatted string of related page summaries.
        """
        # Get pages that link to this page
        backlinks = self.store.get_backlinks(page_id)
        related_ids = {bl.source_page_id for bl in backlinks}

        # Also check if the target page already exists (for updates)
        existing = self.store.get_page(page_id)
        if existing and existing.related:
            related_ids.update(existing.related)

        if not related_ids:
            return ""

        lines: list[str] = []
        token_count = 0
        est_tokens_per_char = 0.25  # rough estimate

        for rid in related_ids:
            page = self.store.get_page(rid)
            if page is None:
                continue

            # Take first 500 chars of content as summary
            summary = page.content_markdown[:500]
            est_tokens = int(len(summary) * est_tokens_per_char)

            if token_count + est_tokens > self.max_related_tokens:
                break

            lines.append(f"### [[{page.page_id}]] - {page.title}")
            lines.append(f"Type: {page.page_type.value} | Confidence: {page.confidence.value}")
            lines.append(summary)
            if len(page.content_markdown) > 500:
                lines.append("...")
            lines.append("")
            token_count += est_tokens

        return "\n".join(lines)

    def assemble_query_context(
        self,
        relevant_pages: list[Page],
        max_tokens: int = 8000,
    ) -> str:
        """Assemble context for answering a query from wiki pages.

        Args:
            relevant_pages: Pages retrieved by search.
            max_tokens: Maximum token budget.

        Returns:
            Formatted context string with page content and citations.
        """
        lines: list[str] = []
        token_count = 0
        est_tokens_per_char = 0.25

        for page in relevant_pages:
            content = page.content_markdown
            est_tokens = int(len(content) * est_tokens_per_char)

            if token_count + est_tokens > max_tokens:
                # Truncate this page to fit
                remaining_chars = int((max_tokens - token_count) / est_tokens_per_char)
                content = content[:remaining_chars] + "..."

            lines.append(f"## [[{page.page_id}]] - {page.title}")
            lines.append(f"Type: {page.page_type.value} | Confidence: {page.confidence.value}")
            lines.append(content)
            lines.append("")

            token_count += int(len(content) * est_tokens_per_char)
            if token_count >= max_tokens:
                break

        return "\n".join(lines)
