"""LLM compilation engine for wiki pages.

Orchestrates the compilation of raw source material into structured
wiki pages using the Databricks Foundation Model API.

Usage:
    from llm_wiki.compiler.engine import WikiCompiler

    compiler = WikiCompiler(delta_store, config)
    results = compiler.compile_pending(limit=10)
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from llm_wiki.compiler.context import ContextAssembler
from llm_wiki.compiler.frontmatter import extract_wikilinks
from llm_wiki.compiler.prompts import get_compilation_prompt
from llm_wiki.config import WikiConfig
from llm_wiki.log import logger
from llm_wiki.models import (
    BackLink,
    CompilationQueueItem,
    CompilationStatus,
    Confidence,
    Frontmatter,
    FreshnessTier,
    Page,
    PageType,
)
from llm_wiki.storage.delta import DeltaStore


class CompilationResult:
    """Result of compiling a single page."""

    def __init__(
        self,
        queue_item: CompilationQueueItem,
        page: Page | None = None,
        error: str = "",
        tokens_used: int = 0,
    ) -> None:
        self.queue_item = queue_item
        self.page = page
        self.error = error
        self.tokens_used = tokens_used

    @property
    def success(self) -> bool:
        """Whether the compilation succeeded."""
        return self.page is not None and not self.error


class WikiCompiler:
    """Compiles raw sources into wiki pages using FMAPI."""

    def __init__(
        self,
        store: DeltaStore,
        config: WikiConfig,
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the wiki compiler.

        Args:
            store: DeltaStore for reading/writing wiki data.
            config: Wiki configuration.
            client: Optional pre-configured WorkspaceClient.
        """
        self.store = store
        self.config = config
        self._client = client or WorkspaceClient()
        self._assembler = ContextAssembler(store)

    def compile_pending(self, limit: int = 50) -> list[CompilationResult]:
        """Process all pending items in the compilation queue.

        Args:
            limit: Maximum items to process in this run.

        Returns:
            List of CompilationResult instances.
        """
        pending = self.store.get_pending_compilations(limit=limit)
        if not pending:
            logger.info("No pending compilations")
            return []

        logger.info("Starting compilation", pending_count=len(pending))
        max_workers = self.config.wiki.max_compilation_concurrency
        results: list[CompilationResult] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._compile_one, item): item
                for item in pending
            }

            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error("Compilation failed", page_id=item.page_id, error=str(e))
                    self.store.update_compilation_status(
                        item.queue_id, CompilationStatus.FAILED, str(e)
                    )
                    results.append(CompilationResult(item, error=str(e)))

        succeeded = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        total_tokens = sum(r.tokens_used for r in results)
        logger.info(
            "Compilation complete",
            succeeded=succeeded,
            failed=failed,
            total_tokens=total_tokens,
        )

        return results

    def compile_page(
        self,
        page_id: str,
        source_ids: list[str],
        page_type: str = "concept",
        title: str | None = None,
    ) -> Page:
        """Compile a single page from source material.

        Args:
            page_id: Target page slug.
            source_ids: Source document IDs to compile from.
            page_type: Type of page to create.
            title: Optional title override.

        Returns:
            Compiled Page instance.
        """
        display_title = title or page_id.replace("-", " ").title()

        # Assemble context
        source_text, related_text = self._assembler.assemble(page_id, source_ids)

        # Build prompt
        messages = get_compilation_prompt(page_type, display_title, source_text, related_text)

        # Call FMAPI
        response = self._call_llm(messages)
        content = response["content"]
        tokens_used = response.get("tokens_used", 0)

        # Extract wikilinks for backlinks
        wikilinks = extract_wikilinks(content)

        # Determine confidence
        confidence = self._assess_confidence(source_ids)

        # Get freshness tier from config
        type_config = self.config.page_types.get(page_type)
        freshness = type_config.default_freshness if type_config else "monthly"

        now = datetime.now(timezone.utc)

        page = Page(
            page_id=page_id,
            title=display_title,
            page_type=PageType(page_type),
            content_markdown=content,
            frontmatter=Frontmatter(
                title=display_title,
                type=PageType(page_type),
                confidence=confidence,
                sources=source_ids,
                related=wikilinks,
                tags=self._extract_tags(content),
                freshness_tier=FreshnessTier(freshness),
                created=now.strftime("%Y-%m-%d"),
                updated=now.strftime("%Y-%m-%d"),
            ),
            confidence=confidence,
            sources=source_ids,
            related=wikilinks,
            tags=self._extract_tags(content),
            freshness_tier=FreshnessTier(freshness),
            created_at=now,
            updated_at=now,
            compiled_by=self.config.wiki.default_model,
        )

        return page

    # ──────────────────────────────────────────────
    # Internal methods
    # ──────────────────────────────────────────────

    def _compile_one(self, item: CompilationQueueItem) -> CompilationResult:
        """Compile a single queue item.

        Args:
            item: The CompilationQueueItem to process.

        Returns:
            CompilationResult instance.
        """
        logger.info("Compiling page", page_id=item.page_id, trigger=item.trigger_type.value)

        # Mark as in progress
        self.store.update_compilation_status(item.queue_id, CompilationStatus.IN_PROGRESS)

        try:
            # Determine page type from existing page or default to concept
            existing = self.store.get_page(item.page_id)
            page_type = existing.page_type.value if existing else "concept"
            title = existing.title if existing else None

            # Compile
            page = self.compile_page(
                item.page_id,
                item.trigger_source_ids,
                page_type=page_type,
                title=title,
            )

            # Write page
            self.store.upsert_page(page)

            # Write backlinks
            backlinks = [
                BackLink(
                    source_page_id=page.page_id,
                    target_page_id=target,
                    link_text=target,
                )
                for target in page.wikilinks
            ]
            self.store.upsert_backlinks(backlinks)

            # Log activity
            self.store.log_activity(
                "compile",
                f"Compiled page '{page.title}' from {len(item.trigger_source_ids)} sources",
                [page.page_id],
            )

            # Mark as completed
            self.store.update_compilation_status(item.queue_id, CompilationStatus.COMPLETED)

            return CompilationResult(item, page=page)

        except Exception as e:
            error_msg = str(e)
            logger.error("Compilation failed", page_id=item.page_id, error=error_msg)
            self.store.update_compilation_status(
                item.queue_id, CompilationStatus.FAILED, error_msg
            )
            return CompilationResult(item, error=error_msg)

    def _call_llm(self, messages: list[dict[str, str]], max_retries: int = 3) -> dict[str, Any]:
        """Call the Foundation Model API with retry logic.

        Args:
            messages: Chat messages for the LLM.
            max_retries: Maximum retry attempts.

        Returns:
            Dict with 'content' and 'tokens_used'.
        """
        endpoint = self.config.wiki.default_model
        last_error = None

        for attempt in range(max_retries):
            try:
                response = self._client.serving_endpoints.query(
                    name=endpoint,
                    messages=messages,
                    max_tokens=4096,
                    temperature=0.3,
                )

                content = ""
                tokens_used = 0

                if hasattr(response, "choices") and response.choices:
                    choice = response.choices[0]
                    if hasattr(choice, "message") and choice.message:
                        content = choice.message.content or ""
                    tokens_used = getattr(response, "usage", None)
                    if tokens_used and hasattr(tokens_used, "total_tokens"):
                        tokens_used = tokens_used.total_tokens
                    else:
                        tokens_used = 0

                return {"content": content, "tokens_used": tokens_used}

            except Exception as e:
                last_error = e
                logger.warning(
                    "LLM call failed, retrying",
                    attempt=attempt + 1,
                    error=str(e),
                )
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # exponential backoff

        raise RuntimeError(f"LLM call failed after {max_retries} attempts: {last_error}")

    def _assess_confidence(self, source_ids: list[str]) -> Confidence:
        """Assess confidence level based on number and quality of sources.

        Args:
            source_ids: Source document IDs used for compilation.

        Returns:
            Confidence level.
        """
        if len(source_ids) >= 2:
            return Confidence.HIGH
        elif len(source_ids) == 1:
            return Confidence.MEDIUM
        return Confidence.LOW

    @staticmethod
    def _extract_tags(content: str) -> list[str]:
        """Extract potential tags from content based on common patterns.

        Looks for bold terms, heading keywords, and recurring concepts.

        Args:
            content: Markdown content.

        Returns:
            List of extracted tags (max 10).
        """
        # Extract bold terms as potential tags
        bold_terms = re.findall(r"\*\*([a-zA-Z][a-zA-Z\s-]{2,30})\*\*", content)
        tags = [t.lower().strip().replace(" ", "-") for t in bold_terms[:10]]
        return list(dict.fromkeys(tags))  # deduplicate
