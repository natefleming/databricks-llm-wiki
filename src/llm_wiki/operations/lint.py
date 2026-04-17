"""Lint operation: wiki health checks and maintenance.

Scans the wiki for quality issues: stale pages, broken links,
orphan pages, and low-confidence content.

Usage:
    from llm_wiki.operations.lint import WikiLinter

    linter = WikiLinter(delta_store, config)
    report = linter.run()
"""

from __future__ import annotations

from datetime import datetime, timezone

from llm_wiki.config import WikiConfig
from llm_wiki.log import logger
from llm_wiki.models import Page
from llm_wiki.storage.delta import DeltaStore


class LintIssue:
    """A single lint issue found during health check."""

    def __init__(self, page_id: str, category: str, severity: str, message: str) -> None:
        self.page_id = page_id
        self.category = category
        self.severity = severity  # error, warning, info
        self.message = message

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary representation."""
        return {
            "page_id": self.page_id,
            "category": self.category,
            "severity": self.severity,
            "message": self.message,
        }


class WikiLinter:
    """Runs health checks on the wiki and reports issues."""

    def __init__(self, store: DeltaStore, config: WikiConfig) -> None:
        """Initialize the linter.

        Args:
            store: DeltaStore for reading wiki data.
            config: Wiki configuration with freshness tiers.
        """
        self._store = store
        self._config = config

    def run(self, check_contradictions: bool = False, contradiction_sample: int = 20) -> dict[str, list | dict | int]:
        """Run all lint checks and return a report.

        Args:
            check_contradictions: If True, also run LLM-based contradiction
                                  detection on related page pairs (costs LLM tokens).
            contradiction_sample: Max page-pairs to check when contradiction
                                  detection is enabled.

        Returns:
            Dictionary with 'issues', 'summary', and 'total_issues'.
        """
        logger.info("Running wiki lint checks", check_contradictions=check_contradictions)
        issues: list[LintIssue] = []

        pages = self._store.list_pages(limit=10000)

        issues.extend(self._check_stale_pages(pages))
        issues.extend(self._check_broken_links(pages))
        issues.extend(self._check_orphan_pages(pages))
        issues.extend(self._check_low_confidence(pages))
        issues.extend(self._check_missing_content(pages))

        if check_contradictions:
            issues.extend(self._check_contradictions(pages, sample=contradiction_sample))

        # Build summary
        summary: dict[str, int] = {}
        for issue in issues:
            key = f"{issue.severity}_{issue.category}"
            summary[key] = summary.get(key, 0) + 1

        # Log activity
        self._store.log_activity(
            "lint",
            f"Lint found {len(issues)} issues across {len(pages)} pages",
        )

        logger.info("Lint complete", total_issues=len(issues), pages_checked=len(pages))

        return {
            "issues": [i.to_dict() for i in issues],
            "summary": summary,
            "total_issues": len(issues),
            "pages_checked": len(pages),
        }

    def _check_stale_pages(self, pages: list[Page]) -> list[LintIssue]:
        """Check for pages past their freshness tier TTL."""
        issues: list[LintIssue] = []
        now = datetime.now(timezone.utc)

        for page in pages:
            if not page.updated_at:
                continue

            tier_config = self._config.freshness_tiers.get(page.freshness_tier.value)
            if not tier_config or tier_config.max_age_minutes is None:
                continue  # permanent tier

            age_minutes = (now - page.updated_at).total_seconds() / 60
            if age_minutes > tier_config.max_age_minutes:
                issues.append(LintIssue(
                    page_id=page.page_id,
                    category="stale",
                    severity="warning",
                    message=(
                        f"Page is stale: {age_minutes:.0f} min old, "
                        f"tier '{page.freshness_tier.value}' allows {tier_config.max_age_minutes} min"
                    ),
                ))

        return issues

    def _check_broken_links(self, pages: list[Page]) -> list[LintIssue]:
        """Check for [[wikilinks]] pointing to non-existent pages."""
        issues: list[LintIssue] = []
        existing_ids = {p.page_id for p in pages}

        for page in pages:
            for link in page.wikilinks:
                if link not in existing_ids:
                    issues.append(LintIssue(
                        page_id=page.page_id,
                        category="broken_link",
                        severity="warning",
                        message=f"Broken link: [[{link}]] does not exist",
                    ))

        return issues

    def _check_orphan_pages(self, pages: list[Page]) -> list[LintIssue]:
        """Check for pages with no incoming links (orphans)."""
        issues: list[LintIssue] = []

        # Build set of all linked-to pages
        linked_to: set[str] = set()
        for page in pages:
            linked_to.update(page.wikilinks)

        for page in pages:
            if page.page_id not in linked_to and page.page_type.value != "index":
                issues.append(LintIssue(
                    page_id=page.page_id,
                    category="orphan",
                    severity="info",
                    message="Orphan page: no other pages link to this page",
                ))

        return issues

    def _check_low_confidence(self, pages: list[Page]) -> list[LintIssue]:
        """Check for pages with low confidence that could be improved."""
        issues: list[LintIssue] = []

        for page in pages:
            if page.confidence.value == "low" and len(page.sources) == 0:
                issues.append(LintIssue(
                    page_id=page.page_id,
                    category="low_confidence",
                    severity="info",
                    message="Low confidence with no sources - consider adding source material",
                ))

        return issues

    def _check_missing_content(self, pages: list[Page]) -> list[LintIssue]:
        """Check for pages with very little content."""
        issues: list[LintIssue] = []

        for page in pages:
            content_len = len(page.content_markdown.strip())
            if content_len < 100:
                issues.append(LintIssue(
                    page_id=page.page_id,
                    category="thin_content",
                    severity="warning",
                    message=f"Thin content: only {content_len} characters",
                ))

        return issues

    def _check_contradictions(self, pages: list[Page], sample: int = 20) -> list[LintIssue]:
        """LLM-based check for contradictions between linked page pairs.

        To keep cost bounded, only checks pairs where page A references page B
        via a wikilink. Samples up to `sample` such pairs (highest-confidence pages first).
        """
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        except Exception:
            return []

        issues: list[LintIssue] = []

        # Build candidate pairs: (source_page, target_page) where source links to target
        page_by_id = {p.page_id: p for p in pages}

        # Prioritize pairs between high/medium confidence pages
        conf_rank = {"high": 0, "medium": 1, "low": 2}
        ranked_pages = sorted(pages, key=lambda p: conf_rank.get(p.confidence.value, 99))

        pairs: list[tuple[Page, Page]] = []
        seen: set[tuple[str, str]] = set()
        for src in ranked_pages:
            for target_slug in src.wikilinks:
                target = page_by_id.get(target_slug)
                if not target:
                    continue
                key = tuple(sorted([src.page_id, target_slug]))
                if key in seen:
                    continue
                seen.add(key)
                pairs.append((src, target))
                if len(pairs) >= sample:
                    break
            if len(pairs) >= sample:
                break

        if not pairs:
            return issues

        try:
            client = WorkspaceClient()
        except Exception as e:
            logger.warning("Cannot init client for contradiction check", error=str(e))
            return []

        model = self._config.wiki.default_model

        for src, tgt in pairs:
            system = (
                "You are a fact-checker. Compare two wiki pages and report any "
                "factual contradictions between them. Return STRICT JSON: "
                '{"contradiction": true|false, "details": "brief description"}. '
                "Report a contradiction ONLY if the pages make directly conflicting claims "
                "about the same subject, not just different emphases."
            )
            user = (
                f"Page A: [[{src.page_id}]] {src.title}\n"
                f"{src.content_markdown[:3000]}\n\n"
                f"---\n\n"
                f"Page B: [[{tgt.page_id}]] {tgt.title}\n"
                f"{tgt.content_markdown[:3000]}"
            )
            try:
                resp = client.serving_endpoints.query(
                    name=model,
                    messages=[
                        ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                        ChatMessage(role=ChatMessageRole.USER, content=user),
                    ],
                    max_tokens=300,
                    temperature=0.0,
                )
                text = ""
                if hasattr(resp, "choices") and resp.choices:
                    c = resp.choices[0]
                    if hasattr(c, "message") and hasattr(c.message, "content"):
                        text = c.message.content or ""
                    elif isinstance(c, dict):
                        text = c.get("message", {}).get("content", "")

                import json as _json, re as _re
                m = _re.search(r"\{.*\}", text, _re.DOTALL)
                if not m:
                    continue
                try:
                    data = _json.loads(m.group(0))
                except Exception:
                    continue

                if data.get("contradiction"):
                    issues.append(LintIssue(
                        page_id=src.page_id,
                        category="contradiction",
                        severity="warning",
                        message=f"Conflicts with [[{tgt.page_id}]]: {data.get('details','')[:200]}",
                    ))
            except Exception as e:
                logger.warning("Contradiction check failed", pair=(src.page_id, tgt.page_id), error=str(e))

        return issues
