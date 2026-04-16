"""YAML frontmatter parsing and generation for wiki pages.

Handles the bidirectional conversion between Pydantic Frontmatter models
and YAML frontmatter strings in Obsidian-compatible markdown.

Usage:
    from llm_wiki.compiler.frontmatter import parse_frontmatter, render_frontmatter

    fm = parse_frontmatter(markdown_string)
    yaml_str = render_frontmatter(frontmatter_model)
"""

from __future__ import annotations

import re
from typing import Any

import yaml

from llm_wiki.models import Confidence, Frontmatter, FreshnessTier, PageType

# Regex to extract YAML frontmatter between --- delimiters
_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n?", re.DOTALL)


def parse_frontmatter(markdown: str) -> tuple[Frontmatter | None, str]:
    """Parse YAML frontmatter from a markdown string.

    Args:
        markdown: Markdown content potentially starting with --- delimiters.

    Returns:
        Tuple of (Frontmatter model or None, remaining body content).
    """
    match = _FRONTMATTER_RE.match(markdown)
    if not match:
        return None, markdown

    yaml_str = match.group(1)
    body = markdown[match.end():]

    try:
        data: dict[str, Any] = yaml.safe_load(yaml_str) or {}
    except yaml.YAMLError:
        return None, markdown

    fm = Frontmatter(
        title=data.get("title", ""),
        type=_safe_enum(PageType, data.get("type"), PageType.CONCEPT),
        confidence=_safe_enum(Confidence, data.get("confidence"), Confidence.LOW),
        sources=_ensure_list(data.get("sources")),
        related=_ensure_list(data.get("related")),
        tags=_ensure_list(data.get("tags")),
        freshness_tier=_safe_enum(
            FreshnessTier, data.get("freshness_tier"), FreshnessTier.MONTHLY
        ),
        created=str(data.get("created", "")),
        updated=str(data.get("updated", "")),
    )

    return fm, body


def render_frontmatter(fm: Frontmatter) -> str:
    """Render a Frontmatter model as a YAML frontmatter string.

    Args:
        fm: The Frontmatter model to render.

    Returns:
        YAML frontmatter string with --- delimiters.
    """
    data: dict[str, Any] = {
        "title": fm.title,
        "type": fm.type.value,
        "confidence": fm.confidence.value,
    }

    if fm.sources:
        data["sources"] = fm.sources
    if fm.related:
        data["related"] = fm.related
    if fm.tags:
        data["tags"] = fm.tags

    data["freshness_tier"] = fm.freshness_tier.value

    if fm.created:
        data["created"] = fm.created
    if fm.updated:
        data["updated"] = fm.updated

    yaml_str = yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    return f"---\n{yaml_str}---\n"


def extract_wikilinks(content: str) -> list[str]:
    """Extract all [[wikilink]] slugs from markdown content.

    Handles formats: [[slug]], [[slug|display text]], [[slug#section]].

    Args:
        content: Markdown content to scan.

    Returns:
        Deduplicated list of referenced page slugs.
    """
    pattern = r"\[\[([a-z0-9][a-z0-9-]*)(?:#[a-z0-9-]*)?(?:\|[^\]]+)?\]\]"
    matches = re.findall(pattern, content)
    return list(dict.fromkeys(matches))  # deduplicate preserving order


def _safe_enum(enum_cls: type, value: Any, default: Any) -> Any:
    """Safely convert a value to an enum, returning default on failure."""
    if value is None:
        return default
    try:
        return enum_cls(str(value).lower())
    except ValueError:
        return default


def _ensure_list(value: Any) -> list[str]:
    """Ensure a value is a list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]
