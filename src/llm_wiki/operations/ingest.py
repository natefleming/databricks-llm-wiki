"""Ingest operation: upload raw sources and trigger processing.

Handles the entry point for adding new content to the wiki:
- Upload text/URL/file to the incoming UC Volume
- Optionally trigger the SDP pipeline
- Optionally enqueue immediate compilation

Usage:
    from llm_wiki.operations.ingest import ingest_source

    result = ingest_source(volume_store, delta_store, text="...", title="My Article")
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

import httpx

from llm_wiki.log import logger
from llm_wiki.storage.delta import DeltaStore
from llm_wiki.storage.volumes import VolumeStore


def ingest_source(
    volume_store: VolumeStore,
    delta_store: DeltaStore,
    text: str | None = None,
    url: str | None = None,
    file_path: str | None = None,
    title: str | None = None,
) -> dict[str, str]:
    """Ingest a new source into the wiki.

    Accepts text, URL, or file path. Uploads to the incoming volume
    and optionally enqueues compilation.

    Args:
        volume_store: VolumeStore for uploading to UC Volumes.
        delta_store: DeltaStore for enqueuing compilation.
        text: Raw text content to ingest.
        url: URL to fetch and ingest.
        file_path: Local file path to ingest.
        title: Optional title for the source.

    Returns:
        Dictionary with 'source_path', 'slug', and 'status'.
    """
    content: str = ""
    source_title = title or "untitled"

    if url:
        content, source_title = _fetch_url(url)
        if not title:
            title = source_title
    elif text:
        content = text
        if not title:
            # Extract title from first heading
            for line in content.split("\n")[:10]:
                if line.strip().startswith("# "):
                    source_title = line.strip()[2:]
                    break
    elif file_path:
        from pathlib import Path

        p = Path(file_path)
        content = p.read_text(encoding="utf-8", errors="replace")
        if not title:
            source_title = p.stem.replace("-", " ").replace("_", " ").title()

    if not content:
        return {"source_path": "", "slug": "", "status": "error: no content provided"}

    # Generate slug from title
    slug = _generate_slug(source_title)
    filename = f"{slug}.md"

    # Upload to incoming volume
    source_path = volume_store.upload_source(filename, content)

    # Enqueue compilation
    content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
    queue_id = delta_store.enqueue_compilation(
        page_id=slug,
        trigger_type="new_source",
        priority=10,
    )

    # Log activity
    delta_store.log_activity(
        "ingest",
        f"Ingested source '{source_title}' from {'url' if url else 'text' if text else 'file'}",
        [slug],
    )

    logger.info("Source ingested", slug=slug, path=source_path, queue_id=queue_id)

    return {
        "source_path": source_path,
        "slug": slug,
        "title": source_title,
        "status": "ingested",
        "queue_id": queue_id,
    }


def _fetch_url(url: str) -> tuple[str, str]:
    """Fetch content from a URL.

    Tries Jina Reader API first for clean markdown extraction,
    then falls back to raw HTTP fetch.

    Args:
        url: URL to fetch.

    Returns:
        Tuple of (content, title).
    """
    # Try Jina Reader API for clean markdown
    try:
        jina_url = f"https://r.jina.ai/{url}"
        response = httpx.get(
            jina_url,
            headers={"Accept": "text/markdown"},
            timeout=30.0,
            follow_redirects=True,
        )
        if response.status_code == 200:
            content = response.text
            # Extract title from first heading
            title = url
            for line in content.split("\n")[:10]:
                if line.strip().startswith("# "):
                    title = line.strip()[2:]
                    break
            logger.info("Fetched via Jina Reader", url=url)
            return content, title
    except Exception as e:
        logger.debug("Jina Reader failed, trying direct fetch", error=str(e))

    # Fallback: direct HTTP fetch
    try:
        response = httpx.get(url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
        content = response.text

        # Basic HTML to text if needed
        if "<html" in content.lower()[:200]:
            content = _html_to_text(content)

        title = url.split("/")[-1].rsplit(".", 1)[0].replace("-", " ").title()
        return content, title
    except Exception as e:
        logger.error("URL fetch failed", url=url, error=str(e))
        return "", url


def _html_to_text(html: str) -> str:
    """Basic HTML to plain text conversion."""
    # Remove script and style tags
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
    # Convert common elements
    text = re.sub(r"<h[1-6][^>]*>(.*?)</h[1-6]>", r"\n## \1\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>(.*?)</p>", r"\1\n\n", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<li[^>]*>(.*?)</li>", r"- \1\n", text, flags=re.DOTALL | re.IGNORECASE)
    # Strip remaining tags
    text = re.sub(r"<[^>]+>", "", text)
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _generate_slug(title: str) -> str:
    """Generate a URL-safe slug from a title.

    Args:
        title: The title to slugify.

    Returns:
        Lowercase hyphenated slug, max 60 characters.
    """
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    slug = slug.strip("-")
    if len(slug) > 60:
        slug = slug[:60].rsplit("-", 1)[0]
    return slug or "untitled"
