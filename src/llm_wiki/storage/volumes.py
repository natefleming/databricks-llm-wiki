"""Unity Catalog Volume operations for LLM Wiki.

Handles uploading raw sources to the incoming volume and
exporting compiled pages as Obsidian-compatible markdown files.

Usage:
    from llm_wiki.storage.volumes import VolumeStore

    store = VolumeStore(catalog="llm_wiki")
    store.upload_source("article.md", content)
    store.export_page_markdown(page)
"""

from __future__ import annotations

import io
from pathlib import Path

from databricks.sdk import WorkspaceClient

from llm_wiki.log import logger
from llm_wiki.models import Page


class VolumeStore:
    """Unity Catalog Volume operations for raw sources and Obsidian sync."""

    def __init__(
        self,
        catalog: str = "llm_wiki",
        raw_schema: str = "raw_sources",
        wiki_schema: str = "wiki",
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the Volume store.

        Args:
            catalog: Unity Catalog catalog name.
            raw_schema: Schema containing raw source volumes.
            wiki_schema: Schema containing wiki volumes.
            client: Optional pre-configured WorkspaceClient.
        """
        self.catalog = catalog
        self.raw_schema = raw_schema
        self.wiki_schema = wiki_schema
        self._client = client or WorkspaceClient()

    @property
    def incoming_volume_path(self) -> str:
        """Path to the incoming sources volume."""
        return f"/Volumes/{self.catalog}/{self.raw_schema}/incoming"

    @property
    def obsidian_volume_path(self) -> str:
        """Path to the Obsidian sync volume."""
        return f"/Volumes/{self.catalog}/{self.wiki_schema}/obsidian"

    # ──────────────────────────────────────────────
    # Raw source upload
    # ──────────────────────────────────────────────

    def upload_source(self, filename: str, content: str | bytes) -> str:
        """Upload a source file to the incoming volume.

        Args:
            filename: Name of the file to create.
            content: File content (str or bytes).

        Returns:
            Full volume path of the uploaded file.
        """
        path = f"{self.incoming_volume_path}/{filename}"
        data = content.encode("utf-8") if isinstance(content, str) else content

        self._client.files.upload(path, io.BytesIO(data), overwrite=True)
        logger.info("Uploaded source", path=path, size=len(data))
        return path

    def upload_source_from_file(self, local_path: str | Path) -> str:
        """Upload a local file to the incoming volume.

        Args:
            local_path: Path to the local file.

        Returns:
            Full volume path of the uploaded file.
        """
        local_path = Path(local_path)
        content = local_path.read_bytes()
        return self.upload_source(local_path.name, content)

    def list_incoming_sources(self) -> list[str]:
        """List files in the incoming volume.

        Returns:
            List of file paths.
        """
        try:
            entries = self._client.files.list_directory_contents(self.incoming_volume_path)
            return [entry.path for entry in entries if entry.path]
        except Exception:
            logger.warning("Could not list incoming volume", path=self.incoming_volume_path)
            return []

    # ──────────────────────────────────────────────
    # Obsidian sync
    # ──────────────────────────────────────────────

    def export_page_markdown(self, page: Page) -> str:
        """Export a single page as an Obsidian-compatible markdown file.

        Creates a .md file with YAML frontmatter and [[wikilinks]] in the
        obsidian volume.

        Args:
            page: The Page to export.

        Returns:
            Full volume path of the exported file.
        """
        markdown = self._render_obsidian_markdown(page)
        path = f"{self.obsidian_volume_path}/{page.page_id}.md"

        self._client.files.upload(path, io.BytesIO(markdown.encode("utf-8")), overwrite=True)
        logger.debug("Exported page to Obsidian", page_id=page.page_id)
        return path

    def export_all_pages(self, pages: list[Page]) -> list[str]:
        """Export all pages to the Obsidian volume.

        Also generates an index.md listing all pages by category.

        Args:
            pages: List of Pages to export.

        Returns:
            List of exported file paths.
        """
        paths: list[str] = []

        for page in pages:
            path = self.export_page_markdown(page)
            paths.append(path)

        # Generate index.md
        index_md = self._render_index(pages)
        index_path = f"{self.obsidian_volume_path}/index.md"
        self._client.files.upload(index_path, io.BytesIO(index_md.encode("utf-8")), overwrite=True)
        paths.append(index_path)

        logger.info("Exported pages to Obsidian", count=len(pages))
        return paths

    # ──────────────────────────────────────────────
    # Rendering helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _render_obsidian_markdown(page: Page) -> str:
        """Render a page as Obsidian-compatible markdown with YAML frontmatter.

        Args:
            page: The Page to render.

        Returns:
            Markdown string with YAML frontmatter.
        """
        # Build frontmatter
        fm_lines = [
            "---",
            f'title: "{page.title}"',
            f"type: {page.page_type.value}",
            f"confidence: {page.confidence.value}",
        ]

        if page.sources:
            fm_lines.append("sources:")
            for s in page.sources:
                fm_lines.append(f"  - {s}")

        if page.related:
            fm_lines.append("related:")
            for r in page.related:
                fm_lines.append(f"  - {r}")

        if page.tags:
            fm_lines.append("tags:")
            for t in page.tags:
                fm_lines.append(f"  - {t}")

        fm_lines.append(f"freshness_tier: {page.freshness_tier.value}")

        if page.created_at:
            fm_lines.append(f"created: {page.created_at.strftime('%Y-%m-%d')}")
        if page.updated_at:
            fm_lines.append(f"updated: {page.updated_at.strftime('%Y-%m-%d')}")

        fm_lines.append("---")
        fm_lines.append("")

        return "\n".join(fm_lines) + page.content_markdown

    @staticmethod
    def _render_index(pages: list[Page]) -> str:
        """Render an index.md listing all pages by category.

        Args:
            pages: List of all wiki pages.

        Returns:
            Markdown string for index.md.
        """
        lines = ["# Wiki Index", "", f"*{len(pages)} pages*", ""]

        # Group by type
        by_type: dict[str, list[Page]] = {}
        for p in pages:
            by_type.setdefault(p.page_type.value, []).append(p)

        for ptype in sorted(by_type.keys()):
            lines.append(f"## {ptype.title()}")
            lines.append("")
            for p in sorted(by_type[ptype], key=lambda x: x.title):
                confidence_badge = f"({p.confidence.value})" if p.confidence else ""
                lines.append(f"- [[{p.page_id}|{p.title}]] {confidence_badge}")
            lines.append("")

        return "\n".join(lines)
