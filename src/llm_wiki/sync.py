"""Sync operations: Delta -> Lakebase, Delta -> UC Volumes (Obsidian), Vector Search.

Handles materialization of Delta table data into serving layers:
- Lakebase for fast MCP/web UI serving
- UC Volumes for Obsidian-compatible markdown files
- Vector Search index triggering

Usage:
    from llm_wiki.sync import WikiSync

    sync = WikiSync(delta_store, lakebase_store, volume_store)
    sync.sync_to_lakebase()
    sync.sync_to_obsidian()
    sync.trigger_vector_search_sync()
"""

from __future__ import annotations

from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

from llm_wiki.log import logger
from llm_wiki.storage.delta import DeltaStore
from llm_wiki.storage.lakebase import LakebaseStore
from llm_wiki.storage.volumes import VolumeStore


class WikiSync:
    """Synchronizes wiki data from Delta tables to serving layers."""

    def __init__(
        self,
        delta_store: DeltaStore,
        lakebase_store: LakebaseStore | None = None,
        volume_store: VolumeStore | None = None,
        vs_index_name: str = "llm_wiki.wiki.pages_vs_index",
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the sync manager.

        Args:
            delta_store: Source Delta table store.
            lakebase_store: Target Lakebase store (optional).
            volume_store: Target Volume store for Obsidian (optional).
            vs_index_name: Vector Search index name.
            client: Optional pre-configured WorkspaceClient.
        """
        self._delta = delta_store
        self._lakebase = lakebase_store
        self._volumes = volume_store
        self._vs_index = vs_index_name
        self._client = client or WorkspaceClient()

    def sync_to_lakebase(self) -> int:
        """Sync all pages from Delta to Lakebase.

        Performs a full upsert of all pages and their backlinks.

        Returns:
            Number of pages synced.
        """
        if not self._lakebase:
            logger.warning("Lakebase store not configured, skipping sync")
            return 0

        pages = self._delta.list_pages(limit=10000)
        logger.info("Syncing pages to Lakebase", count=len(pages))

        for page in pages:
            self._lakebase.upsert_page(page)

        # Sync backlinks for each page
        for page in pages:
            backlinks = self._delta.get_backlinks(page.page_id)
            # Also get outgoing links
            for wikilink in page.wikilinks:
                from llm_wiki.models import BackLink
                backlinks.append(BackLink(
                    source_page_id=page.page_id,
                    target_page_id=wikilink,
                    link_text=wikilink,
                ))
            if backlinks:
                self._lakebase.upsert_backlinks(backlinks)

        logger.info("Lakebase sync complete", pages=len(pages))
        return len(pages)

    def sync_to_obsidian(self) -> int:
        """Sync all pages from Delta to UC Volumes as Obsidian markdown.

        Exports each page as a .md file with YAML frontmatter and
        generates an index.md.

        Returns:
            Number of pages exported.
        """
        if not self._volumes:
            logger.warning("Volume store not configured, skipping Obsidian sync")
            return 0

        pages = self._delta.list_pages(limit=10000)
        logger.info("Syncing pages to Obsidian volume", count=len(pages))

        paths = self._volumes.export_all_pages(pages)

        logger.info("Obsidian sync complete", files=len(paths))
        return len(pages)

    def trigger_vector_search_sync(self) -> bool:
        """Trigger the Vector Search delta sync index to re-sync.

        Returns:
            True if sync was triggered successfully.
        """
        try:
            self._client.vector_search_indexes.sync(name=self._vs_index)
            logger.info("Vector Search sync triggered", index=self._vs_index)
            return True
        except Exception as e:
            logger.warning("Vector Search sync failed", error=str(e))
            return False

    def sync_all(self) -> dict[str, int | bool]:
        """Run all sync operations.

        Returns:
            Dictionary with results from each sync target.
        """
        results: dict[str, int | bool] = {}
        results["lakebase_pages"] = self.sync_to_lakebase()
        results["obsidian_pages"] = self.sync_to_obsidian()
        results["vector_search"] = self.trigger_vector_search_sync()

        logger.info("Full sync complete", results=results)
        return results
