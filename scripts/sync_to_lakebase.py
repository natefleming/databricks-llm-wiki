#!/usr/bin/env python3
"""Sync wiki pages from Delta to Lakebase with embeddings.

Reads pages from the Delta `pages` table, generates embeddings via the
Databricks Foundation Model API (`databricks-gte-large-en`), and upserts
into the Lakebase `llm_wiki.pages` table (with pgvector column populated).

Idempotent: safe to re-run. Only regenerates embeddings for pages whose
content_hash changed since the last sync.

Usage:
    python scripts/sync_to_lakebase.py [--profile aws-field-eng]
        [--catalog nfleming] [--wiki-schema wiki_nate_fleming]
        [--instance llm-wiki-db] [--database llm_wiki]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole  # noqa (kept for reuse)

# Ensure repo src on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from llm_wiki.storage.lakebase import LakebaseStore
from llm_wiki.storage.delta import DeltaStore


def embed_batch(w: WorkspaceClient, texts: list[str], endpoint: str = "databricks-gte-large-en") -> list[list[float]]:
    """Call embedding endpoint for a batch of texts. Returns list of 1024-dim vectors."""
    # The embedding endpoint accepts 'input' as array of strings
    response = w.serving_endpoints.query(name=endpoint, input=texts)
    embeddings: list[list[float]] = []
    for item in response.data:
        vec = item.embedding if hasattr(item, "embedding") else item.get("embedding") if isinstance(item, dict) else None
        if vec is None:
            raise RuntimeError(f"Unexpected embedding response format: {item}")
        embeddings.append(list(vec))
    return embeddings


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "aws-field-eng"))
    p.add_argument("--catalog", default="nfleming")
    p.add_argument("--wiki-schema", default="wiki_nate_fleming")
    p.add_argument("--raw-schema", default="raw_sources_nate_fleming")
    p.add_argument("--warehouse-id", default="4b9b953939869799")
    p.add_argument("--instance", default="DONOTDELETE-vibe-coding-workshop-lakebase")
    p.add_argument("--database", default="llm_wiki")
    p.add_argument("--batch-size", type=int, default=16)
    p.add_argument("--force", action="store_true", help="re-embed every page regardless of hash")
    args = p.parse_args()

    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile

    # Read pages from Delta
    print(f"[read] Delta pages from {args.catalog}.{args.wiki_schema}.pages ...")
    delta = DeltaStore(
        catalog=args.catalog,
        wiki_schema=args.wiki_schema,
        raw_schema=args.raw_schema,
        warehouse_id=args.warehouse_id,
    )
    pages = delta.list_pages(limit=10000)
    print(f"[read] {len(pages)} pages loaded")

    # Connect to Lakebase
    print(f"[conn] Lakebase {args.instance} / db={args.database} ...")
    lb = LakebaseStore.from_instance(
        instance_name=args.instance,
        database=args.database,
        profile=args.profile,
    )

    # Get existing hashes to skip unchanged pages
    existing_hashes: dict[str, str] = {}
    if not args.force:
        with lb._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT page_id, content_hash FROM pages WHERE embedding IS NOT NULL")
            for row in cur.fetchall():
                existing_hashes[row[0]] = row[1] or ""

    to_sync = [p for p in pages if existing_hashes.get(p.page_id) != p.content_hash]
    print(f"[sync] {len(to_sync)} pages need embedding (skipped {len(pages) - len(to_sync)} unchanged)")

    if not to_sync:
        print("[done] all pages up-to-date")
        lb.close()
        return

    # Generate embeddings in batches
    w = WorkspaceClient(profile=args.profile)
    succeeded = 0
    failed = 0

    for i in range(0, len(to_sync), args.batch_size):
        batch = to_sync[i : i + args.batch_size]
        # Truncate content to a reasonable size for embedding (gte has 8K token context)
        texts = [(p.title + "\n\n" + (p.content_markdown or ""))[:8000] for p in batch]

        try:
            t0 = time.time()
            embeddings = embed_batch(w, texts)
            print(f"[embed] batch {i // args.batch_size + 1}: {len(batch)} pages in {time.time()-t0:.1f}s")

            for page, emb in zip(batch, embeddings):
                try:
                    lb.upsert_page(page, embedding=emb)
                    succeeded += 1
                except Exception as e:
                    print(f"  [fail] {page.page_id}: {e}")
                    failed += 1
        except Exception as e:
            print(f"[fail] batch starting {i}: {e}")
            failed += len(batch)

    # Sync backlinks too (no embeddings needed)
    from llm_wiki.models import BackLink
    print(f"[sync] backlinks ...")
    bl_sql = f"SELECT source_page_id, target_page_id, link_text, context_snippet FROM {args.catalog}.{args.wiki_schema}.backlinks"
    bl_rows = delta._execute(bl_sql)  # type: ignore[attr-defined]
    all_links = [BackLink(source_page_id=r[0] or "", target_page_id=r[1] or "", link_text=r[2] or "", context_snippet=r[3] or "") for r in bl_rows]
    # chunk to avoid very large statements
    for i in range(0, len(all_links), 500):
        lb.upsert_backlinks(all_links[i : i + 500])
    print(f"[sync] {len(all_links)} backlinks upserted")

    print(f"\n[done] succeeded={succeeded} failed={failed}")

    # Verify
    stats = lb.get_stats()
    print(f"Lakebase state: {stats['total_pages']} pages "
          f"({stats.get('pages_with_embeddings', 0)} with embeddings), "
          f"{stats['total_backlinks']} backlinks")

    lb.close()


if __name__ == "__main__":
    main()
