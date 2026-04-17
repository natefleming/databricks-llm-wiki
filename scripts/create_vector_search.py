#!/usr/bin/env python3
"""Provision the Vector Search endpoint + delta-sync index for LLM Wiki.

Idempotent: safe to re-run. Creates (if missing):
  1. VS endpoint `llm-wiki-vs-endpoint` (STANDARD), waits until ONLINE
  2. VS index on `<catalog>.<wiki_schema>.pages` embedding `content_markdown`
     with `databricks-gte-large-en`, pipeline_type=TRIGGERED
  3. Triggers an initial sync to embed all existing pages

Usage:
    python scripts/create_vector_search.py [--profile aws-field-eng] \
        [--catalog nfleming] [--schema wiki_nate_fleming]
"""

from __future__ import annotations

import argparse
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)


def ensure_endpoint(w: WorkspaceClient, name: str, timeout_s: int = 900) -> None:
    """Create VS endpoint if missing and wait until ONLINE."""
    try:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=name)
        state = ep.endpoint_status.state if ep.endpoint_status else "UNKNOWN"
        print(f"[endpoint] {name} exists (state={state})")
        if str(state) == "ONLINE":
            return
    except Exception:
        print(f"[endpoint] creating {name} ...")
        w.vector_search_endpoints.create_endpoint(
            name=name,
            endpoint_type=EndpointType.STANDARD,
        )

    print(f"[endpoint] waiting for ONLINE (timeout={timeout_s}s) ...")
    start = time.time()
    while time.time() - start < timeout_s:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=name)
        state = str(ep.endpoint_status.state) if ep.endpoint_status else "UNKNOWN"
        print(f"  state={state} ({int(time.time() - start)}s elapsed)")
        if state == "ONLINE":
            print(f"[endpoint] {name} is ONLINE")
            return
        if state in ("OFFLINE", "FAILED"):
            msg = ep.endpoint_status.message if ep.endpoint_status else ""
            raise RuntimeError(f"endpoint {name} is {state}: {msg}")
        time.sleep(30)
    raise TimeoutError(f"endpoint {name} did not reach ONLINE within {timeout_s}s")


def ensure_index(
    w: WorkspaceClient,
    endpoint: str,
    index_name: str,
    source_table: str,
    embedding_endpoint: str,
) -> None:
    """Create VS index if missing."""
    try:
        idx = w.vector_search_indexes.get_index(index_name=index_name)
        print(f"[index] {index_name} exists (ready={idx.status.ready if idx.status else '?'})")
        return
    except Exception:
        pass

    print(f"[index] creating {index_name} on {source_table} ...")
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=endpoint,
        primary_key="page_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=source_table,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="content_markdown",
                    embedding_model_endpoint_name=embedding_endpoint,
                )
            ],
        ),
    )
    print(f"[index] {index_name} created")


def sync_and_wait(w: WorkspaceClient, index_name: str, timeout_s: int = 600) -> None:
    """Trigger sync and wait until index is ready."""
    print(f"[sync] triggering {index_name} ...")
    try:
        w.vector_search_indexes.sync_index(index_name=index_name)
    except Exception as e:
        # First-time creation may already be syncing; that's fine
        print(f"[sync] warning: {e}")

    print(f"[sync] waiting for ready (timeout={timeout_s}s) ...")
    start = time.time()
    while time.time() - start < timeout_s:
        idx = w.vector_search_indexes.get_index(index_name=index_name)
        ready = idx.status.ready if idx.status else False
        rows = idx.status.indexed_row_count if idx.status else 0
        state = idx.status.detailed_state if idx.status else "?"
        print(f"  ready={ready} indexed_rows={rows} state={state} ({int(time.time() - start)}s)")
        if ready and rows > 0:
            print(f"[sync] {index_name} ready with {rows} indexed rows")
            return
        time.sleep(30)
    raise TimeoutError(f"index {index_name} did not become ready within {timeout_s}s")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "aws-field-eng"))
    p.add_argument("--catalog", default="nfleming")
    p.add_argument("--schema", default="wiki_nate_fleming")
    p.add_argument("--endpoint", default="llm-wiki-vs-endpoint")
    p.add_argument("--embedding-endpoint", default="databricks-gte-large-en")
    p.add_argument("--skip-sync", action="store_true", help="skip triggering the sync")
    args = p.parse_args()

    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile

    w = WorkspaceClient()
    source_table = f"{args.catalog}.{args.schema}.pages"
    index_name = f"{args.catalog}.{args.schema}.pages_vs_index"

    print(f"Profile:   {args.profile}")
    print(f"Endpoint:  {args.endpoint}")
    print(f"Index:     {index_name}")
    print(f"Source:    {source_table}")
    print(f"Embedder:  {args.embedding_endpoint}")
    print()

    ensure_endpoint(w, args.endpoint)
    ensure_index(w, args.endpoint, index_name, source_table, args.embedding_endpoint)

    if not args.skip_sync:
        sync_and_wait(w, index_name)

    print("\nDone.")


if __name__ == "__main__":
    main()
