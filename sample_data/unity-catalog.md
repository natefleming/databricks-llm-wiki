# Unity Catalog

Unity Catalog is the unified governance solution for all data and AI assets on Databricks. It provides a single place to manage permissions, lineage, and discovery across tables, volumes, models, and functions.

## Key Components

**Catalogs** are the top-level container in the three-level namespace: `catalog.schema.table`. A catalog typically represents a business domain or environment (dev, staging, prod).

**Schemas** group related tables, views, volumes, and functions within a catalog.

**Volumes** provide managed or external storage for non-tabular data (files, images, documents). Volumes are accessible via `/Volumes/catalog/schema/volume_name/` paths.

**Registered Models** track ML models with versioning and stage transitions.

## Volumes in the LLM Wiki

The [[llm-wiki-pattern]] uses Unity Catalog Volumes for two purposes:

1. **Incoming volume** (`/Volumes/{catalog}/raw_sources/incoming/`) — Drop zone where new source files (PDFs, markdown, text) are uploaded for ingestion by [[spark-declarative-pipelines]].

2. **Obsidian volume** (`/Volumes/{catalog}/wiki/obsidian/`) — Export destination for compiled wiki pages as Obsidian-compatible markdown files. These can be synced to a local [[obsidian-knowledge-management]] vault.

## Governance

Unity Catalog provides:

- **Fine-grained access control** — Grant/revoke permissions on catalogs, schemas, tables, and volumes
- **Data lineage** — Automatic tracking of how data flows between tables
- **Auditing** — Full audit log of all access and changes
- **Discovery** — Search and browse all data assets from a single interface

These capabilities are important for the LLM Wiki at enterprise scale, where multiple teams may contribute sources and consume wiki content.
