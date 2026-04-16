# Spark Declarative Pipelines (SDP)

Spark Declarative Pipelines (SDP), formerly known as Delta Live Tables (DLT), is a framework for building reliable, maintainable data pipelines on Databricks.

## Core Concepts

SDP uses a declarative approach where you define **what** your data should look like rather than **how** to compute it. The framework handles:

- **Dependency resolution** — Tables are defined as transformations of other tables, and the system automatically determines execution order.
- **Data quality** — Expectations (`@dlt.expect`) enforce data quality constraints at the table level.
- **Incremental processing** — Streaming tables use Auto Loader for efficient incremental ingestion.

## Table Types

- **Streaming tables** — Append-only tables that process new data incrementally. Ideal for ingestion from files or message queues.
- **Materialized views** — Tables recomputed from their source query on each pipeline update. Good for aggregations and transformations.

## Pipeline Configuration

Pipelines are configured in YAML when using Databricks Asset Bundles:

```yaml
resources:
  pipelines:
    my_pipeline:
      name: my-pipeline
      catalog: my_catalog
      target: my_schema
      serverless: true
      libraries:
        - notebook:
            path: ./pipeline/bronze.py
```

## Role in the LLM Wiki

The [[llm-wiki-pattern]] uses SDP for the deterministic ETL portions of the ingestion pipeline:

- **Bronze** — Auto Loader streaming table ingests raw source files from [[unity-catalog]] Volumes
- **Silver** — Materialized views extract text, chunk content, and extract metadata
- **Gold** — Change detection compares source hashes against compiled pages to populate the compilation queue

LLM compilation (which is non-deterministic and requires API calls to the [[foundation-model-api]]) runs in a separate Databricks Job rather than within the SDP pipeline.
