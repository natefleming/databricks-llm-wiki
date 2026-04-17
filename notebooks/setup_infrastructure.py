# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Wiki - Infrastructure Setup
# MAGIC
# MAGIC Idempotent setup notebook that provisions:
# MAGIC 1. Catalog and schemas (if not created by DABs)
# MAGIC 2. Delta tables (pages, backlinks, compilation_queue, activity_log)
# MAGIC 3. Lakebase instance and tables (optional - skipped if unavailable)
# MAGIC 4. Vector Search endpoint and sync index
# MAGIC
# MAGIC Safe to re-run - all operations check for existence first.

# COMMAND ----------

dbutils.widgets.text("catalog", "nfleming")
dbutils.widgets.text("wiki_schema", "wiki")
dbutils.widgets.text("raw_schema", "raw_sources")
dbutils.widgets.text("lakebase_instance", "llm-wiki-db")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

catalog = dbutils.widgets.get("catalog")
wiki_schema = dbutils.widgets.get("wiki_schema")
raw_schema = dbutils.widgets.get("raw_schema")
lakebase_instance = dbutils.widgets.get("lakebase_instance")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")

print(f"Catalog: {catalog}")
print(f"Wiki schema: {wiki_schema}")
print(f"Raw schema: {raw_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ensure Catalog and Schemas Exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{wiki_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{raw_schema}")

# Create volumes (idempotent)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{raw_schema}.incoming
    COMMENT 'Drop zone for new source files'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{wiki_schema}.obsidian
    COMMENT 'Markdown files for Obsidian sync'
""")

print(f"Catalog, schemas, and volumes ready:")
print(f"  {catalog}.{wiki_schema} (+ obsidian volume)")
print(f"  {catalog}.{raw_schema} (+ incoming volume)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Delta Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{wiki_schema}.pages (
        page_id STRING NOT NULL,
        title STRING NOT NULL,
        page_type STRING NOT NULL,
        content_markdown STRING,
        frontmatter STRING,
        confidence STRING,
        sources ARRAY<STRING>,
        related ARRAY<STRING>,
        tags ARRAY<STRING>,
        freshness_tier STRING,
        content_hash STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        compiled_by STRING
    )
    USING DELTA
    COMMENT 'LLM Wiki compiled pages'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print(f"Table ready: {catalog}.{wiki_schema}.pages")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{wiki_schema}.backlinks (
        source_page_id STRING NOT NULL,
        target_page_id STRING NOT NULL,
        link_text STRING,
        context_snippet STRING
    )
    USING DELTA
    COMMENT 'Wiki page cross-reference links'
""")
print(f"Table ready: {catalog}.{wiki_schema}.backlinks")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{wiki_schema}.compilation_queue (
        queue_id STRING NOT NULL,
        page_id STRING NOT NULL,
        trigger_type STRING NOT NULL,
        trigger_source_ids ARRAY<STRING>,
        priority INT,
        status STRING NOT NULL,
        created_at TIMESTAMP,
        completed_at TIMESTAMP,
        error_message STRING
    )
    USING DELTA
    COMMENT 'Queue of pages pending LLM compilation'
""")
print(f"Table ready: {catalog}.{wiki_schema}.compilation_queue")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{wiki_schema}.activity_log (
        log_id STRING NOT NULL,
        operation STRING NOT NULL,
        details STRING,
        page_ids ARRAY<STRING>,
        timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Append-only log of wiki operations'
""")
print(f"Table ready: {catalog}.{wiki_schema}.activity_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lakebase (Optional)
# MAGIC
# MAGIC Skipped if Lakebase is not available on this workspace.

# COMMAND ----------

lakebase_available = False
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    existing = w.lakebase.get_database_instance(lakebase_instance)
    print(f"Lakebase instance '{lakebase_instance}' exists")
    lakebase_available = True
except AttributeError:
    print("SKIP: Lakebase API not available on this workspace")
except Exception as e:
    if "not found" in str(e).lower() or "404" in str(e):
        print("SKIP: Lakebase API not available on this workspace")
    else:
        print(f"SKIP: Lakebase error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vector Search Endpoint and Index

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

vs_endpoint_name = "llm-wiki-vs-endpoint"

# Note: endpoint creation is async - reaching ONLINE takes 5-10 min.
# Use scripts/create_vector_search.py for a provisioning workflow that waits.
existing_endpoints = list(w.vector_search_endpoints.list_endpoints())
endpoint_names = [ep.name for ep in existing_endpoints]

if vs_endpoint_name not in endpoint_names:
    print(f"Creating Vector Search endpoint '{vs_endpoint_name}'...")
    w.vector_search_endpoints.create_endpoint(
        name=vs_endpoint_name,
        endpoint_type="STANDARD",
    )
    print(f"Vector Search endpoint created (may take a few minutes to become ONLINE)")
else:
    print(f"Vector Search endpoint '{vs_endpoint_name}' already exists")

# COMMAND ----------

vs_index_name = f"{catalog}.{wiki_schema}.pages_vs_index"
source_table = f"{catalog}.{wiki_schema}.pages"

try:
    existing_indexes = list(w.vector_search_indexes.list_indexes(endpoint_name=vs_endpoint_name))
    index_names = [idx.name for idx in existing_indexes]

    if vs_index_name not in index_names:
        print(f"Creating Vector Search index '{vs_index_name}'...")
        w.vector_search_indexes.create_index(
            name=vs_index_name,
            endpoint_name=vs_endpoint_name,
            primary_key="page_id",
            delta_sync_index_spec={
                "source_table": source_table,
                "embedding_source_columns": [
                    {
                        "name": "content_markdown",
                        "embedding_model_endpoint_name": embedding_endpoint,
                    }
                ],
                "pipeline_type": "TRIGGERED",
            },
        )
        print(f"Vector Search index created")
    else:
        print(f"Vector Search index '{vs_index_name}' already exists")
except Exception as e:
    print(f"Warning: Could not create Vector Search index: {e}")
    print("This is expected if the pages table is empty. Re-run after first compilation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete

# COMMAND ----------

print("\n=== LLM Wiki Infrastructure Setup Complete ===")
print(f"  Catalog:         {catalog}")
print(f"  Wiki schema:     {catalog}.{wiki_schema}")
print(f"  Raw schema:      {catalog}.{raw_schema}")
print(f"  Delta tables:    pages, backlinks, compilation_queue, activity_log")
print(f"  Lakebase:        {'configured' if lakebase_available else 'not available (using Delta fallback)'}")
print(f"  Vector Search:   endpoint={vs_endpoint_name}")
