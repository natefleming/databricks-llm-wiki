# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Wiki - Infrastructure Setup
# MAGIC
# MAGIC Idempotent setup notebook that provisions:
# MAGIC 1. Delta tables (pages, backlinks, compilation_queue, activity_log)
# MAGIC 2. Lakebase instance and tables
# MAGIC 3. Vector Search endpoint and sync index
# MAGIC
# MAGIC Safe to re-run - all operations check for existence first.

# COMMAND ----------

# Parameters (set by DABs job or manually)
dbutils.widgets.text("catalog", "llm_wiki")
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
print(f"Lakebase instance: {lakebase_instance}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Delta Tables

# COMMAND ----------

# Wiki pages table
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
print(f"Created {catalog}.{wiki_schema}.pages")

# COMMAND ----------

# Backlinks table
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
print(f"Created {catalog}.{wiki_schema}.backlinks")

# COMMAND ----------

# Compilation queue table
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
print(f"Created {catalog}.{wiki_schema}.compilation_queue")

# COMMAND ----------

# Activity log table
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
print(f"Created {catalog}.{wiki_schema}.activity_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Provision Lakebase Instance and Tables

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create Lakebase instance (idempotent)
try:
    existing = w.lakebase.get_database_instance(lakebase_instance)
    print(f"Lakebase instance '{lakebase_instance}' already exists")
except Exception:
    print(f"Creating Lakebase instance '{lakebase_instance}'...")
    w.lakebase.create_database_instance(
        name=lakebase_instance,
        capacity="SMALL",
    )
    print(f"Lakebase instance '{lakebase_instance}' created")

# COMMAND ----------

# Create Lakebase tables via SQL
# Note: Lakebase SQL execution requires the instance connection details
import psycopg

lakebase_info = w.lakebase.get_database_instance(lakebase_instance)
lb_host = lakebase_info.host
lb_port = lakebase_info.port or 5432

# Get credentials from the Lakebase instance
conn_str = f"host={lb_host} port={lb_port} dbname=wiki user=admin"

try:
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Enable extensions
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

            # Pages table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pages (
                    page_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    page_type TEXT NOT NULL,
                    content_markdown TEXT NOT NULL DEFAULT '',
                    frontmatter JSONB NOT NULL DEFAULT '{}',
                    confidence TEXT,
                    sources TEXT[],
                    related TEXT[],
                    tags TEXT[],
                    freshness_tier TEXT,
                    created_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ
                )
            """)

            # Full-text search indexes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pages_title_trgm
                ON pages USING gin (title gin_trgm_ops)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pages_content_fts
                ON pages USING gin (to_tsvector('english', content_markdown))
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pages_tags
                ON pages USING gin (tags)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pages_type
                ON pages (page_type)
            """)

            # Backlinks table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS backlinks (
                    source_page_id TEXT REFERENCES pages(page_id) ON DELETE CASCADE,
                    target_page_id TEXT REFERENCES pages(page_id) ON DELETE CASCADE,
                    link_text TEXT,
                    context_snippet TEXT,
                    PRIMARY KEY (source_page_id, target_page_id)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_backlinks_target
                ON backlinks (target_page_id)
            """)

            # Activity log table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS activity_log (
                    log_id TEXT PRIMARY KEY,
                    operation TEXT NOT NULL,
                    details TEXT,
                    page_ids TEXT[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

    print("Lakebase tables created successfully")
except Exception as e:
    print(f"Warning: Could not create Lakebase tables: {e}")
    print("You may need to configure Lakebase credentials manually.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Provision Vector Search Endpoint and Index

# COMMAND ----------

# Create Vector Search endpoint
try:
    existing_endpoints = list(w.vector_search_endpoints.list())
    endpoint_names = [ep.name for ep in existing_endpoints]

    if "llm-wiki-vs-endpoint" not in endpoint_names:
        print("Creating Vector Search endpoint 'llm-wiki-vs-endpoint'...")
        w.vector_search_endpoints.create(
            name="llm-wiki-vs-endpoint",
            endpoint_type="STANDARD",
        )
        print("Vector Search endpoint created")
    else:
        print("Vector Search endpoint 'llm-wiki-vs-endpoint' already exists")
except Exception as e:
    print(f"Warning: Could not create Vector Search endpoint: {e}")

# COMMAND ----------

# Create sync index on pages table
vs_index_name = f"{catalog}.{wiki_schema}.pages_vs_index"
source_table = f"{catalog}.{wiki_schema}.pages"

try:
    existing_indexes = list(
        w.vector_search_indexes.list(endpoint_name="llm-wiki-vs-endpoint")
    )
    index_names = [idx.name for idx in existing_indexes]

    if vs_index_name not in index_names:
        print(f"Creating Vector Search index '{vs_index_name}'...")
        w.vector_search_indexes.create(
            name=vs_index_name,
            endpoint_name="llm-wiki-vs-endpoint",
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
        print(f"Vector Search index '{vs_index_name}' created")
    else:
        print(f"Vector Search index '{vs_index_name}' already exists")
except Exception as e:
    print(f"Warning: Could not create Vector Search index: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC Infrastructure provisioned:
# MAGIC - Delta tables: `pages`, `backlinks`, `compilation_queue`, `activity_log`
# MAGIC - Lakebase instance: `{lakebase_instance}` with `pages`, `backlinks`, `activity_log` tables
# MAGIC - Vector Search: endpoint `llm-wiki-vs-endpoint` + sync index on pages table

print("LLM Wiki infrastructure setup complete!")
