# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Wiki - Sync
# MAGIC
# MAGIC Syncs compiled wiki pages from Delta tables to serving layers:
# MAGIC - **lakebase**: Upsert pages and backlinks to Lakebase/Postgres
# MAGIC - **obsidian**: Export markdown files to UC Volume for Obsidian
# MAGIC - **vector_search**: Trigger Vector Search index resync

# COMMAND ----------

# Parameters
dbutils.widgets.text("target", "lakebase")  # lakebase, obsidian, vector_search, all
dbutils.widgets.text("catalog", "llm_wiki")
dbutils.widgets.text("wiki_schema", "wiki")
dbutils.widgets.text("raw_schema", "raw_sources")
dbutils.widgets.text("lakebase_instance", "llm-wiki-db")

target = dbutils.widgets.get("target")
catalog = dbutils.widgets.get("catalog")
wiki_schema = dbutils.widgets.get("wiki_schema")
raw_schema = dbutils.widgets.get("raw_schema")
lakebase_instance = dbutils.widgets.get("lakebase_instance")

print(f"Sync target: {target}")
print(f"Catalog: {catalog}")
print(f"Wiki schema: {wiki_schema}")

# COMMAND ----------

from llm_wiki.storage.delta import DeltaStore
from llm_wiki.storage.lakebase import LakebaseStore
from llm_wiki.storage.volumes import VolumeStore
from llm_wiki.sync import WikiSync

# Initialize Delta store (always needed)
delta_store = DeltaStore(
    catalog=catalog,
    wiki_schema=wiki_schema,
    raw_schema=raw_schema,
    server_hostname=spark.conf.get("spark.databricks.workspaceUrl", ""),
)

# Initialize optional stores based on target
lakebase_store = None
volume_store = None

if target in ("lakebase", "all"):
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    lb_info = w.lakebase.get_database_instance(lakebase_instance)
    lakebase_store = LakebaseStore(
        host=lb_info.host,
        port=lb_info.port or 5432,
        database="wiki",
    )

if target in ("obsidian", "all"):
    volume_store = VolumeStore(
        catalog=catalog,
        raw_schema=raw_schema,
        wiki_schema=wiki_schema,
    )

# Initialize sync manager
sync = WikiSync(
    delta_store=delta_store,
    lakebase_store=lakebase_store,
    volume_store=volume_store,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Sync

# COMMAND ----------

if target == "all":
    results = sync.sync_all()
    print(f"Sync results: {results}")
elif target == "lakebase":
    count = sync.sync_to_lakebase()
    print(f"Synced {count} pages to Lakebase")
elif target == "obsidian":
    count = sync.sync_to_obsidian()
    print(f"Exported {count} pages to Obsidian volume")
elif target == "vector_search":
    success = sync.trigger_vector_search_sync()
    print(f"Vector Search sync triggered: {success}")
else:
    print(f"Unknown target: {target}")

# COMMAND ----------

# Cleanup
if lakebase_store:
    lakebase_store.close()
print("Sync complete!")
