# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Wiki - Page Compilation
# MAGIC
# MAGIC Processes pending items from the compilation queue:
# MAGIC 1. Reads pending compilation requests
# MAGIC 2. Assembles context (source chunks + related pages)
# MAGIC 3. Calls FMAPI to compile wiki pages
# MAGIC 4. Writes compiled pages to Delta tables
# MAGIC 5. Updates backlinks

# COMMAND ----------

# Parameters
dbutils.widgets.text("catalog", "llm_wiki")
dbutils.widgets.text("wiki_schema", "wiki")
dbutils.widgets.text("raw_schema", "raw_sources")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4")
dbutils.widgets.text("max_pages", "50")

catalog = dbutils.widgets.get("catalog")
wiki_schema = dbutils.widgets.get("wiki_schema")
raw_schema = dbutils.widgets.get("raw_schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
max_pages = int(dbutils.widgets.get("max_pages"))

print(f"Catalog: {catalog}")
print(f"Wiki schema: {wiki_schema}")
print(f"Raw schema: {raw_schema}")
print(f"LLM endpoint: {llm_endpoint}")
print(f"Max pages: {max_pages}")

# COMMAND ----------

from llm_wiki.config import load_config
from llm_wiki.compiler.engine import WikiCompiler
from llm_wiki.storage.delta import DeltaStore

# Load configuration
config = load_config()
# Override with job parameters
config.wiki.catalog = catalog
config.wiki.default_model = llm_endpoint

# Initialize storage
store = DeltaStore(
    catalog=catalog,
    wiki_schema=wiki_schema,
    raw_schema=raw_schema,
    server_hostname=spark.conf.get("spark.databricks.workspaceUrl", ""),
)

# Initialize compiler
compiler = WikiCompiler(store=store, config=config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Compilation

# COMMAND ----------

results = compiler.compile_pending(limit=max_pages)

# Summary
succeeded = [r for r in results if r.success]
failed = [r for r in results if not r.success]

print(f"\nCompilation Results:")
print(f"  Succeeded: {len(succeeded)}")
print(f"  Failed: {len(failed)}")
print(f"  Total tokens: {sum(r.tokens_used for r in results)}")

if succeeded:
    print(f"\nCompiled pages:")
    for r in succeeded:
        print(f"  - {r.page.page_id}: {r.page.title}")

if failed:
    print(f"\nFailed compilations:")
    for r in failed:
        print(f"  - {r.queue_item.page_id}: {r.error[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

page_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{wiki_schema}.pages").first().cnt
backlink_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{wiki_schema}.backlinks").first().cnt
pending_count = spark.sql(
    f"SELECT COUNT(*) as cnt FROM {catalog}.{wiki_schema}.compilation_queue WHERE status = 'pending'"
).first().cnt

print(f"Wiki status:")
print(f"  Total pages: {page_count}")
print(f"  Total backlinks: {backlink_count}")
print(f"  Remaining pending: {pending_count}")
