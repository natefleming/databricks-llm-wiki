# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Wiki - Sync
# MAGIC
# MAGIC Syncs compiled wiki pages from Delta tables to serving layers:
# MAGIC - **lakebase**: Upsert pages to Lakebase/Postgres (skipped if unavailable)
# MAGIC - **obsidian**: Export markdown files to UC Volume for Obsidian
# MAGIC - **vector_search**: Trigger Vector Search index resync

# COMMAND ----------

dbutils.widgets.text("target", "obsidian")
dbutils.widgets.text("catalog", "nfleming")
dbutils.widgets.text("wiki_schema", "wiki_nate_fleming")
dbutils.widgets.text("lakebase_instance", "llm-wiki-db")

target = dbutils.widgets.get("target")
catalog = dbutils.widgets.get("catalog")
wiki_schema = dbutils.widgets.get("wiki_schema")
lakebase_instance = dbutils.widgets.get("lakebase_instance")

print(f"Sync target: {target}")
print(f"Source: {catalog}.{wiki_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Pages from Delta

# COMMAND ----------

pages_df = spark.sql(f"""
    SELECT page_id, title, page_type, content_markdown, confidence,
           sources, related, tags, freshness_tier, created_at, updated_at
    FROM {catalog}.{wiki_schema}.pages
""")
pages = pages_df.collect()
print(f"Loaded {len(pages)} pages from Delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync to Obsidian Volume

# COMMAND ----------

if target in ("obsidian", "all"):
    import yaml

    obsidian_path = f"/Volumes/{catalog}/{wiki_schema}/obsidian"

    for page in pages:
        # Build YAML frontmatter
        fm = {
            "title": page.title,
            "type": page.page_type or "concept",
            "confidence": page.confidence or "low",
        }
        if page.sources:
            fm["sources"] = list(page.sources)
        if page.related:
            fm["related"] = list(page.related)
        if page.tags:
            fm["tags"] = list(page.tags)
        fm["freshness_tier"] = page.freshness_tier or "monthly"
        if page.created_at:
            fm["created"] = page.created_at.strftime("%Y-%m-%d")
        if page.updated_at:
            fm["updated"] = page.updated_at.strftime("%Y-%m-%d")

        fm_str = yaml.dump(fm, default_flow_style=False, sort_keys=False, allow_unicode=True)
        md_content = f"---\n{fm_str}---\n\n{page.content_markdown or ''}"

        # Write to volume
        file_path = f"{obsidian_path}/{page.page_id}.md"
        dbutils.fs.put(file_path, md_content, overwrite=True)

    # Generate index.md
    index_lines = [f"# Wiki Index\n\n*{len(pages)} pages*\n"]
    by_type = {}
    for p in pages:
        by_type.setdefault(p.page_type or "other", []).append(p)

    for ptype in sorted(by_type.keys()):
        index_lines.append(f"## {ptype.title()}\n")
        for p in sorted(by_type[ptype], key=lambda x: x.title):
            index_lines.append(f"- [[{p.page_id}|{p.title}]] ({p.confidence})")
        index_lines.append("")

    dbutils.fs.put(f"{obsidian_path}/index.md", "\n".join(index_lines), overwrite=True)
    print(f"Exported {len(pages)} pages + index.md to {obsidian_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync to Lakebase (Optional)

# COMMAND ----------

if target in ("lakebase", "all"):
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        lb_info = w.lakebase.get_database_instance(lakebase_instance)
        print(f"Lakebase available at {lb_info.host}")
        # TODO: implement psycopg upsert when Lakebase is available
    except Exception as e:
        print(f"SKIP: Lakebase not available ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Vector Search Sync

# COMMAND ----------

if target in ("vector_search", "all"):
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        vs_index = f"{catalog}.{wiki_schema}.pages_vs_index"
        w.vector_search_indexes.sync_index(index_name=vs_index)
        print(f"Vector Search sync triggered for {vs_index}")
    except Exception as e:
        print(f"SKIP: Vector Search sync failed ({e})")

# COMMAND ----------

print(f"Sync complete for target: {target}")
