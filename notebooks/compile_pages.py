# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Wiki - Page Compilation
# MAGIC
# MAGIC Processes pending items from the compilation queue:
# MAGIC 1. Reads pending compilation requests
# MAGIC 2. Assembles context (source chunks + related pages)
# MAGIC 3. Calls FMAPI to compile wiki pages
# MAGIC 4. Writes compiled pages to Delta tables
# MAGIC 5. Extracts wikilinks and updates backlinks

# COMMAND ----------

dbutils.widgets.text("catalog", "nfleming")
dbutils.widgets.text("wiki_schema", "wiki_nate_fleming")
dbutils.widgets.text("raw_schema", "raw_sources_nate_fleming")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5")
dbutils.widgets.text("max_pages", "50")

catalog = dbutils.widgets.get("catalog")
wiki_schema = dbutils.widgets.get("wiki_schema")
raw_schema = dbutils.widgets.get("raw_schema")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
max_pages = int(dbutils.widgets.get("max_pages"))

print(f"Catalog: {catalog}, Wiki: {wiki_schema}, Raw: {raw_schema}")
print(f"LLM endpoint: {llm_endpoint}, Max pages: {max_pages}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Pending Compilations

# COMMAND ----------

pending_df = spark.sql(f"""
    SELECT queue_id, page_id, trigger_type, trigger_source_ids, priority
    FROM {catalog}.{wiki_schema}.compilation_queue
    WHERE status = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT {max_pages}
""")

pending_items = pending_df.collect()
print(f"Found {len(pending_items)} pending compilations")

if not pending_items:
    dbutils.notebook.exit("No pending compilations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compile Pages

# COMMAND ----------

import re
import hashlib
from datetime import datetime, timezone

def compile_page(page_id, source_ids, llm_endpoint):
    """Compile a single wiki page from source chunks using FMAPI."""
    # Gather source content
    source_text_parts = []
    for sid in source_ids:
        chunks = spark.sql(f"""
            SELECT chunk_text FROM {catalog}.{raw_schema}.source_chunks
            WHERE source_id = '{sid}' ORDER BY chunk_index
        """).collect()
        for c in chunks:
            source_text_parts.append(c.chunk_text)

    source_text = "\n\n".join(source_text_parts) if source_text_parts else ""

    # Get title from silver content
    title_row = spark.sql(f"""
        SELECT title FROM {catalog}.{raw_schema}.silver_content
        WHERE slug = '{page_id}' LIMIT 1
    """).collect()
    title = title_row[0].title if title_row else page_id.replace("-", " ").title()

    # Build prompt
    messages = [
        {"role": "system", "content": """You are a wiki compiler. Synthesize raw source material into well-structured wiki pages.
Rules:
1. Write in clear, encyclopedic prose. Be factual and precise.
2. Use [[slug-format]] wikilinks to reference other concepts.
3. Include a ## Sources section at the end.
4. Do NOT include YAML frontmatter.
5. Start with a one-paragraph summary, then expand into sections."""},
        {"role": "user", "content": f"""Compile a wiki page about: {title}

Source material:
{source_text[:8000]}

Write a comprehensive wiki page with cross-references using [[slug]] wikilinks."""}
    ]

    # Call FMAPI via REST API (avoids SDK serialization issues)
    import requests
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    api_url = f"https://{workspace_url}/serving-endpoints/{llm_endpoint}/invocations"
    payload = {
        "messages": messages,
        "max_tokens": 4096,
        "temperature": 0.3,
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(api_url, json=payload, headers=headers, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    content = ""
    choices = data.get("choices", [])
    if choices:
        content = choices[0].get("message", {}).get("content", "")

    return title, content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Queue

# COMMAND ----------

results = {"succeeded": [], "failed": []}

for item in pending_items:
    page_id = item.page_id
    source_ids = item.trigger_source_ids or []

    print(f"Compiling: {page_id}...")

    # Mark as in_progress
    spark.sql(f"""
        UPDATE {catalog}.{wiki_schema}.compilation_queue
        SET status = 'in_progress'
        WHERE queue_id = '{item.queue_id}'
    """)

    try:
        title, content = compile_page(page_id, source_ids, llm_endpoint)
        now = datetime.now(timezone.utc).isoformat()
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

        # Extract wikilinks
        wikilinks = list(set(re.findall(r"\[\[([a-z0-9][a-z0-9-]*)\]\]", content)))

        # Extract tags from bold terms
        tags = list(set(t.lower().strip().replace(" ", "-")
                       for t in re.findall(r"\*\*([a-zA-Z][a-zA-Z\s-]{2,30})\*\*", content)[:10]))

        # Determine confidence
        confidence = "medium" if len(source_ids) >= 1 else "low"
        if len(source_ids) >= 2:
            confidence = "high"

        # Escape content for SQL
        safe_content = content.replace("'", "''")
        safe_title = title.replace("'", "''")
        sources_arr = "ARRAY(" + ",".join(f"'{s}'" for s in source_ids) + ")" if source_ids else "ARRAY()"
        related_arr = "ARRAY(" + ",".join(f"'{r}'" for r in wikilinks) + ")" if wikilinks else "ARRAY()"
        tags_arr = "ARRAY(" + ",".join(f"'{t}'" for t in tags) + ")" if tags else "ARRAY()"

        # Upsert page
        spark.sql(f"""
            MERGE INTO {catalog}.{wiki_schema}.pages AS target
            USING (SELECT '{page_id}' AS page_id) AS source
            ON target.page_id = source.page_id
            WHEN MATCHED THEN UPDATE SET
                title = '{safe_title}', content_markdown = '{safe_content}',
                confidence = '{confidence}', sources = {sources_arr},
                related = {related_arr}, tags = {tags_arr},
                freshness_tier = 'monthly', content_hash = '{content_hash}',
                updated_at = '{now}', compiled_by = '{llm_endpoint}'
            WHEN NOT MATCHED THEN INSERT (
                page_id, title, page_type, content_markdown, frontmatter,
                confidence, sources, related, tags, freshness_tier,
                content_hash, created_at, updated_at, compiled_by
            ) VALUES (
                '{page_id}', '{safe_title}', 'concept', '{safe_content}', '',
                '{confidence}', {sources_arr}, {related_arr}, {tags_arr}, 'monthly',
                '{content_hash}', '{now}', '{now}', '{llm_endpoint}'
            )
        """)

        # Write backlinks
        for target_slug in wikilinks:
            safe_link = target_slug.replace("'", "''")
            spark.sql(f"""
                MERGE INTO {catalog}.{wiki_schema}.backlinks AS target
                USING (SELECT '{page_id}' AS src, '{safe_link}' AS tgt) AS source
                ON target.source_page_id = source.src AND target.target_page_id = source.tgt
                WHEN NOT MATCHED THEN INSERT (source_page_id, target_page_id, link_text)
                VALUES (source.src, source.tgt, source.tgt)
            """)

        # Mark completed
        spark.sql(f"""
            UPDATE {catalog}.{wiki_schema}.compilation_queue
            SET status = 'completed', completed_at = '{now}'
            WHERE queue_id = '{item.queue_id}'
        """)

        results["succeeded"].append(page_id)
        print(f"  OK: {title} ({len(content)} chars, {len(wikilinks)} links)")

    except Exception as e:
        error_msg = str(e)[:500].replace("'", "''")
        spark.sql(f"""
            UPDATE {catalog}.{wiki_schema}.compilation_queue
            SET status = 'failed', error_message = '{error_msg}'
            WHERE queue_id = '{item.queue_id}'
        """)
        results["failed"].append((page_id, str(e)[:100]))
        print(f"  FAILED: {page_id}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

print(f"\n=== Compilation Results ===")
print(f"  Succeeded: {len(results['succeeded'])}")
print(f"  Failed:    {len(results['failed'])}")

if results["succeeded"]:
    print(f"\nCompiled pages:")
    for p in results["succeeded"]:
        print(f"  - {p}")

if results["failed"]:
    print(f"\nFailed:")
    for p, e in results["failed"]:
        print(f"  - {p}: {e}")

# Verify
page_count = spark.sql(f"SELECT count(*) FROM {catalog}.{wiki_schema}.pages").first()[0]
link_count = spark.sql(f"SELECT count(*) FROM {catalog}.{wiki_schema}.backlinks").first()[0]
pending_count = spark.sql(f"SELECT count(*) FROM {catalog}.{wiki_schema}.compilation_queue WHERE status = 'pending'").first()[0]

print(f"\nWiki status: {page_count} pages, {link_count} backlinks, {pending_count} pending")
