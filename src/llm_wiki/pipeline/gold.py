# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Change Detection and Compilation Queue
# MAGIC
# MAGIC Detects new and updated sources by comparing content hashes
# MAGIC against existing compiled pages, and populates the compilation queue.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Pipeline configuration
catalog = spark.conf.get("wiki.catalog", "llm_wiki")
wiki_schema = spark.conf.get("wiki.wiki_schema", "wiki")


@dlt.table(
    name="compilation_queue_pending",
    comment="Sources that need compilation (new or content-changed)",
    table_properties={"quality": "gold"},
    temporary=True,
)
def compilation_queue_pending():
    """Detect sources that need (re)compilation.

    Compares silver_content slugs and content hashes against existing
    compiled pages to find:
    - New sources with no corresponding page
    - Updated sources where the content hash has changed
    """
    silver = dlt.read("silver_content").select(
        "source_id",
        "slug",
        "content_hash",
        "title",
        "content_type",
    )

    # Read existing pages from the wiki schema
    # Use spark.table for cross-schema reads
    try:
        existing_pages = spark.table(f"{catalog}.{wiki_schema}.pages").select(
            F.col("page_id").alias("existing_page_id"),
            F.col("content_hash").alias("existing_hash"),
        )

        # Left anti join: sources with no matching page
        new_sources = silver.join(
            existing_pages,
            silver.slug == existing_pages.existing_page_id,
            "left_anti",
        ).withColumn("trigger_type", F.lit("new_source"))

        # Inner join where hash differs: updated sources
        updated_sources = (
            silver.join(
                existing_pages,
                silver.slug == existing_pages.existing_page_id,
                "inner",
            )
            .where(silver.content_hash != existing_pages.existing_hash)
            .drop("existing_page_id", "existing_hash")
            .withColumn("trigger_type", F.lit("source_updated"))
        )

        return new_sources.union(updated_sources).select(
            F.expr("uuid()").alias("queue_id"),
            F.col("slug").alias("page_id"),
            "trigger_type",
            F.array(F.col("source_id")).alias("trigger_source_ids"),
            F.when(F.col("trigger_type") == "new_source", F.lit(10))
            .otherwise(F.lit(5))
            .alias("priority"),
            F.lit("pending").alias("status"),
            F.current_timestamp().alias("created_at"),
            F.lit(None).cast("timestamp").alias("completed_at"),
            F.lit("").alias("error_message"),
        )

    except Exception:
        # If pages table doesn't exist yet, all sources are new
        return silver.select(
            F.expr("uuid()").alias("queue_id"),
            F.col("slug").alias("page_id"),
            F.lit("new_source").alias("trigger_type"),
            F.array(F.col("source_id")).alias("trigger_source_ids"),
            F.lit(10).alias("priority"),
            F.lit("pending").alias("status"),
            F.current_timestamp().alias("created_at"),
            F.lit(None).cast("timestamp").alias("completed_at"),
            F.lit("").alias("error_message"),
        )


@dlt.table(
    name="source_stats",
    comment="Aggregate statistics about ingested sources",
    table_properties={"quality": "gold"},
)
def source_stats():
    """Compute aggregate statistics for monitoring."""
    silver = dlt.read("silver_content")

    return silver.agg(
        F.count("*").alias("total_sources"),
        F.countDistinct("content_type").alias("distinct_types"),
        F.avg(F.length("clean_content")).alias("avg_content_length"),
        F.min("ingested_at").alias("earliest_ingestion"),
        F.max("ingested_at").alias("latest_ingestion"),
    )
