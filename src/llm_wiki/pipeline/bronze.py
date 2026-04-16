# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Source Ingestion
# MAGIC
# MAGIC Auto Loader streaming table that watches the `incoming/` UC Volume
# MAGIC for new source files and records them in the `sources` table.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Pipeline configuration
catalog = spark.conf.get("wiki.catalog", "llm_wiki")
raw_schema = spark.conf.get("wiki.raw_schema", "raw_sources")
volume_path = f"/Volumes/{catalog}/{raw_schema}/incoming"


@dlt.table(
    name="sources",
    comment="Raw source documents ingested via Auto Loader from UC Volumes",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def sources():
    """Ingest raw source files from the incoming UC Volume.

    Reads text-based files (markdown, text, HTML) using Auto Loader
    with schema inference. Each file becomes a row with its content
    and metadata extracted.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("wholetext", "true")
        .load(volume_path)
        .select(
            F.expr("uuid()").alias("source_id"),
            F.input_file_name().alias("file_path"),
            # Infer content type from file extension
            F.when(F.input_file_name().endswith(".md"), F.lit("article"))
            .when(F.input_file_name().endswith(".txt"), F.lit("note"))
            .when(F.input_file_name().endswith(".html"), F.lit("article"))
            .when(F.input_file_name().endswith(".pdf"), F.lit("paper"))
            .otherwise(F.lit("article"))
            .alias("content_type"),
            F.col("value").alias("raw_text"),
            F.sha2(F.col("value"), 256).alias("content_hash"),
            # Extract basic metadata from filename
            F.map_from_arrays(
                F.array(F.lit("filename"), F.lit("file_size")),
                F.array(
                    F.element_at(F.split(F.input_file_name(), "/"), -1),
                    F.length(F.col("value")).cast(StringType()),
                ),
            ).alias("metadata"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
