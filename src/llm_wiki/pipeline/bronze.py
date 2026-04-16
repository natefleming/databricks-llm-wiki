# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Source Ingestion
# MAGIC
# MAGIC Auto Loader streaming table that watches the `incoming/` UC Volume
# MAGIC for new source files. Reads files as binary to support all formats
# MAGIC (PDF, DOCX, PPTX, images, text, markdown).

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

catalog = spark.conf.get("wiki.catalog", "nfleming")
raw_schema = spark.conf.get("wiki.raw_schema", "raw_sources")
volume_path = f"/Volumes/{catalog}/{raw_schema}/incoming"

# COMMAND ----------

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

    Reads all file types as binary using Auto Loader. Each file becomes
    a row with its binary content and metadata for downstream parsing.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(volume_path)
        .select(
            F.expr("uuid()").alias("source_id"),
            F.col("path").alias("file_path"),
            # Infer content type from file extension
            F.when(F.col("path").endswith(".md"), F.lit("article"))
            .when(F.col("path").endswith(".txt"), F.lit("note"))
            .when(F.col("path").endswith(".html"), F.lit("article"))
            .when(F.col("path").endswith(".pdf"), F.lit("paper"))
            .when(F.col("path").endswith(".docx"), F.lit("article"))
            .when(F.col("path").endswith(".pptx"), F.lit("presentation"))
            .when(F.col("path").rlike("\\.(jpg|jpeg|png)$"), F.lit("image"))
            .otherwise(F.lit("article"))
            .alias("content_type"),
            F.col("content").alias("raw_bytes"),
            F.col("length").alias("file_size"),
            F.sha2(F.col("content"), 256).alias("content_hash"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
