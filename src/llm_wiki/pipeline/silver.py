# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Content Extraction and Chunking
# MAGIC
# MAGIC Processes raw sources into clean text with metadata extraction
# MAGIC and chunks them for downstream compilation and embedding.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType


# ──────────────────────────────────────────────
# UDFs for text processing
# ──────────────────────────────────────────────

@F.udf(returnType=StringType())
def extract_title(raw_text: str, file_path: str) -> str:
    """Extract a title from the content or filename.

    Checks for a markdown heading first, then falls back to the filename.
    """
    if not raw_text:
        return file_path.split("/")[-1].rsplit(".", 1)[0] if file_path else "untitled"

    for line in raw_text.split("\n")[:10]:
        line = line.strip()
        if line.startswith("# ") and not line.startswith("##"):
            return line[2:].strip()

    # Fall back to filename
    if file_path:
        name = file_path.split("/")[-1].rsplit(".", 1)[0]
        return name.replace("-", " ").replace("_", " ").title()

    return "Untitled"


@F.udf(returnType=StringType())
def clean_text(raw_text: str) -> str:
    """Clean and normalize raw text content.

    Strips excessive whitespace, normalizes line endings,
    and removes common artifacts.
    """
    if not raw_text:
        return ""

    import re

    text = raw_text

    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Remove excessive blank lines (more than 2 consecutive)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Strip trailing whitespace on each line
    text = "\n".join(line.rstrip() for line in text.split("\n"))

    return text.strip()


@F.udf(returnType=StringType())
def generate_slug(title: str) -> str:
    """Generate a URL-safe slug from a title.

    Lowercases, replaces spaces with hyphens, removes special chars,
    and truncates to 60 characters.
    """
    if not title:
        return "untitled"

    import re

    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    slug = slug.strip("-")

    # Truncate to 60 chars on a word boundary
    if len(slug) > 60:
        slug = slug[:60].rsplit("-", 1)[0]

    return slug or "untitled"


# Chunk schema for explode
chunk_schema = ArrayType(
    StructType([
        StructField("chunk_index", IntegerType()),
        StructField("chunk_text", StringType()),
        StructField("token_count", IntegerType()),
    ])
)


@F.udf(returnType=chunk_schema)
def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> list:
    """Split text into overlapping chunks by estimated token count.

    Args:
        text: The text to chunk.
        chunk_size: Target tokens per chunk.
        overlap: Token overlap between chunks.

    Returns:
        List of dicts with chunk_index, chunk_text, token_count.
    """
    if not text:
        return [{"chunk_index": 0, "chunk_text": "", "token_count": 0}]

    # Estimate tokens as words * 1.3
    words = text.split()
    tokens_per_word = 1.3
    words_per_chunk = int(chunk_size / tokens_per_word)
    words_overlap = int(overlap / tokens_per_word)

    chunks = []
    start = 0
    idx = 0

    while start < len(words):
        end = min(start + words_per_chunk, len(words))
        chunk_words = words[start:end]
        chunk_text = " ".join(chunk_words)
        token_est = int(len(chunk_words) * tokens_per_word)

        chunks.append({
            "chunk_index": idx,
            "chunk_text": chunk_text,
            "token_count": token_est,
        })

        start = end - words_overlap if end < len(words) else end
        idx += 1

    return chunks


# ──────────────────────────────────────────────
# Silver tables
# ──────────────────────────────────────────────

@dlt.table(
    name="silver_content",
    comment="Cleaned and enriched source content with extracted metadata",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("content_not_empty", "clean_content IS NOT NULL AND LENGTH(clean_content) > 0")
def silver_content():
    """Clean raw source text and extract metadata."""
    return (
        dlt.read("sources")
        .withColumn("title", extract_title(F.col("raw_text"), F.col("file_path")))
        .withColumn("slug", generate_slug(F.col("title")))
        .withColumn("clean_content", clean_text(F.col("raw_text")))
        .select(
            "source_id",
            "file_path",
            "content_type",
            "title",
            "slug",
            "clean_content",
            "content_hash",
            "metadata",
            "ingested_at",
        )
    )


@dlt.table(
    name="source_chunks",
    comment="Source content split into overlapping chunks for compilation",
    table_properties={"quality": "silver"},
)
def source_chunks():
    """Chunk silver content into overlapping segments."""
    return (
        dlt.read("silver_content")
        .withColumn("chunks", chunk_text(F.col("clean_content")))
        .select(
            "source_id",
            F.explode("chunks").alias("chunk"),
        )
        .select(
            F.expr("uuid()").alias("chunk_id"),
            "source_id",
            F.col("chunk.chunk_index").alias("chunk_index"),
            F.col("chunk.chunk_text").alias("chunk_text"),
            F.col("chunk.token_count").alias("token_count"),
        )
    )
