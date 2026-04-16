# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Content Extraction and Chunking
# MAGIC
# MAGIC Uses `ai_parse_document` for structured document parsing (PDF, DOCX, PPTX, images)
# MAGIC and text extraction for plain text/markdown files. Chunks content for downstream
# MAGIC compilation and embedding.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text Processing UDFs

# COMMAND ----------

@F.udf(returnType=StringType())
def extract_title(text: str, file_path: str) -> str:
    """Extract a title from content or filename."""
    if text:
        for line in text.split("\n")[:10]:
            line = line.strip()
            if line.startswith("# ") and not line.startswith("##"):
                return line[2:].strip()

    if file_path:
        import re
        name = file_path.split("/")[-1].rsplit(".", 1)[0]
        return name.replace("-", " ").replace("_", " ").title()

    return "Untitled"

# COMMAND ----------

@F.udf(returnType=StringType())
def generate_slug(title: str) -> str:
    """Generate a URL-safe slug from a title."""
    if not title:
        return "untitled"

    import re
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    slug = slug.strip("-")
    if len(slug) > 60:
        slug = slug[:60].rsplit("-", 1)[0]
    return slug or "untitled"

# COMMAND ----------

@F.udf(returnType=StringType())
def elements_to_markdown(parsed_json: str) -> str:
    """Convert ai_parse_document output elements to markdown.

    Reconstructs readable markdown from the structured elements
    returned by ai_parse_document.
    """
    if not parsed_json:
        return ""

    import json
    try:
        parsed = json.loads(parsed_json) if isinstance(parsed_json, str) else parsed_json
    except (json.JSONDecodeError, TypeError):
        return str(parsed_json)

    doc = parsed.get("document", {})
    elements = doc.get("elements", [])
    if not elements:
        return ""

    lines = []
    for elem in elements:
        etype = elem.get("type", "text")
        content = elem.get("content", "")

        if etype == "title":
            lines.append(f"# {content}\n")
        elif etype == "section_header":
            lines.append(f"## {content}\n")
        elif etype == "table":
            lines.append(content)  # Already HTML from ai_parse_document
            lines.append("")
        elif etype == "figure":
            desc = elem.get("description", "")
            lines.append(f"*[Figure: {desc or 'image'}]*\n")
        elif etype in ("page_header", "page_footer", "page_number"):
            continue  # Skip layout elements
        else:
            lines.append(content)
            lines.append("")

    return "\n".join(lines).strip()

# COMMAND ----------

chunk_schema = ArrayType(
    StructType([
        StructField("chunk_index", IntegerType()),
        StructField("chunk_text", StringType()),
        StructField("token_count", IntegerType()),
    ])
)

@F.udf(returnType=chunk_schema)
def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> list:
    """Split text into overlapping chunks by estimated token count."""
    if not text:
        return [{"chunk_index": 0, "chunk_text": "", "token_count": 0}]

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
        chunk_str = " ".join(chunk_words)
        token_est = int(len(chunk_words) * tokens_per_word)

        chunks.append({
            "chunk_index": idx,
            "chunk_text": chunk_str,
            "token_count": token_est,
        })

        start = end - words_overlap if end < len(words) else end
        idx += 1

    return chunks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables

# COMMAND ----------

@dlt.table(
    name="silver_content",
    comment="Parsed and enriched source content. Uses ai_parse_document for binary docs, direct text for markdown/text files.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("content_not_empty", "clean_content IS NOT NULL AND LENGTH(clean_content) > 10")
def silver_content():
    """Parse documents and extract clean text content.

    Binary formats (PDF, DOCX, PPTX, images) are parsed with ai_parse_document.
    Text/markdown files are decoded directly from bytes.
    """
    sources = dlt.read("sources")

    # Split into binary docs (need ai_parse_document) and text files (direct decode)
    is_text_file = F.col("content_type").isin("article", "note") & (
        F.col("file_path").endswith(".md")
        | F.col("file_path").endswith(".txt")
        | F.col("file_path").endswith(".html")
    )

    # For text files: decode bytes directly
    text_docs = (
        sources.filter(is_text_file)
        .withColumn("clean_content", F.col("raw_bytes").cast("string"))
    )

    # For binary docs: use ai_parse_document
    binary_docs = (
        sources.filter(~is_text_file)
        .withColumn(
            "parsed",
            F.expr("ai_parse_document(raw_bytes, MAP('version', '2.0'))"),
        )
        .withColumn(
            "clean_content",
            elements_to_markdown(F.to_json(F.col("parsed"))),
        )
    )

    # Union both paths
    combined = text_docs.unionByName(binary_docs, allowMissingColumns=True)

    return (
        combined
        .withColumn("title", extract_title(F.col("clean_content"), F.col("file_path")))
        .withColumn("slug", generate_slug(F.col("title")))
        .select(
            "source_id",
            "file_path",
            "content_type",
            "title",
            "slug",
            "clean_content",
            "content_hash",
            "ingested_at",
        )
    )

# COMMAND ----------

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
