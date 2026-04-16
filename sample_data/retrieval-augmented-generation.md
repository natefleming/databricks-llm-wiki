# Retrieval-Augmented Generation (RAG)

Retrieval-Augmented Generation (RAG) is a technique that enhances LLM responses by retrieving relevant documents from an external knowledge base before generating an answer.

## How RAG Works

1. **Indexing** — Documents are chunked, embedded into vectors, and stored in a [[vector-databases]] index.
2. **Retrieval** — When a query arrives, it is embedded and used to find the most similar document chunks.
3. **Generation** — The retrieved chunks are prepended to the prompt as context, and the LLM generates an answer grounded in those chunks.

## Limitations

RAG suffers from several well-known limitations:

- **Amnesia** — The LLM rediscovers knowledge from scratch per query. There is no persistent state between queries.
- **Context window pressure** — Retrieved chunks compete for space in the context window with the actual question and instructions.
- **Chunk boundary issues** — Information split across chunks may not be retrieved together, leading to incomplete answers.
- **No synthesis** — RAG does not build up cross-referenced knowledge over time. Each query is independent.

The [[llm-wiki-pattern]] addresses many of these limitations by compiling knowledge once during ingestion rather than re-deriving it per query.

## RAG on Databricks

Databricks provides several components for building RAG systems:

- **[[vector-databases]]** via Databricks Vector Search for storing and querying embeddings
- **[[foundation-model-api]]** for the generation step
- **[[unity-catalog]]** for governing the source documents and embeddings
- **[[spark-declarative-pipelines]]** for building the ingestion pipeline that prepares documents for indexing

## When to Use RAG vs. LLM Wiki

RAG is best for scenarios where:
- The source corpus changes rapidly (real-time data)
- Simple question-answering without deep synthesis is sufficient
- The knowledge domain is very broad with infrequent repeat queries

The LLM Wiki pattern is better when:
- Knowledge needs to compound over time
- Cross-referencing and contradiction detection matter
- The same domain is queried repeatedly
- Synthesis and analysis across sources are valued
