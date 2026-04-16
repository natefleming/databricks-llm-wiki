# Foundation Model API (FMAPI)

The Databricks Foundation Model API provides pay-per-token access to state-of-the-art language models without managing any serving infrastructure.

## Available Models

FMAPI provides access to models from multiple providers:

- **Anthropic Claude** — Claude Sonnet 4.5, Claude Opus 4.5, Claude Haiku 4.5 for text generation
- **Meta Llama** — Llama 3.1, Llama 3.3, Llama 4 Maverick for open-source alternatives
- **Embedding models** — `databricks-gte-large-en` for text embeddings used by [[vector-databases]]

## How to Call FMAPI

Using the Databricks Python SDK:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="databricks-claude-sonnet-4-5",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is the LLM Wiki pattern?"}
    ],
    max_tokens=2048,
    temperature=0.3,
)
answer = response.choices[0].message.content
```

The API follows the OpenAI chat completions format, making it compatible with most LLM frameworks.

## Role in the LLM Wiki

The [[llm-wiki-pattern]] uses FMAPI for two operations:

1. **Page compilation** — The compilation engine calls FMAPI to transform raw source chunks into structured wiki pages with cross-references, summaries, and metadata.

2. **Query synthesis** — When answering questions, the query engine retrieves relevant wiki pages and uses FMAPI to synthesize a cited answer.

The default model is `databricks-claude-sonnet-4-5`, chosen for its balance of capability and cost. The model endpoint is configurable in `wiki_config.yaml`.

## SQL AI Functions

Databricks also provides AI capabilities as SQL functions:

- `ai_query()` — Call any serving endpoint from SQL
- `ai_parse_document()` — Parse PDFs, DOCX, PPTX into structured elements
- `ai_classify()`, `ai_extract()`, `ai_gen()` — Task-specific AI functions

The [[spark-declarative-pipelines]] in the LLM Wiki use `ai_parse_document()` in the silver layer to extract text from binary document formats.
