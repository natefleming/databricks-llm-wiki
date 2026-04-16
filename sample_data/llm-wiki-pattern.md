# The LLM Wiki Pattern

The LLM Wiki pattern, proposed by Andrej Karpathy in April 2026, is an alternative to traditional RAG (Retrieval-Augmented Generation) for knowledge management.

## Core Idea

Instead of retrieving and re-synthesizing from raw documents on every query, the LLM incrementally builds and maintains a persistent wiki — a structured, interlinked collection of markdown files that sits between you and the raw sources.

The key distinction from [[retrieval-augmented-generation]] is that knowledge is compiled once during ingest and kept current, not re-derived on every query. The wiki is a persistent, compounding artifact where cross-references are already established and contradictions have already been flagged.

## Three-Layer Architecture

The pattern defines three layers:

1. **Raw Sources** — Immutable documents such as articles, papers, images, and data files. These serve as the single source of truth.

2. **The Wiki** — LLM-generated markdown files containing summaries, entity pages, concept pages, and synthesized content. The LLM owns this layer entirely.

3. **The Schema** — A configuration document specifying wiki structure, conventions, and workflows that govern how the LLM maintains the knowledge base.

## Core Operations

- **Ingest** — Process new sources into wiki pages. The LLM reads the source, creates summary pages, updates concept pages, and logs actions.
- **Query** — Answer questions from the wiki. Good answers generated during queries are filed back as new wiki pages.
- **Lint** — Periodic health checks scanning for stale claims, missing cross-references, orphan pages, and contradictions.

## Relationship to Vector Databases

At moderate scale (hundreds of pages), the pattern avoids [[vector-databases]] entirely by relying on markdown indexing and simple search. At larger scale, hybrid approaches combining the wiki pattern with vector retrieval become necessary.

## Tools and Ecosystem

Karpathy recommends using [[obsidian-knowledge-management]] as an IDE for browsing and visualizing the wiki. The wiki can also be treated as a git repository for version control.

The pattern is designed to be implemented with any LLM agent framework and can be extended with the [[foundation-model-api]] for enterprise deployments.
