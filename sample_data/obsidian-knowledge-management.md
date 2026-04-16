# Obsidian for Knowledge Management

Obsidian is a free, open-source knowledge management application that works with local markdown files. It is the recommended viewer for the [[llm-wiki-pattern]].

## Why Obsidian

Obsidian provides native support for the key features the LLM Wiki relies on:

- **Wikilinks** — `[[page-slug]]` syntax creates bidirectional links between pages. Obsidian renders these as clickable links and tracks backlinks automatically.
- **Graph view** — An interactive knowledge graph shows how all pages are interconnected, making it easy to discover relationships.
- **YAML frontmatter** — Obsidian reads YAML frontmatter at the top of markdown files, displaying metadata like page type, confidence level, and tags.
- **Search** — Full-text search across all pages in the vault.
- **Local-first** — All data stays on your machine as plain markdown files. No cloud dependency for viewing.

## Integration with the LLM Wiki

The LLM Wiki exports compiled pages to a [[unity-catalog]] Volume as markdown files with YAML frontmatter and `[[wikilinks]]`. To use Obsidian:

1. Sync pages from the Databricks Volume to a local directory:
   ```bash
   databricks fs cp /Volumes/nfleming/wiki/obsidian/ ./vault/ --recursive --overwrite
   ```

2. Open the local directory as an Obsidian vault.

3. Use the graph view to explore the knowledge graph, click wikilinks to navigate between pages, and use the backlinks panel to see which pages reference the current one.

## Page Format

Each exported page follows this format:

```markdown
---
title: "Example Page"
type: concept
confidence: high
sources:
  - source-doc-1
related:
  - related-page
tags:
  - example
freshness_tier: weekly
created: 2026-04-16
updated: 2026-04-16
---

# Example Page

Content with [[wikilinks]] to other pages...
```

## Alternatives

While Obsidian is the recommended viewer, the wiki pages are standard markdown files that work with any editor. The web UI provided by the Databricks App offers a similar browsing experience without requiring a local install.
