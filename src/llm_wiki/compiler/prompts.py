"""Prompt templates for LLM wiki page compilation.

Each page type has a dedicated prompt template that instructs the LLM
how to compile source content into a structured wiki page.

Usage:
    from llm_wiki.compiler.prompts import get_compilation_prompt

    prompt = get_compilation_prompt("concept", context)
"""

from __future__ import annotations

from typing import Any

# ──────────────────────────────────────────────
# System prompt (shared across all page types)
# ──────────────────────────────────────────────

SYSTEM_PROMPT = """You are a wiki compiler. Your job is to synthesize raw source material \
into well-structured, interlinked wiki pages.

Rules:
1. Write in clear, encyclopedic prose. Be factual and precise.
2. Use [[slug-format]] wikilinks to reference other concepts and entities.
3. Include a "## Sources" section at the end listing source references.
4. Flag any contradictions between sources explicitly with both viewpoints.
5. Assign confidence based on source quality: high (official docs, peer-reviewed), \
medium (reputable blogs, talks), low (single unverified source).
6. Use markdown formatting: headings, bullet lists, code blocks where appropriate.
7. Do NOT include YAML frontmatter - that will be added separately.
8. Start with a one-paragraph summary, then expand into sections."""


# ──────────────────────────────────────────────
# Page type templates
# ──────────────────────────────────────────────

_TEMPLATES: dict[str, str] = {
    "concept": """Compile a **concept page** about: {title}

Source material:
{source_chunks}

Related existing pages:
{related_pages}

Write a comprehensive wiki page that:
- Opens with a clear one-paragraph definition/summary
- Explains the concept in depth with subsections
- Links to related concepts using [[slug]] wikilinks
- Includes practical examples or use cases where relevant
- Notes any nuances, caveats, or common misconceptions
- Ends with a ## Sources section referencing the source material""",

    "entity": """Compile an **entity page** about: {title}

Source material:
{source_chunks}

Related existing pages:
{related_pages}

Write a wiki page for this entity (person, organization, project, tool) that:
- Opens with a brief identification (what/who this is)
- Covers key facts, history, and significance
- Links to related entities and concepts using [[slug]] wikilinks
- Notes relationships to other entities mentioned in sources
- Ends with a ## Sources section""",

    "source": """Compile a **source summary page** for: {title}

Full source content:
{source_chunks}

Related existing pages:
{related_pages}

Write a summary page that:
- Opens with a one-paragraph overview of the source document
- Identifies the key claims, findings, or arguments
- Extracts and lists the most important takeaways
- Links to existing wiki concepts using [[slug]] wikilinks
- Notes the source's credibility and any limitations
- Ends with full bibliographic reference in ## Sources""",

    "analysis": """Compile an **analysis page** about: {title}

Source material from multiple sources:
{source_chunks}

Related existing pages:
{related_pages}

Write a cross-cutting analysis page that:
- Opens with a thesis or key insight synthesized across sources
- Compares and contrasts different perspectives
- Identifies patterns, trends, or contradictions
- Links extensively to other wiki pages using [[slug]] wikilinks
- Draws conclusions supported by evidence from sources
- Explicitly flags where sources disagree
- Ends with a ## Sources section""",

    "index": """Compile an **index page** for the category: {title}

Pages to index:
{related_pages}

Source material for context:
{source_chunks}

Write an index page that:
- Opens with a brief description of this category
- Lists all relevant pages organized by subcategory
- Uses [[slug|Display Name]] wikilinks for each entry
- Includes one-line descriptions for each linked page
- Identifies any gaps in coverage""",
}


def get_compilation_prompt(
    page_type: str,
    title: str,
    source_chunks: str,
    related_pages: str,
) -> list[dict[str, str]]:
    """Build the full prompt messages for page compilation.

    Args:
        page_type: The type of page to compile (concept, entity, etc.).
        title: The page title.
        source_chunks: Formatted source material text.
        related_pages: Formatted text of related existing pages.

    Returns:
        List of message dicts suitable for LLM API call.
    """
    template = _TEMPLATES.get(page_type, _TEMPLATES["concept"])
    user_message = template.format(
        title=title,
        source_chunks=source_chunks or "(no source material available)",
        related_pages=related_pages or "(no related pages yet)",
    )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]


def get_query_prompt(question: str, context_pages: str) -> list[dict[str, str]]:
    """Build a prompt for answering a question from wiki pages.

    Args:
        question: The user's question.
        context_pages: Formatted text of relevant wiki pages.

    Returns:
        List of message dicts for LLM API call.
    """
    return [
        {
            "role": "system",
            "content": (
                "You are a wiki assistant. Answer questions using ONLY the wiki pages "
                "provided as context. Cite your sources using [[page-slug]] wikilinks. "
                "If the wiki doesn't contain enough information, say so clearly."
            ),
        },
        {
            "role": "user",
            "content": f"""Question: {question}

Wiki context:
{context_pages}

Answer the question using the wiki context above. Include [[slug]] citations.""",
        },
    ]
