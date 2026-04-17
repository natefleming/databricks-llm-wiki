"""Query operation: Karpathy-style index-first Q&A.

Two-stage retrieval:
  1. Show the LLM the compact wiki index and ask which pages are relevant.
  2. Fetch those pages in full, ask the LLM to synthesize with citations.

Falls back to vector search if the index path returns nothing useful.

Usage:
    from llm_wiki.operations.query import query_wiki

    answer = query_wiki(search, store, question="Who is the ring-bearer?")
"""

from __future__ import annotations

import json
import re
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from llm_wiki.compiler.context import ContextAssembler
from llm_wiki.compiler.prompts import get_query_prompt
from llm_wiki.config import WikiConfig
from llm_wiki.log import logger
from llm_wiki.search import WikiSearch


class QueryEngine:
    """Index-first query engine following Karpathy's LLM Wiki pattern."""

    def __init__(
        self,
        search: WikiSearch,
        delta_store: Any,
        config: WikiConfig,
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the query engine.

        Args:
            search: WikiSearch for vector fallback.
            delta_store: Any store that supports get_index, get_page, list_pages.
                         (In practice, LakebaseStore when available.)
            config: Wiki configuration.
            client: Optional pre-configured WorkspaceClient.
        """
        self._search = search
        self._delta = delta_store
        self._config = config
        self._client = client or WorkspaceClient()
        self._assembler = ContextAssembler(delta_store)

    def query(
        self,
        question: str,
        use_index: bool = True,
        max_pages: int = 6,
    ) -> dict[str, Any]:
        """Answer a question using the wiki.

        Follows Karpathy's pattern:
          1. Hand the LLM the wiki index and the question.
          2. LLM picks relevant page slugs.
          3. Fetch those pages in full.
          4. LLM synthesizes a cited answer.

        Args:
            question: The user's question.
            use_index: If True, do index-first retrieval. If False, skip to
                       vector search.
            max_pages: Max pages to load in step 3.

        Returns:
            dict with keys: answer, citations, pages_used, retrieval_path.
        """
        logger.info("Processing query", question=question[:100], use_index=use_index)

        page_ids: list[str] = []
        retrieval_path = "vector"

        # Stage 1: Index-first retrieval
        if use_index and hasattr(self._delta, "get_index"):
            try:
                page_ids = self._select_pages_from_index(question, max_pages=max_pages)
                if page_ids:
                    retrieval_path = "index"
            except Exception as e:
                logger.warning("Index-first retrieval failed, falling back to vector", error=str(e))

        # Stage 2: Vector fallback if index didn't yield anything
        if not page_ids:
            search_results = self._search.search(question, limit=max_pages, mode="hybrid")
            page_ids = [r.page_id for r in search_results[:max_pages]]
            retrieval_path = "vector"

        if not page_ids:
            return {
                "answer": "No relevant pages found in the wiki for this question.",
                "citations": [],
                "pages_used": [],
                "retrieval_path": retrieval_path,
            }

        # Stage 3: Load full page content
        pages = []
        for pid in page_ids:
            page = self._delta.get_page(pid)
            if page:
                pages.append(page)

        if not pages:
            return {
                "answer": f"Identified {len(page_ids)} relevant pages but none could be loaded.",
                "citations": [],
                "pages_used": page_ids,
                "retrieval_path": retrieval_path,
            }

        # Stage 4: Synthesize cited answer
        context_text = self._assembler.assemble_query_context(pages)
        messages = get_query_prompt(question, context_text)
        answer = self._llm_synthesize(messages)

        citations = re.findall(r"\[\[([a-z0-9][a-z0-9-]*)\]\]", answer)
        citations = list(dict.fromkeys(citations))

        # Log activity (best effort)
        try:
            self._delta.log_activity(
                "query",
                f"Query: {question[:100]} via {retrieval_path}",
                [p.page_id for p in pages],
            )
        except Exception:
            pass

        return {
            "answer": answer,
            "citations": citations,
            "pages_used": [p.page_id for p in pages],
            "retrieval_path": retrieval_path,
        }

    def _select_pages_from_index(self, question: str, max_pages: int = 6) -> list[str]:
        """Ask the LLM to pick relevant page slugs from the wiki index.

        Returns:
            Ordered list of page_ids the LLM deems relevant (can be empty).
        """
        index_entries = self._delta.get_index()
        if not index_entries:
            return []

        # Cap the index to what fits comfortably in prompt (roughly 500 pages).
        if len(index_entries) > 500:
            index_entries = index_entries[:500]

        index_md_lines: list[str] = []
        for e in index_entries:
            summary = e.get("summary") or ""
            index_md_lines.append(
                f"- {e['page_id']} ({e['page_type']}): {e['title']} — {summary}"
            )
        index_md = "\n".join(index_md_lines)

        system = (
            "You are a wiki retrieval planner. Given the wiki index and a user question, "
            "choose the page_ids most likely to answer it. Return STRICT JSON: "
            '{"page_ids": ["slug-a", "slug-b", ...]} '
            f"with at most {max_pages} entries, ordered by expected relevance. "
            "Choose broadly if the question is open-ended. "
            "Return {\"page_ids\": []} only if nothing in the index is relevant."
        )
        user = f"Question: {question}\n\nWiki index:\n{index_md}"

        messages = [
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
            ChatMessage(role=ChatMessageRole.USER, content=user),
        ]

        try:
            resp = self._client.serving_endpoints.query(
                name=self._config.wiki.default_model,
                messages=messages,
                max_tokens=512,
                temperature=0.0,
            )
            text = _extract_text(resp)
            data = _safe_json_extract(text)
            ids = data.get("page_ids") if isinstance(data, dict) else []
            if not isinstance(ids, list):
                return []
            # Filter to actual slugs that exist in the index
            valid = {e["page_id"] for e in index_entries}
            return [pid for pid in ids if isinstance(pid, str) and pid in valid][:max_pages]
        except Exception as e:
            logger.warning("Index-selection LLM call failed", error=str(e))
            return []

    def _llm_synthesize(self, messages: list[dict[str, str]]) -> str:
        """Call the synthesis LLM. Returns the answer text."""
        try:
            sdk_messages = [
                ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])
                for m in messages
            ]
            resp = self._client.serving_endpoints.query(
                name=self._config.wiki.default_model,
                messages=sdk_messages,
                max_tokens=2048,
                temperature=0.2,
            )
            return _extract_text(resp)
        except Exception as e:
            logger.error("Synthesis LLM call failed", error=str(e))
            return f"Error synthesizing answer: {e}"


def _extract_text(response: Any) -> str:
    """Pull the assistant content from an FMAPI chat response."""
    if hasattr(response, "choices") and response.choices:
        choice = response.choices[0]
        if hasattr(choice, "message") and hasattr(choice.message, "content"):
            return choice.message.content or ""
        if isinstance(choice, dict):
            return choice.get("message", {}).get("content", "")
    return ""


def _safe_json_extract(text: str) -> dict:
    """Pull a JSON object out of possibly-chatty LLM output."""
    text = text.strip()
    # Try direct parse first
    try:
        return json.loads(text)
    except Exception:
        pass
    # Look for the first {...} block
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}
