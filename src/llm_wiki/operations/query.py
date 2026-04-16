"""Query operation: search the wiki and synthesize answers.

Handles answering questions by searching for relevant pages and
using the LLM to synthesize a cited answer.

Usage:
    from llm_wiki.operations.query import query_wiki

    answer = query_wiki(search, compiler, "What is Kubernetes?")
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

from llm_wiki.compiler.context import ContextAssembler
from llm_wiki.compiler.prompts import get_query_prompt
from llm_wiki.config import WikiConfig
from llm_wiki.log import logger
from llm_wiki.models import SearchResult
from llm_wiki.search import WikiSearch
from llm_wiki.storage.delta import DeltaStore
from llm_wiki.storage.lakebase import LakebaseStore


class QueryEngine:
    """Answers questions by searching the wiki and synthesizing responses."""

    def __init__(
        self,
        search: WikiSearch,
        delta_store: DeltaStore,
        config: WikiConfig,
        client: WorkspaceClient | None = None,
    ) -> None:
        """Initialize the query engine.

        Args:
            search: WikiSearch instance for finding relevant pages.
            delta_store: DeltaStore for reading full page content.
            config: Wiki configuration.
            client: Optional pre-configured WorkspaceClient.
        """
        self._search = search
        self._delta = delta_store
        self._config = config
        self._client = client or WorkspaceClient()
        self._assembler = ContextAssembler(delta_store)

    def query(self, question: str, mode: str = "hybrid") -> dict[str, str | list]:
        """Answer a question using the wiki.

        Searches for relevant pages, assembles context, and uses the LLM
        to synthesize a cited answer.

        Args:
            question: The question to answer.
            mode: Search mode ('fulltext', 'semantic', 'hybrid').

        Returns:
            Dictionary with 'answer', 'citations', and 'sources_used'.
        """
        logger.info("Processing query", question=question[:100], mode=mode)

        # Search for relevant pages
        search_results = self._search.search(question, limit=10, mode=mode)

        if not search_results:
            return {
                "answer": "No relevant pages found in the wiki for this question.",
                "citations": [],
                "sources_used": [],
            }

        # Load full page content for top results
        pages = []
        for result in search_results[:5]:
            page = self._delta.get_page(result.page_id)
            if page:
                pages.append(page)

        if not pages:
            return {
                "answer": "Found matching pages but could not load their content.",
                "citations": [],
                "sources_used": [r.page_id for r in search_results],
            }

        # Assemble context
        context_text = self._assembler.assemble_query_context(pages)

        # Build prompt and call LLM
        messages = get_query_prompt(question, context_text)

        try:
            response = self._client.serving_endpoints.query(
                name=self._config.wiki.default_model,
                messages=messages,
                max_tokens=2048,
                temperature=0.2,
            )

            answer = ""
            if hasattr(response, "choices") and response.choices:
                choice = response.choices[0]
                if hasattr(choice, "message") and choice.message:
                    answer = choice.message.content or ""

        except Exception as e:
            logger.error("Query LLM call failed", error=str(e))
            # Fall back to returning raw context
            answer = f"Could not synthesize an answer (LLM error: {e}). Relevant pages:\n\n"
            for page in pages:
                answer += f"- [[{page.page_id}]] {page.title}\n"

        # Extract citations from answer
        import re

        citations = re.findall(r"\[\[([a-z0-9][a-z0-9-]*)\]\]", answer)

        # Log activity
        self._delta.log_activity(
            "query",
            f"Query: {question[:100]}",
            [p.page_id for p in pages],
        )

        return {
            "answer": answer,
            "citations": list(dict.fromkeys(citations)),
            "sources_used": [r.page_id for r in search_results],
        }
