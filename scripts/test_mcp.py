#!/usr/bin/env python3
"""Test every MCP tool exposed by the LLM Wiki app.

Connects to the MCP Streamable HTTP endpoint, initializes a session,
lists tools, and calls each one with a reasonable input. Prints the
first few hundred chars of each response.

Usage:
    python scripts/test_mcp.py [--url URL] [--profile PROFILE]
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def get_token(profile: str) -> str:
    """Get a Databricks OAuth token for the given CLI profile."""
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile],
        capture_output=True, text=True, check=True,
    )
    import json
    return json.loads(result.stdout)["access_token"]


async def test_all_tools(url: str, token: str) -> None:
    """Connect to MCP server and call every tool."""
    headers = {"Authorization": f"Bearer {token}"}

    print(f"Connecting to: {url}")
    print(f"With token: {token[:20]}...\n")

    async with streamablehttp_client(url, headers=headers) as (read, write, _session_id_cb):
        async with ClientSession(read, write) as session:
            # Initialize
            init_result = await session.initialize()
            print(f"[OK] initialize -> server: {init_result.serverInfo.name} v{init_result.serverInfo.version}\n")

            # List tools
            tools_result = await session.list_tools()
            print(f"[OK] list_tools -> {len(tools_result.tools)} tools")
            for tool in tools_result.tools:
                print(f"     - {tool.name}: {tool.description.splitlines()[0][:80] if tool.description else ''}")
            print()

            # Define test cases per tool
            test_cases: list[tuple[str, dict[str, Any]]] = [
                ("wiki_stats", {}),
                ("wiki_list", {"limit": 5}),
                ("wiki_list", {"page_type": "concept", "limit": 3}),
                ("wiki_search", {"query": "gandalf", "limit": 3}),
                ("wiki_search", {"query": "lakehouse", "limit": 3}),
                ("wiki_read", {"page_id": "gandalf"}),
                ("wiki_read", {"page_id": "nonexistent-xyz"}),
                ("wiki_lint", {}),
                # wiki_query uses LLM - slow, skip by default
                # ("wiki_query", {"question": "Who is Sauron?"}),
                # wiki_ingest writes data - skip to keep test read-only
                # ("wiki_ingest", {"text": "Test content", "title": "test"}),
            ]

            for tool_name, args in test_cases:
                print(f"[CALL] {tool_name}({args})")
                try:
                    result = await session.call_tool(tool_name, args)
                    # Extract text from content
                    text_parts = []
                    for block in result.content:
                        if hasattr(block, "text"):
                            text_parts.append(block.text)
                    output = "\n".join(text_parts)
                    preview = output[:400].replace("\n", " ")
                    print(f"       {len(output)} chars | {preview}...")
                    print()
                except Exception as e:
                    print(f"       ERROR: {e}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Test LLM Wiki MCP server")
    parser.add_argument(
        "--url",
        default="https://llm-wiki-dev-1444828305810485.aws.databricksapps.com/mcp",
        help="MCP endpoint URL",
    )
    parser.add_argument("--profile", default="aws-field-eng", help="Databricks CLI profile for auth")
    args = parser.parse_args()

    token = get_token(args.profile)
    asyncio.run(test_all_tools(args.url, token))


if __name__ == "__main__":
    main()
