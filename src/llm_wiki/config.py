"""Configuration management for LLM Wiki.

Loads settings from wiki_config.yaml with environment variable overrides.

Usage:
    from llm_wiki.config import get_config

    config = get_config()
    print(config.wiki.catalog)
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class WikiSettings(BaseModel):
    """Top-level wiki settings."""

    name: str = "LLM Wiki"
    catalog: str = "llm_wiki"
    default_model: str = "databricks-claude-sonnet-4"
    embedding_model: str = "databricks-gte-large-en"
    max_compilation_concurrency: int = 5


class PageTypeConfig(BaseModel):
    """Configuration for a wiki page type."""

    description: str
    default_freshness: str = "monthly"


class FreshnessTierConfig(BaseModel):
    """Configuration for a freshness tier."""

    max_age_minutes: int | None = None


class ConfidenceLevelConfig(BaseModel):
    """Configuration for a confidence level."""

    description: str
    min_sources: int = 0


class LakebaseConfig(BaseModel):
    """Lakebase connection settings."""

    instance_name: str = "llm-wiki-db"
    database_name: str = "wiki"


class VectorSearchConfig(BaseModel):
    """Vector Search settings."""

    endpoint_name: str = "llm-wiki-vs-endpoint"
    index_name: str = "llm_wiki.wiki.pages_vs_index"


class ObsidianConfig(BaseModel):
    """Obsidian sync settings."""

    volume_path: str = "/Volumes/llm_wiki/wiki/obsidian"


class WikiConfig(BaseModel):
    """Root configuration model for LLM Wiki."""

    wiki: WikiSettings = Field(default_factory=WikiSettings)
    page_types: dict[str, PageTypeConfig] = Field(default_factory=dict)
    freshness_tiers: dict[str, FreshnessTierConfig] = Field(default_factory=dict)
    confidence_levels: dict[str, ConfidenceLevelConfig] = Field(default_factory=dict)
    lakebase: LakebaseConfig = Field(default_factory=LakebaseConfig)
    vector_search: VectorSearchConfig = Field(default_factory=VectorSearchConfig)
    obsidian: ObsidianConfig = Field(default_factory=ObsidianConfig)


def load_config(config_path: str | Path | None = None) -> WikiConfig:
    """Load configuration from a YAML file.

    Args:
        config_path: Path to wiki_config.yaml. If None, searches common locations.

    Returns:
        Parsed WikiConfig instance.
    """
    if config_path is None:
        search_paths = [
            Path("wiki_config.yaml"),
            Path(__file__).parent.parent.parent / "wiki_config.yaml",
        ]
        for p in search_paths:
            if p.exists():
                config_path = p
                break

    if config_path is None:
        return WikiConfig()

    config_path = Path(config_path)
    if not config_path.exists():
        return WikiConfig()

    with open(config_path) as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    return WikiConfig.model_validate(raw)


@lru_cache(maxsize=1)
def get_config(config_path: str | None = None) -> WikiConfig:
    """Get the cached configuration singleton.

    Args:
        config_path: Optional path to wiki_config.yaml.

    Returns:
        Cached WikiConfig instance.
    """
    return load_config(config_path)
