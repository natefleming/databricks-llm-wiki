"""Tests for configuration loading."""

from pathlib import Path

from llm_wiki.config import WikiConfig, load_config


class TestWikiConfig:
    """Tests for WikiConfig model."""

    def test_default_config(self) -> None:
        config = WikiConfig()
        assert config.wiki.name == "LLM Wiki"
        assert config.wiki.catalog == "llm_wiki"
        assert config.wiki.default_model == "databricks-claude-sonnet-4"
        assert config.wiki.max_compilation_concurrency == 5

    def test_load_from_file(self) -> None:
        config_path = Path(__file__).parent.parent / "wiki_config.yaml"
        if config_path.exists():
            config = load_config(config_path)
            assert config.wiki.name == "LLM Wiki"
            assert "concept" in config.page_types
            assert "permanent" in config.freshness_tiers
            assert config.lakebase.instance_name == "llm-wiki-db"

    def test_load_missing_file(self) -> None:
        config = load_config("/nonexistent/path.yaml")
        # Should return defaults
        assert config.wiki.name == "LLM Wiki"

    def test_page_types(self) -> None:
        config_path = Path(__file__).parent.parent / "wiki_config.yaml"
        if config_path.exists():
            config = load_config(config_path)
            assert "concept" in config.page_types
            assert config.page_types["concept"].default_freshness == "monthly"
            assert "entity" in config.page_types

    def test_freshness_tiers(self) -> None:
        config_path = Path(__file__).parent.parent / "wiki_config.yaml"
        if config_path.exists():
            config = load_config(config_path)
            assert config.freshness_tiers["live"].max_age_minutes == 15
            assert config.freshness_tiers["permanent"].max_age_minutes is None
