"""Unit tests for the term generators."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.models import AssetMetadata, ColumnMetadata, UsageSignals, GlossaryTermDraft
from generators.context_builder import ContextBuilder
from generators.prompts import PromptTemplates
from generators.term_generator import TermGenerator


class TestContextBuilder:
    """Tests for the ContextBuilder class."""

    def test_build_asset_context_basic(self):
        """Test building context for a basic asset."""
        builder = ContextBuilder()
        asset = AssetMetadata(
            qualified_name="db/schema/table",
            name="users",
            type_name="Table",
            description="User data table",
        )

        context = builder.build_asset_context(asset)

        assert context["name"] == "users"
        assert context["type"] == "Table"
        assert context["description"] == "User data table"

    def test_build_asset_context_with_columns(self):
        """Test building context with column information."""
        builder = ContextBuilder()
        asset = AssetMetadata(
            qualified_name="db/schema/table",
            name="users",
            type_name="Table",
            columns=[
                ColumnMetadata(name="id", data_type="INTEGER", description="User ID"),
                ColumnMetadata(name="email", data_type="VARCHAR", description="Email address"),
            ],
        )

        context = builder.build_asset_context(asset)

        assert len(context["columns"]) == 2
        assert context["columns"][0]["name"] == "id"
        assert context["columns"][0]["data_type"] == "INTEGER"

    def test_build_asset_context_with_usage(self):
        """Test building context with usage signals."""
        builder = ContextBuilder()
        asset = AssetMetadata(
            qualified_name="db/schema/table",
            name="users",
            type_name="Table",
        )
        usage = UsageSignals(
            qualified_name="db/schema/table",
            query_frequency=100,
            unique_users=25,
            popularity_score=0.85,
        )

        context = builder.build_asset_context(asset, usage)

        assert context["usage_stats"]["query_frequency"] == 100
        assert context["usage_stats"]["unique_users"] == 25
        assert context["usage_stats"]["popularity_score"] == 0.85

    def test_truncate_context_within_limits(self):
        """Test that context within limits is not truncated."""
        builder = ContextBuilder()
        context = {"name": "test", "type": "Table"}

        result = builder.truncate_context(context, max_tokens=1000)

        assert result == context

    def test_truncate_context_reduces_columns(self):
        """Test that large column lists are truncated."""
        builder = ContextBuilder()
        columns = [{"name": f"col_{i}", "data_type": "VARCHAR"} for i in range(50)]
        context = {"name": "test", "columns": columns}

        result = builder.truncate_context(context, max_tokens=500)

        assert len(result["columns"]) < len(columns)


class TestPromptTemplates:
    """Tests for the PromptTemplates class."""

    def test_term_definition_prompt_basic(self):
        """Test basic prompt generation."""
        prompt = PromptTemplates.term_definition_prompt(
            asset_name="users",
            asset_type="Table",
        )

        assert "users" in prompt
        assert "Table" in prompt
        assert "JSON" in prompt

    def test_term_definition_prompt_with_description(self):
        """Test prompt with existing description."""
        prompt = PromptTemplates.term_definition_prompt(
            asset_name="users",
            asset_type="Table",
            description="Stores user account information",
        )

        assert "Stores user account information" in prompt

    def test_term_definition_prompt_with_columns(self):
        """Test prompt with column information."""
        columns = [
            {"name": "id", "data_type": "INTEGER"},
            {"name": "email", "data_type": "VARCHAR"},
        ]
        prompt = PromptTemplates.term_definition_prompt(
            asset_name="users",
            asset_type="Table",
            columns=columns,
        )

        assert "id" in prompt
        assert "email" in prompt

    def test_batch_definition_prompt(self):
        """Test batch prompt generation."""
        assets = [
            {"name": "users", "type": "Table"},
            {"name": "orders", "type": "Table"},
        ]
        prompt = PromptTemplates.batch_definition_prompt(assets)

        assert "users" in prompt
        assert "orders" in prompt
        assert "Asset 1" in prompt
        assert "Asset 2" in prompt


class TestTermGenerator:
    """Tests for the TermGenerator class."""

    @pytest.mark.asyncio
    async def test_generate_term_success(self):
        """Test successful term generation."""
        mock_llm = AsyncMock()
        mock_llm.generate_term_definition.return_value = {
            "name": "User Accounts",
            "definition": "A table storing user account information.",
            "short_description": "User account data",
            "examples": ["User registration", "Profile management"],
            "synonyms": ["accounts", "members"],
            "confidence": "high",
        }

        generator = TermGenerator(llm_client=mock_llm)
        asset = AssetMetadata(
            qualified_name="db/schema/users",
            name="users",
            type_name="Table",
        )

        term = await generator.generate_term(asset, target_glossary_qn="test/glossary")

        assert term is not None
        assert term.name == "User Accounts"
        assert term.confidence == "high"
        assert "db/schema/users" in term.source_assets

    @pytest.mark.asyncio
    async def test_generate_term_handles_error(self):
        """Test that errors are handled gracefully."""
        mock_llm = AsyncMock()
        mock_llm.generate_term_definition.side_effect = Exception("API Error")

        generator = TermGenerator(llm_client=mock_llm)
        asset = AssetMetadata(
            qualified_name="db/schema/users",
            name="users",
            type_name="Table",
        )

        term = await generator.generate_term(asset, target_glossary_qn="test/glossary")

        assert term is None

    @pytest.mark.asyncio
    async def test_generate_terms_batch(self):
        """Test batch term generation."""
        mock_llm = AsyncMock()
        mock_llm.generate_term_definition.return_value = {
            "name": "Generated Term",
            "definition": "A generated definition.",
            "confidence": "medium",
        }

        generator = TermGenerator(llm_client=mock_llm, batch_size=2)
        assets = [
            AssetMetadata(qualified_name=f"db/schema/table_{i}", name=f"table_{i}", type_name="Table")
            for i in range(3)
        ]
        usage_signals = {}

        terms = await generator.generate_terms_batch(assets, usage_signals, "test/glossary")

        assert len(terms) == 3
        assert mock_llm.generate_term_definition.call_count == 3
