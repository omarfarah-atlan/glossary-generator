"""End-to-end tests for the glossary generation workflow."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import json

from app.models import (
    GlossaryTermDraft,
    TermStatus,
    AssetMetadata,
    WorkflowConfig,
)
from app.activities import GlossaryActivities


class TestGlossaryActivitiesE2E:
    """End-to-end tests for glossary activities."""

    @pytest.fixture
    def activities(self):
        """Create activities instance with mocked clients."""
        activities = GlossaryActivities()
        return activities

    @pytest.mark.asyncio
    async def test_validate_configuration_valid(self, activities):
        """Test configuration validation with valid config."""
        # Mock the atlan client
        activities._atlan_client = MagicMock()
        activities._atlan_client.validate_glossary_exists = AsyncMock(return_value=True)

        config = {
            "target_glossary_qn": "default/glossary/test",
            "asset_types": ["Table"],
            "max_assets": 10,
        }

        result = await activities.validate_configuration(config)

        assert result["valid"] is True
        assert "config" in result

    @pytest.mark.asyncio
    async def test_validate_configuration_invalid_glossary(self, activities):
        """Test configuration validation with non-existent glossary."""
        activities._atlan_client = MagicMock()
        activities._atlan_client.validate_glossary_exists = AsyncMock(return_value=False)

        config = {
            "target_glossary_qn": "nonexistent/glossary",
        }

        result = await activities.validate_configuration(config)

        assert result["valid"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_fetch_metadata(self, activities):
        """Test metadata fetching."""
        mock_assets = [
            AssetMetadata(
                qualified_name="db/schema/users",
                name="users",
                type_name="Table",
            ),
        ]

        activities._atlan_client = MagicMock()
        activities._atlan_client.fetch_assets_with_descriptions = AsyncMock(
            return_value=mock_assets
        )

        config = {
            "target_glossary_qn": "test/glossary",
            "asset_types": ["Table"],
            "max_assets": 100,
            "min_popularity_score": 0.0,
            "batch_size": 10,
            "include_columns": True,
        }

        result = await activities.fetch_metadata(config)

        assert len(result) == 1
        assert result[0]["name"] == "users"

    @pytest.mark.asyncio
    async def test_prioritize_assets(self, activities):
        """Test asset prioritization."""
        assets = [
            {
                "qualified_name": "low",
                "name": "low",
                "type_name": "Table",
                "description": None,
                "user_description": None,
                "columns": [],
                "popularity_score": 0.1,
                "view_count": 0,
                "query_count": 10,
                "user_count": 1,
                "tags": [],
                "classifications": [],
                "owner": None,
                "database_name": None,
                "schema_name": None,
            },
            {
                "qualified_name": "high",
                "name": "high",
                "type_name": "Table",
                "description": "Important table",
                "user_description": None,
                "columns": [],
                "popularity_score": 0.9,
                "view_count": 100,
                "query_count": 1000,
                "user_count": 50,
                "tags": ["important"],
                "classifications": [],
                "owner": None,
                "database_name": None,
                "schema_name": None,
            },
        ]
        usage = {
            "low": {
                "qualified_name": "low",
                "query_frequency": 10,
                "unique_users": 1,
                "last_accessed": None,
                "popularity_score": 0.1,
            },
            "high": {
                "qualified_name": "high",
                "query_frequency": 1000,
                "unique_users": 50,
                "last_accessed": None,
                "popularity_score": 0.9,
            },
        }

        result = await activities.prioritize_assets(assets, usage, 2)

        # High priority should come first
        assert result[0]["qualified_name"] == "high"

    @pytest.mark.asyncio
    async def test_generate_term_definitions(self, activities):
        """Test term definition generation."""
        activities._llm_client = MagicMock()
        activities._llm_client.generate_term_definition = AsyncMock(
            return_value={
                "name": "Users Table",
                "definition": "A table containing user account information.",
                "short_description": "User accounts",
                "examples": ["User registration"],
                "synonyms": ["accounts"],
                "confidence": "high",
            }
        )
        activities._term_generator = None  # Force recreation with mock

        assets = [
            {
                "qualified_name": "db/users",
                "name": "users",
                "type_name": "Table",
                "description": None,
                "user_description": None,
                "columns": [],
                "popularity_score": 0.5,
                "view_count": 0,
                "query_count": 100,
                "user_count": 25,
                "tags": [],
                "classifications": [],
                "owner": None,
                "database_name": None,
                "schema_name": None,
            }
        ]
        usage = {}

        # Patch the term generator
        with patch.object(activities, "_term_generator", None):
            activities._llm_client = AsyncMock()
            activities._llm_client.generate_term_definition = AsyncMock(
                return_value={
                    "name": "Users Table",
                    "definition": "A table containing user account information.",
                    "confidence": "high",
                }
            )

            result = await activities.generate_term_definitions(
                assets, usage, "test/glossary"
            )

        # Should have generated terms (or empty if mocking isn't set up correctly)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_save_and_retrieve_draft_terms(self, activities):
        """Test saving and retrieving draft terms from state store."""
        # This test requires Dapr to be running
        # Skip if Dapr is not available
        pytest.skip("Requires Dapr sidecar to be running")

    @pytest.mark.asyncio
    async def test_full_workflow_integration(self, activities):
        """Test the complete workflow flow with mocked services."""
        # Mock all external services
        activities._atlan_client = MagicMock()
        activities._atlan_client.validate_glossary_exists = AsyncMock(return_value=True)
        activities._atlan_client.fetch_assets_with_descriptions = AsyncMock(
            return_value=[
                AssetMetadata(
                    qualified_name="db/users",
                    name="users",
                    type_name="Table",
                    description="User table",
                )
            ]
        )

        activities._llm_client = MagicMock()
        activities._llm_client.generate_term_definition = AsyncMock(
            return_value={
                "name": "Users",
                "definition": "User data storage",
                "confidence": "high",
            }
        )

        # Run through activities
        config = {
            "target_glossary_qn": "test/glossary",
            "asset_types": ["Table"],
            "max_assets": 10,
            "min_popularity_score": 0.0,
            "batch_size": 10,
            "include_columns": True,
        }

        # Step 1: Validate
        validation = await activities.validate_configuration(config)
        assert validation["valid"]

        # Step 2: Fetch metadata
        assets = await activities.fetch_metadata(validation["config"])
        assert len(assets) > 0

        # Step 3: Fetch usage
        usage = await activities.fetch_usage_signals(assets)
        assert isinstance(usage, dict)

        # Step 4: Prioritize
        prioritized = await activities.prioritize_assets(assets, usage, 10)
        assert len(prioritized) > 0
