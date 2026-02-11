"""Unit tests for the client modules."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.models import AssetMetadata, UsageSignals
from clients.usage_client import UsageSignalClient


class TestUsageSignalClient:
    """Tests for the UsageSignalClient class."""

    @pytest.mark.asyncio
    async def test_fetch_usage_signals(self):
        """Test fetching usage signals from assets."""
        client = UsageSignalClient()
        assets = [
            AssetMetadata(
                qualified_name="db/schema/users",
                name="users",
                type_name="Table",
                query_count=100,
                user_count=25,
                popularity_score=0.85,
            ),
        ]

        signals = await client.fetch_usage_signals(assets)

        assert "db/schema/users" in signals
        assert signals["db/schema/users"].query_frequency == 100
        assert signals["db/schema/users"].unique_users == 25

    def test_calculate_priority_score_basic(self):
        """Test basic priority score calculation."""
        client = UsageSignalClient()
        asset = AssetMetadata(
            qualified_name="test",
            name="test",
            type_name="Table",
            popularity_score=0.5,
        )

        score = client.calculate_priority_score(asset)

        assert score > 0

    def test_calculate_priority_score_with_description(self):
        """Test that descriptions increase score."""
        client = UsageSignalClient()
        asset_no_desc = AssetMetadata(
            qualified_name="test",
            name="test",
            type_name="Table",
        )
        asset_with_desc = AssetMetadata(
            qualified_name="test",
            name="test",
            type_name="Table",
            description="A test table",
        )

        score_no_desc = client.calculate_priority_score(asset_no_desc)
        score_with_desc = client.calculate_priority_score(asset_with_desc)

        assert score_with_desc > score_no_desc

    def test_calculate_priority_score_with_usage(self):
        """Test that usage signals affect score."""
        client = UsageSignalClient()
        asset = AssetMetadata(
            qualified_name="test",
            name="test",
            type_name="Table",
        )
        usage = UsageSignals(
            qualified_name="test",
            query_frequency=1000,
            unique_users=50,
            popularity_score=0.9,
        )

        score_without = client.calculate_priority_score(asset)
        score_with = client.calculate_priority_score(asset, usage)

        assert score_with > score_without

    def test_prioritize_assets(self):
        """Test asset prioritization."""
        client = UsageSignalClient()
        assets = [
            AssetMetadata(
                qualified_name="low",
                name="low",
                type_name="Table",
                popularity_score=0.1,
            ),
            AssetMetadata(
                qualified_name="high",
                name="high",
                type_name="Table",
                popularity_score=0.9,
                description="High priority table",
            ),
        ]
        usage = {
            "low": UsageSignals(qualified_name="low", query_frequency=10, unique_users=1),
            "high": UsageSignals(qualified_name="high", query_frequency=1000, unique_users=100),
        }

        prioritized = client.prioritize_assets(assets, usage, max_results=2)

        # High priority asset should come first
        assert prioritized[0].qualified_name == "high"

    def test_prioritize_assets_respects_limit(self):
        """Test that prioritization respects max_results."""
        client = UsageSignalClient()
        assets = [
            AssetMetadata(qualified_name=f"test_{i}", name=f"test_{i}", type_name="Table")
            for i in range(10)
        ]
        usage = {}

        prioritized = client.prioritize_assets(assets, usage, max_results=3)

        assert len(prioritized) == 3
