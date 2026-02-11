"""Client for fetching usage signals from Atlan."""

import logging
from typing import Dict, List, Optional
from datetime import datetime

from app.models import UsageSignals, AssetMetadata

logger = logging.getLogger(__name__)


class UsageSignalClient:
    """Client for aggregating usage signals for assets."""

    def __init__(self, atlan_client=None):
        self._atlan_client = atlan_client

    async def fetch_usage_signals(
        self,
        assets: List[AssetMetadata]
    ) -> Dict[str, UsageSignals]:
        """Fetch usage signals for a list of assets.

        In a production environment, this would query Atlan's usage
        analytics APIs. For now, we extract signals from the asset metadata.
        """
        signals = {}

        for asset in assets:
            signals[asset.qualified_name] = UsageSignals(
                qualified_name=asset.qualified_name,
                query_frequency=asset.query_count,
                unique_users=asset.user_count,
                last_accessed=None,  # Would come from usage API
                popularity_score=asset.popularity_score,
            )

        return signals

    def calculate_priority_score(
        self,
        asset: AssetMetadata,
        usage: Optional[UsageSignals] = None
    ) -> float:
        """Calculate a priority score for an asset based on usage and metadata quality.

        Higher scores indicate higher priority for glossary term generation.
        """
        score = 0.0

        # Base score from popularity
        if usage:
            score += min(usage.popularity_score * 10, 30)  # Max 30 points
            score += min(usage.query_frequency / 100, 20)  # Max 20 points
            score += min(usage.unique_users * 2, 20)  # Max 20 points
        else:
            score += min(asset.popularity_score * 10, 30)

        # Bonus for having existing descriptions
        if asset.description:
            score += 10
        if asset.user_description:
            score += 5

        # Bonus for having column information
        if asset.columns:
            score += min(len(asset.columns), 10)
            described_cols = sum(1 for c in asset.columns if c.description)
            score += min(described_cols * 2, 10)

        # Bonus for having tags/classifications
        score += min(len(asset.tags) * 2, 10)
        score += min(len(asset.classifications) * 3, 15)

        return score

    def prioritize_assets(
        self,
        assets: List[AssetMetadata],
        usage_signals: Dict[str, UsageSignals],
        max_results: int = 100
    ) -> List[AssetMetadata]:
        """Sort assets by priority score and return top results."""
        scored_assets = []

        for asset in assets:
            usage = usage_signals.get(asset.qualified_name)
            score = self.calculate_priority_score(asset, usage)
            scored_assets.append((score, asset))

        # Sort by score descending
        scored_assets.sort(key=lambda x: x[0], reverse=True)

        # Return top assets
        return [asset for _, asset in scored_assets[:max_results]]
