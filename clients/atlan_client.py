"""Atlan client wrapper for metadata operations."""

import os
import json
import logging
from typing import List, Optional
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import (
    AtlasGlossary,
    AtlasGlossaryTerm,
    Asset,
    Table,
    View,
)
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.enums import AtlanConnectorType

from app.models import AssetMetadata, ColumnMetadata, GlossaryTermDraft

logger = logging.getLogger(__name__)


def _get_settings_from_store() -> dict:
    """Load settings from Dapr state store."""
    try:
        from dapr.clients import DaprClient
        with DaprClient() as client:
            state = client.get_state(store_name="statestore", key="app_settings")
            if state.data:
                return json.loads(state.data)
    except Exception as e:
        logger.debug(f"Could not load settings from Dapr: {e}")
    return {}


class AtlanMetadataClient:
    """Client for interacting with Atlan metadata catalog."""

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        # Try to load from state store first, then fall back to env vars
        settings = _get_settings_from_store()

        self.base_url = base_url or settings.get("atlan_base_url") or os.environ.get("ATLAN_BASE_URL")
        self.api_key = api_key or settings.get("atlan_api_key") or os.environ.get("ATLAN_API_KEY")
        self._client: Optional[AtlanClient] = None

    @property
    def client(self) -> AtlanClient:
        """Lazy initialization of Atlan client."""
        if self._client is None:
            if self.base_url and self.api_key:
                self._client = AtlanClient(base_url=self.base_url, api_key=self.api_key)
            elif self.base_url:
                # Use base URL with default auth
                self._client = AtlanClient(base_url=self.base_url)
            else:
                # Use default client from environment
                self._client = AtlanClient()
        return self._client

    async def validate_glossary_exists(self, glossary_qn: str) -> bool:
        """Check if a glossary exists in Atlan."""
        try:
            search = (
                FluentSearch()
                .where(FluentSearch.QUALIFIED_NAME.eq(glossary_qn))
                .where(FluentSearch.TYPE_NAME.eq("AtlasGlossary"))
                .page_size(1)
            )
            results = self.client.asset.search(search)
            return results.count > 0
        except Exception as e:
            logger.error(f"Error validating glossary: {e}")
            return False

    async def fetch_assets_with_descriptions(
        self,
        asset_types: List[str],
        max_results: int = 100,
        min_popularity: float = 0.0
    ) -> List[AssetMetadata]:
        """Fetch assets that have descriptions from Atlan."""
        assets = []

        try:
            # Build search for SQL assets with descriptions
            search = (
                FluentSearch()
                .where(FluentSearch.SUPER_TYPE_NAMES.eq("SQL"))
                .where(FluentSearch.TYPE_NAME.within(asset_types))
                .page_size(min(max_results, 100))
            )

            results = self.client.asset.search(search)

            for asset in results:
                if len(assets) >= max_results:
                    break

                metadata = self._convert_to_asset_metadata(asset)
                if metadata:
                    assets.append(metadata)

            logger.info(f"Fetched {len(assets)} assets from Atlan")
            return assets

        except Exception as e:
            logger.error(f"Error fetching assets: {e}")
            return assets

    def _convert_to_asset_metadata(self, asset: Asset) -> Optional[AssetMetadata]:
        """Convert an Atlan asset to our metadata model."""
        try:
            columns = []
            if hasattr(asset, "columns") and asset.columns:
                for col in asset.columns:
                    columns.append(ColumnMetadata(
                        name=col.name,
                        data_type=getattr(col, "data_type", None),
                        description=getattr(col, "description", None),
                        is_primary_key=getattr(col, "is_primary", False),
                        is_foreign_key=getattr(col, "is_foreign", False),
                        is_nullable=getattr(col, "is_nullable", True),
                    ))

            return AssetMetadata(
                qualified_name=asset.qualified_name,
                name=asset.name,
                type_name=asset.type_name,
                description=getattr(asset, "description", None),
                user_description=getattr(asset, "user_description", None),
                columns=columns,
                popularity_score=getattr(asset, "popularity_score", 0.0) or 0.0,
                view_count=getattr(asset, "view_count", 0) or 0,
                tags=[t.type_name for t in (asset.atlan_tags or [])],
                owner=getattr(asset, "owner_users", [None])[0] if getattr(asset, "owner_users", None) else None,
                database_name=getattr(asset, "database_name", None),
                schema_name=getattr(asset, "schema_name", None),
            )
        except Exception as e:
            logger.warning(f"Error converting asset {getattr(asset, 'name', 'unknown')}: {e}")
            return None

    async def create_glossary_term(
        self,
        term_draft: GlossaryTermDraft,
        glossary_qn: str
    ) -> Optional[str]:
        """Create a glossary term in Atlan from a draft."""
        try:
            term = AtlasGlossaryTerm.creator(
                name=term_draft.name,
                anchor=AtlasGlossary.ref_by_qualified_name(glossary_qn),
            )

            # Set the definition
            definition = term_draft.get_final_definition()
            term.description = definition

            # Set short description if available
            if term_draft.short_description:
                term.user_description = term_draft.short_description

            # Save the term
            response = self.client.asset.save(term)

            if response and response.assets_created(AtlasGlossaryTerm):
                created_term = response.assets_created(AtlasGlossaryTerm)[0]
                logger.info(f"Created glossary term: {created_term.qualified_name}")
                return created_term.qualified_name

            return None

        except Exception as e:
            logger.error(f"Error creating glossary term {term_draft.name}: {e}")
            return None

    async def get_glossary_terms(self, glossary_qn: str) -> List[str]:
        """Get existing term names in a glossary to avoid duplicates."""
        try:
            search = (
                FluentSearch()
                .where(FluentSearch.TYPE_NAME.eq("AtlasGlossaryTerm"))
                .where(AtlasGlossaryTerm.ANCHOR.eq(glossary_qn))
                .page_size(1000)
            )

            results = self.client.asset.search(search)
            return [asset.name for asset in results]

        except Exception as e:
            logger.error(f"Error fetching existing terms: {e}")
            return []
