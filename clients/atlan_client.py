"""Atlan client wrapper for metadata operations."""

import os
import logging
from typing import List, Optional
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import (
    AtlasGlossary,
    AtlasGlossaryCategory,
    AtlasGlossaryTerm,
    Asset,
    Connection,
    Table,
    View,
    DbtModel,
)
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.enums import AtlanConnectorType

from app.models import AssetMetadata, ColumnMetadata, GlossaryTermDraft

logger = logging.getLogger(__name__)


class AtlanMetadataClient:
    """Client for interacting with Atlan metadata catalog."""

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        # Load settings from persistent store (file + Dapr)
        from app.settings_store import load_settings
        settings = load_settings()

        self.base_url = base_url or settings.atlan_base_url or os.environ.get("ATLAN_BASE_URL")
        self.api_key = api_key or settings.atlan_api_key or os.environ.get("ATLAN_API_KEY")
        self._client: Optional[AtlanClient] = None
        self._category_cache: dict = {}  # (glossary_qn, category_name) -> category_qn

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
            glossary = self.client.asset.get_by_qualified_name(
                qualified_name=glossary_qn,
                asset_type=AtlasGlossary,
            )
            return glossary is not None
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
                .where(Asset.SUPER_TYPE_NAMES.eq("SQL"))
                .where(Asset.TYPE_NAME.within(asset_types))
                .include_on_results(View.DEFINITION)
                .include_on_results(Table.TABLE_DEFINITION)
                .page_size(min(max_results, 100))
            )

            results = self.client.asset.search(search.to_request())

            for asset in results:
                if len(assets) >= max_results:
                    break

                metadata = self._convert_to_asset_metadata(asset)
                if metadata:
                    assets.append(metadata)

            logger.info(f"Fetched {len(assets)} assets from Atlan")

            # Enrich with dbt model metadata
            assets = await self.fetch_dbt_models_for_assets(assets)

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

            # Extract SQL definition (View.definition or Table.table_definition)
            sql_definition = getattr(asset, "definition", None) or getattr(asset, "table_definition", None)

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
                sql_definition=sql_definition,
            )
        except Exception as e:
            logger.warning(f"Error converting asset {getattr(asset, 'name', 'unknown')}: {e}")
            return None

    async def fetch_dbt_models_for_assets(self, assets: List[AssetMetadata]) -> List[AssetMetadata]:
        """Fetch dbt model metadata linked to SQL assets and enrich AssetMetadata objects."""
        if not assets:
            return assets

        qualified_names = [a.qualified_name for a in assets]
        qn_to_asset = {a.qualified_name: a for a in assets}

        try:
            search = (
                FluentSearch()
                .where(DbtModel.TYPE_NAME.eq("DbtModel"))
                .where(DbtModel.SQL_ASSETS.within(qualified_names))
                .include_on_results(DbtModel.DBT_RAW_SQL)
                .include_on_results(DbtModel.DBT_COMPILED_SQL)
                .include_on_results(DbtModel.DBT_MATERIALIZATION_TYPE)
                .page_size(min(len(qualified_names), 100))
            )

            results = self.client.asset.search(search.to_request())
            enriched_count = 0

            for dbt_model in results:
                # Get linked SQL asset qualified names
                sql_assets = getattr(dbt_model, "sql_assets", None) or []
                for sql_asset in sql_assets:
                    sql_qn = getattr(sql_asset, "qualified_name", None)
                    if sql_qn and sql_qn in qn_to_asset:
                        asset = qn_to_asset[sql_qn]
                        asset.dbt_model_name = dbt_model.name
                        asset.dbt_raw_sql = getattr(dbt_model, "dbt_raw_sql", None)
                        asset.dbt_compiled_sql = getattr(dbt_model, "dbt_compiled_sql", None)
                        asset.dbt_materialization_type = getattr(dbt_model, "dbt_materialization_type", None)
                        enriched_count += 1

            logger.info(f"Enriched {enriched_count} assets with dbt model metadata")

        except Exception as e:
            logger.warning(f"Could not fetch dbt models (continuing without): {e}")

        return assets

    # Term type to category name mapping
    TERM_TYPE_CATEGORY_MAP = {
        "business_term": "Business Terms",
        "metric": "Metrics",
        "dimension": "Dimensions",
    }

    async def get_or_create_category(
        self,
        glossary_qn: str,
        category_name: str,
    ) -> Optional[str]:
        """Get or create a glossary category, with caching within a run."""
        cache_key = (glossary_qn, category_name)
        if cache_key in self._category_cache:
            return self._category_cache[cache_key]

        try:
            # Search for existing category by name within the glossary
            search = (
                FluentSearch()
                .where(AtlasGlossaryCategory.TYPE_NAME.eq("AtlasGlossaryCategory"))
                .where(AtlasGlossaryCategory.ANCHOR.eq(glossary_qn))
                .page_size(100)
            )

            results = self.client.asset.search(search.to_request())
            for cat in results:
                if cat.name == category_name:
                    cat_qn = cat.qualified_name
                    self._category_cache[cache_key] = cat_qn
                    logger.info(f"Found existing category: {category_name} ({cat_qn})")
                    return cat_qn

            # Not found — create it
            category = AtlasGlossaryCategory.creator(
                name=category_name,
                anchor=AtlasGlossary.ref_by_qualified_name(glossary_qn),
            )

            response = self.client.asset.save(category)
            if response and response.assets_created(AtlasGlossaryCategory):
                created = response.assets_created(AtlasGlossaryCategory)[0]
                cat_qn = created.qualified_name
                self._category_cache[cache_key] = cat_qn
                logger.info(f"Created category: {category_name} ({cat_qn})")
                return cat_qn

            return None

        except Exception as e:
            logger.error(f"Error getting/creating category '{category_name}': {e}")
            return None

    async def create_glossary_term(
        self,
        term_draft: GlossaryTermDraft,
        glossary_qn: str,
        term_type: Optional[str] = None,
    ) -> Optional[str]:
        """Create a glossary term in Atlan from a draft, optionally assigning to a category."""
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

            # Assign to category based on term type
            effective_type = term_type or getattr(term_draft, 'term_type', None)
            if effective_type:
                type_value = effective_type.value if hasattr(effective_type, 'value') else str(effective_type)
                category_name = self.TERM_TYPE_CATEGORY_MAP.get(type_value)
                if category_name:
                    cat_qn = await self.get_or_create_category(glossary_qn, category_name)
                    if cat_qn:
                        term.categories = [AtlasGlossaryCategory.ref_by_qualified_name(cat_qn)]

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

    async def create_glossary(self, name: str, description: Optional[str] = None) -> dict:
        """Create a new glossary in Atlan."""
        try:
            glossary = AtlasGlossary.creator(name=name)
            if description:
                glossary.description = description
            response = self.client.asset.save(glossary)
            if response and response.assets_created(AtlasGlossary):
                created = response.assets_created(AtlasGlossary)[0]
                logger.info(f"Created glossary: {created.qualified_name}")
                return {
                    "name": created.name,
                    "qualified_name": created.qualified_name,
                    "description": description,
                }
            raise ValueError("Glossary creation returned no created assets")
        except Exception as e:
            logger.error(f"Error creating glossary '{name}': {e}")
            raise

    async def get_glossary_terms(self, glossary_qn: str) -> List[str]:
        """Get existing term names in a glossary to avoid duplicates."""
        try:
            search = (
                FluentSearch()
                .where(AtlasGlossaryTerm.TYPE_NAME.eq("AtlasGlossaryTerm"))
                .where(AtlasGlossaryTerm.ANCHOR.eq(glossary_qn))
                .page_size(1000)
            )

            results = self.client.asset.search(search.to_request())
            return [asset.name for asset in results]

        except Exception as e:
            logger.error(f"Error fetching existing terms: {e}")
            return []

    async def get_all_glossaries(self) -> List[dict]:
        """Fetch all glossaries from Atlan."""
        try:
            search = (
                FluentSearch()
                .where(AtlasGlossary.TYPE_NAME.eq("AtlasGlossary"))
                .page_size(100)
            )

            results = self.client.asset.search(search.to_request())
            glossaries = []

            for glossary in results:
                glossaries.append({
                    "name": glossary.name,
                    "qualified_name": glossary.qualified_name,
                    "description": getattr(glossary, "description", None),
                })

            logger.info(f"Fetched {len(glossaries)} glossaries from Atlan")
            return glossaries

        except Exception as e:
            logger.error(f"Error fetching glossaries: {e}")
            return []

    async def get_all_connections(self, connector_type: Optional[str] = None) -> List[dict]:
        """Fetch all connections from Atlan, optionally filtered by connector type."""
        try:
            search = FluentSearch().where(Connection.TYPE_NAME.eq("Connection"))
            search = search.page_size(100)

            results = self.client.asset.search(search.to_request())
            connections = []

            for conn in results:
                # Extract connector type from qualified name (format: default/{connector}/{id})
                qn_parts = conn.qualified_name.split("/")
                connector_name = qn_parts[1] if len(qn_parts) > 1 else "unknown"

                # Filter by connector type if specified
                if connector_type and connector_name.lower() != connector_type.lower():
                    continue

                connections.append({
                    "name": conn.name,
                    "qualified_name": conn.qualified_name,
                    "connector_name": connector_name,
                    "status": getattr(conn, "connection_status", None),
                })

            logger.info(f"Fetched {len(connections)} connections from Atlan" +
                       (f" for connector {connector_type}" if connector_type else ""))
            return connections

        except Exception as e:
            logger.error(f"Error fetching connections: {e}")
            return []

    async def get_connector_types(self) -> List[dict]:
        """Get all unique connector types from connections."""
        try:
            search = (
                FluentSearch()
                .where(Connection.TYPE_NAME.eq("Connection"))
                .page_size(100)
            )

            results = self.client.asset.search(search.to_request())
            connector_types = set()

            for conn in results:
                # Extract connector type from qualified name (format: default/{connector}/{id})
                qn_parts = conn.qualified_name.split("/")
                if len(qn_parts) > 1:
                    connector_types.add(qn_parts[1])

            # Convert to list of dicts with display names
            connectors = []
            for conn_type in sorted(connector_types):
                # Capitalize for display
                display_name = conn_type.replace("-", " ").title()
                connectors.append({
                    "value": conn_type,
                    "label": display_name,
                })

            logger.info(f"Found {len(connectors)} connector types")
            return connectors

        except Exception as e:
            logger.error(f"Error fetching connector types: {e}")
            return []
