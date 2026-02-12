"""MDLH (Metadata Lake House) client for supplemental Snowflake queries."""

import os
import logging
from typing import Dict, List, Optional

from app.models import AssetMetadata, ColumnMetadata

logger = logging.getLogger(__name__)


class MDLHClient:
    """Client for querying Atlan's MDLH Snowflake tables.

    Can be used as primary data source (fetch_assets_with_descriptions)
    or for enrichment (enrich_assets) with lineage (BASE_EDGES) and
    additional metadata (ASSETS) from the MDLH gold layer.
    Uses externalbrowser (SSO) authentication.
    """

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
    ):
        from app.settings_store import load_settings

        settings = load_settings()

        self.account = account or settings.snowflake_account or os.environ.get("SNOWFLAKE_ACCOUNT")
        self.user = user or settings.snowflake_user or os.environ.get("SNOWFLAKE_USER")
        self.warehouse = warehouse or settings.snowflake_warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE")
        self.database = database or settings.snowflake_database or os.environ.get("SNOWFLAKE_DATABASE", "MDLH_GOLD_RKO")
        self.schema = schema or settings.snowflake_schema or os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.role = role or settings.snowflake_role or os.environ.get("SNOWFLAKE_ROLE")
        self._conn = None

    @property
    def is_configured(self) -> bool:
        """Check if minimum required settings are present."""
        return bool(self.account and self.user)

    def _get_connection(self):
        """Get or create a Snowflake connection using externalbrowser SSO."""
        if self._conn is None or self._conn.is_closed():
            import snowflake.connector

            connect_params = {
                "account": self.account,
                "user": self.user,
                "authenticator": "externalbrowser",
                "database": self.database,
                "schema": self.schema,
            }
            if self.warehouse:
                connect_params["warehouse"] = self.warehouse
            if self.role:
                connect_params["role"] = self.role

            self._conn = snowflake.connector.connect(**connect_params)
        return self._conn

    def close(self):
        """Close the Snowflake connection."""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            self._conn = None

    def test_connection(self) -> dict:
        """Test the Snowflake connection. Returns success/error dict."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            row = cursor.fetchone()
            cursor.close()
            return {
                "success": True,
                "user": row[0],
                "role": row[1],
                "warehouse": row[2],
            }
        except Exception as e:
            logger.error(f"MDLH connection test failed: {e}")
            return {"success": False, "error": str(e)}

    async def fetch_assets_with_descriptions(
        self,
        asset_types: List[str],
        max_results: int = 100,
        min_popularity: float = 0.0,
        connection_qualified_name: Optional[str] = None,
    ) -> List[AssetMetadata]:
        """Fetch SQL assets directly from MDLH Snowflake tables.
        
        This method queries MDLH as the PRIMARY data source, not enrichment.
        """
        if not self.is_configured:
            logger.warning("MDLH not configured, returning empty list")
            return []

        assets = []
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Map asset types to MDLH type names
            type_conditions = []
            for asset_type in asset_types:
                if asset_type == "Table":
                    type_conditions.append("ASSET_TYPE = 'Table'")
                elif asset_type == "View":
                    type_conditions.append("ASSET_TYPE = 'View'")
                elif asset_type == "MaterializedView":
                    type_conditions.append("ASSET_TYPE = 'MaterialisedView'")

            if not type_conditions:
                logger.warning(f"No supported asset types in: {asset_types}")
                return []

            type_filter = " OR ".join(type_conditions)

            # Build query
            query = f"""
                SELECT
                    ASSET_QUALIFIED_NAME,
                    ASSET_NAME,
                    ASSET_TYPE,
                    DESCRIPTION,
                    COALESCE(POPULARITY_SCORE, 0) AS POPULARITY_SCORE,
                    CONNECTOR_NAME,
                    CONNECTION_QUALIFIED_NAME,
                    DATABASE_NAME,
                    SCHEMA_NAME,
                    OWNER_USERS,
                    GUID
                FROM {self.database}.{self.schema}.ASSETS
                WHERE ({type_filter})
                  AND DESCRIPTION IS NOT NULL
                  AND DESCRIPTION != ''
                  AND COALESCE(POPULARITY_SCORE, 0) >= %s
            """
            
            params = [min_popularity]
            
            # Add connection filter if specified
            if connection_qualified_name:
                query += " AND CONNECTION_QUALIFIED_NAME = %s"
                params.append(connection_qualified_name)
            
            query += f" ORDER BY POPULARITY_SCORE DESC LIMIT {max_results}"
            
            logger.info(f"Executing MDLH query with types: {asset_types}, min_popularity: {min_popularity}")
            cursor.execute(query, params)
            
            # Fetch all results
            rows = cursor.fetchall()
            logger.info(f"MDLH returned {len(rows)} assets")
            
            # Get column names for reference
            column_names = [desc[0] for desc in cursor.description]
            
            for row in rows:
                row_dict = dict(zip(column_names, row))
                
                asset = AssetMetadata(
                    qualified_name=row_dict["ASSET_QUALIFIED_NAME"],
                    name=row_dict["ASSET_NAME"],
                    asset_type=row_dict["ASSET_TYPE"],
                    description=row_dict["DESCRIPTION"],
                    popularity_score=float(row_dict["POPULARITY_SCORE"]) if row_dict["POPULARITY_SCORE"] else 0.0,
                    connector_name=row_dict["CONNECTOR_NAME"],
                    connection_qualified_name=row_dict["CONNECTION_QUALIFIED_NAME"],
                    database_name=row_dict["DATABASE_NAME"],
                    schema_name=row_dict["SCHEMA_NAME"],
                    owner=row_dict["OWNER_USERS"][0] if row_dict["OWNER_USERS"] else None,
                )
                assets.append(asset)
            
            cursor.close()
            
            # Fetch columns for these assets
            if assets:
                assets = self._enrich_with_columns(assets)
            
            # Fetch lineage for these assets
            if assets:
                qualified_names = [a.qualified_name for a in assets]
                lineage = self.fetch_lineage(qualified_names)
                for asset in assets:
                    if asset.qualified_name in lineage:
                        asset.upstream_assets = lineage[asset.qualified_name].get("upstream", [])
                        asset.downstream_assets = lineage[asset.qualified_name].get("downstream", [])
            
            logger.info(f"Fetched {len(assets)} assets from MDLH with columns and lineage")
            return assets

        except Exception as e:
            logger.error(f"Error fetching assets from MDLH: {e}", exc_info=True)
            return []

    def _enrich_with_columns(self, assets: List[AssetMetadata]) -> List[AssetMetadata]:
        """Fetch and attach column metadata for assets."""
        if not assets:
            return assets
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get all table qualified names
            table_qns = [a.qualified_name for a in assets]
            placeholders = ", ".join(["%s"] * len(table_qns))
            
            # Query columns table (assuming it exists in MDLH)
            query = f"""
                SELECT
                    c.TABLE_QUALIFIED_NAME,
                    c.NAME AS COLUMN_NAME,
                    c.DATA_TYPE,
                    c.DESCRIPTION
                FROM {self.database}.{self.schema}.COLUMN c
                WHERE c.TABLE_QUALIFIED_NAME IN ({placeholders})
                ORDER BY c.TABLE_QUALIFIED_NAME, c.ORDER
            """
            
            cursor.execute(query, table_qns)
            
            # Group columns by table
            columns_by_table: Dict[str, List[ColumnMetadata]] = {}
            for row in cursor:
                table_qn = row[0]
                col = ColumnMetadata(
                    name=row[1],
                    data_type=row[2],
                    description=row[3],
                )
                if table_qn not in columns_by_table:
                    columns_by_table[table_qn] = []
                columns_by_table[table_qn].append(col)
            
            cursor.close()
            
            # Attach columns to assets
            for asset in assets:
                if asset.qualified_name in columns_by_table:
                    asset.columns = columns_by_table[asset.qualified_name]
                    
            logger.info(f"Enriched {len(columns_by_table)} assets with column metadata")
            
        except Exception as e:
            logger.warning(f"Could not fetch columns from MDLH (continuing without): {e}")
        
        return assets

    def fetch_asset_details(self, qualified_names: List[str]) -> Dict[str, dict]:
        """Fetch asset details from ASSETS table by qualified name.

        Returns a dict mapping qualified_name to asset details (popularity, tags, etc.).
        """
        if not qualified_names or not self.is_configured:
            return {}

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            placeholders = ", ".join(["%s"] * len(qualified_names))
            query = f"""
                SELECT
                    ASSET_QUALIFIED_NAME,
                    COALESCE(POPULARITY_SCORE, 0) AS POPULARITY_SCORE,
                    HAS_LINEAGE,
                    TAGS,
                    OWNER_USERS
                FROM {self.database}.{self.schema}.ASSETS
                WHERE ASSET_QUALIFIED_NAME IN ({placeholders})
            """
            cursor.execute(query, qualified_names)

            details: Dict[str, dict] = {}
            for row in cursor:
                details[row[0]] = {
                    "popularity_score": float(row[1]) if row[1] else 0.0,
                    "has_lineage": row[2],
                    "tags": row[3] if row[3] else [],
                    "owner_users": row[4] if row[4] else [],
                }

            cursor.close()
            return details

        except Exception as e:
            logger.error(f"Error fetching asset details from MDLH: {e}")
            return {}

    def fetch_lineage(self, qualified_names: List[str]) -> Dict[str, dict]:
        """Fetch upstream and downstream lineage from BASE_EDGES joined with ASSETS.

        Returns a dict mapping each qualified_name to:
          {"upstream": [list of upstream qualified names],
           "downstream": [list of downstream qualified names]}
        """
        if not qualified_names or not self.is_configured:
            return {}

        result: Dict[str, dict] = {qn: {"upstream": [], "downstream": []} for qn in qualified_names}

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            placeholders = ", ".join(["%s"] * len(qualified_names))

            # Upstream: assets that feed INTO the target assets
            upstream_query = f"""
                SELECT
                    target.ASSET_QUALIFIED_NAME AS target_qn,
                    source.ASSET_QUALIFIED_NAME AS source_qn
                FROM {self.database}.{self.schema}.BASE_EDGES e
                JOIN {self.database}.{self.schema}.ASSETS target ON target.GUID = e.OUTPUT_GUID
                JOIN {self.database}.{self.schema}.ASSETS source ON source.GUID = e.INPUT_GUID
                WHERE target.ASSET_QUALIFIED_NAME IN ({placeholders})
            """
            cursor.execute(upstream_query, qualified_names)
            for row in cursor:
                target_qn, source_qn = row[0], row[1]
                if target_qn in result:
                    result[target_qn]["upstream"].append(source_qn)

            # Downstream: assets that the target assets feed INTO
            downstream_query = f"""
                SELECT
                    source.ASSET_QUALIFIED_NAME AS source_qn,
                    target.ASSET_QUALIFIED_NAME AS target_qn
                FROM {self.database}.{self.schema}.BASE_EDGES e
                JOIN {self.database}.{self.schema}.ASSETS source ON source.GUID = e.INPUT_GUID
                JOIN {self.database}.{self.schema}.ASSETS target ON target.GUID = e.OUTPUT_GUID
                WHERE source.ASSET_QUALIFIED_NAME IN ({placeholders})
            """
            cursor.execute(downstream_query, qualified_names)
            for row in cursor:
                source_qn, target_qn = row[0], row[1]
                if source_qn in result:
                    result[source_qn]["downstream"].append(target_qn)

            cursor.close()
            return result

        except Exception as e:
            logger.error(f"Error fetching lineage from MDLH: {e}")
            return {}

    def enrich_assets(self, assets: List[AssetMetadata]) -> List[AssetMetadata]:
        """Enrich a list of AssetMetadata with MDLH data (lineage + asset details).

        Modifies assets in-place and returns them. Returns assets unchanged
        if MDLH is not configured or on error.
        """
        if not self.is_configured or not assets:
            return assets

        qualified_names = [a.qualified_name for a in assets]

        # Fetch asset details from ASSETS table
        try:
            details = self.fetch_asset_details(qualified_names)
        except Exception:
            details = {}

        # Fetch lineage from BASE_EDGES
        try:
            lineage = self.fetch_lineage(qualified_names)
        except Exception:
            lineage = {}

        # Enrich each asset
        for asset in assets:
            qn = asset.qualified_name

            # Supplement popularity score (fill-in, not override)
            if qn in details:
                mdlh_detail = details[qn]
                if asset.popularity_score == 0.0 and mdlh_detail.get("popularity_score", 0) > 0:
                    asset.popularity_score = mdlh_detail["popularity_score"]

            # Add lineage
            if qn in lineage:
                asset.upstream_assets = lineage[qn].get("upstream", [])
                asset.downstream_assets = lineage[qn].get("downstream", [])

        logger.info(
            f"Enriched {len(assets)} assets with MDLH data "
            f"(details: {len(details)}, lineage: {len(lineage)})"
        )
        return assets
