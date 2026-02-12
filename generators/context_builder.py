"""Context builder for aggregating metadata into LLM prompts."""

from typing import Dict, List, Optional

from app.models import AssetMetadata, ColumnMetadata, TermType, UsageSignals


class ContextBuilder:
    """Builds context for LLM prompts from asset metadata."""

    def __init__(self, max_columns: int = 20, max_context_length: int = 4000):
        self.max_columns = max_columns
        self.max_context_length = max_context_length

    def build_asset_context(
        self,
        asset: AssetMetadata,
        usage: Optional[UsageSignals] = None
    ) -> dict:
        """Build context dictionary for a single asset."""

        context = {
            "name": asset.name,
            "type": asset.type_name,
            "qualified_name": asset.qualified_name,
        }

        # Add descriptions
        if asset.description:
            context["description"] = asset.description
        elif asset.user_description:
            context["description"] = asset.user_description

        # Add column information
        if asset.columns:
            context["columns"] = [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "description": col.description,
                }
                for col in asset.columns[:self.max_columns]
            ]

        # Add usage statistics
        if usage:
            context["usage_stats"] = {
                "query_frequency": usage.query_frequency,
                "unique_users": usage.unique_users,
                "popularity_score": usage.popularity_score,
            }
        else:
            context["usage_stats"] = {
                "query_frequency": asset.query_count,
                "unique_users": asset.user_count,
                "popularity_score": asset.popularity_score,
            }

        # Add additional metadata
        if asset.tags:
            context["tags"] = asset.tags
        if asset.classifications:
            context["classifications"] = asset.classifications
        if asset.database_name:
            context["database"] = asset.database_name
        if asset.schema_name:
            context["schema"] = asset.schema_name

        # Add SQL definition/transformation
        if asset.sql_definition:
            context["sql_definition"] = asset.sql_definition[:1500]

        # Add dbt transformation context
        if asset.dbt_model_name:
            dbt_context = {"model_name": asset.dbt_model_name}
            if asset.dbt_materialization_type:
                dbt_context["materialization_type"] = asset.dbt_materialization_type
            if asset.dbt_raw_sql:
                dbt_context["raw_sql"] = asset.dbt_raw_sql[:2000]
            context["dbt_context"] = dbt_context

        # Add lineage information (from MDLH)
        if asset.upstream_assets:
            context["upstream_assets"] = asset.upstream_assets[:10]
            context["upstream_count"] = len(asset.upstream_assets)
        if asset.downstream_assets:
            context["downstream_assets"] = asset.downstream_assets[:10]
            context["downstream_count"] = len(asset.downstream_assets)

        return context

    def build_column_context(
        self,
        asset: AssetMetadata,
        column: ColumnMetadata,
        term_type: TermType,
        usage: Optional[UsageSignals] = None,
    ) -> dict:
        """Build context dictionary for a single column within an asset."""

        context = {
            "column_name": column.name,
            "column_data_type": column.data_type,
            "column_description": column.description,
            "parent_asset_name": asset.name,
            "parent_asset_type": asset.type_name,
            "parent_description": asset.description or asset.user_description,
            "term_type": term_type.value,
        }

        # Add sibling columns (up to 15, excluding the target column)
        if asset.columns:
            siblings = [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "description": col.description,
                }
                for col in asset.columns[:16]
                if col.name != column.name
            ]
            context["sibling_columns"] = siblings[:15]

        # Add SQL definition (truncated)
        if asset.sql_definition:
            context["sql_definition"] = asset.sql_definition[:1500]

        return context

    def build_batch_context(
        self,
        assets: List[AssetMetadata],
        usage_signals: Dict[str, UsageSignals]
    ) -> List[dict]:
        """Build context for a batch of assets."""

        contexts = []
        for asset in assets:
            usage = usage_signals.get(asset.qualified_name)
            context = self.build_asset_context(asset, usage)
            contexts.append(context)

        return contexts

    def estimate_token_count(self, text: str) -> int:
        """Rough estimation of token count (approximately 4 chars per token)."""
        return len(text) // 4

    def truncate_context(self, context: dict, max_tokens: int = 2000) -> dict:
        """Truncate context to fit within token limits."""

        import json
        serialized = json.dumps(context)

        if self.estimate_token_count(serialized) <= max_tokens:
            return context

        # Progressively remove less important fields
        truncated = context.copy()

        # First, remove dbt SQL (can be large)
        if "dbt_context" in truncated:
            dbt = truncated["dbt_context"]
            if "raw_sql" in dbt:
                del dbt["raw_sql"]
            if not dbt or dbt == {"model_name": truncated.get("dbt_context", {}).get("model_name")}:
                pass  # keep minimal dbt context
            serialized = json.dumps(truncated)
            if self.estimate_token_count(serialized) <= max_tokens:
                return truncated

        # Next, remove SQL definition (large but less structured than columns)
        if "sql_definition" in truncated:
            del truncated["sql_definition"]

        serialized = json.dumps(truncated)
        if self.estimate_token_count(serialized) <= max_tokens:
            return truncated

        # Remove remaining dbt context
        if "dbt_context" in truncated:
            del truncated["dbt_context"]

        serialized = json.dumps(truncated)
        if self.estimate_token_count(serialized) <= max_tokens:
            return truncated

        # Next, remove lineage details (keep counts)
        if "upstream_assets" in truncated:
            del truncated["upstream_assets"]
        if "downstream_assets" in truncated:
            del truncated["downstream_assets"]

        serialized = json.dumps(truncated)
        if self.estimate_token_count(serialized) <= max_tokens:
            return truncated

        # Next, reduce columns
        if "columns" in truncated and len(truncated["columns"]) > 10:
            truncated["columns"] = truncated["columns"][:10]

        serialized = json.dumps(truncated)
        if self.estimate_token_count(serialized) <= max_tokens:
            return truncated

        # Remove column descriptions
        if "columns" in truncated:
            truncated["columns"] = [
                {"name": c["name"], "data_type": c.get("data_type")}
                for c in truncated["columns"]
            ]

        serialized = json.dumps(truncated)
        if self.estimate_token_count(serialized) <= max_tokens:
            return truncated

        # Further reduce columns
        if "columns" in truncated and len(truncated["columns"]) > 5:
            truncated["columns"] = truncated["columns"][:5]

        return truncated
