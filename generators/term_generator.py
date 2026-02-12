"""Term generator orchestrating LLM calls for glossary generation."""

import logging
import asyncio
from typing import Dict, List, Optional
from uuid import uuid4

from app.models import AssetMetadata, ColumnClassification, ColumnMetadata, GlossaryTermDraft, TermStatus, TermType, UsageSignals
from clients.llm_client import ClaudeClient
from generators.context_builder import ContextBuilder
from generators.prompts import PromptTemplates

logger = logging.getLogger(__name__)


class TermGenerator:
    """Orchestrates LLM-based generation of glossary terms."""

    def __init__(
        self,
        llm_client: Optional[ClaudeClient] = None,
        context_builder: Optional[ContextBuilder] = None,
        batch_size: int = 5,
        max_concurrent: int = 3,
    ):
        self.llm_client = llm_client or ClaudeClient()
        self.context_builder = context_builder or ContextBuilder()
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def generate_term(
        self,
        asset: AssetMetadata,
        usage: Optional[UsageSignals] = None,
        target_glossary_qn: str = "",
        custom_context: Optional[str] = None,
    ) -> Optional[GlossaryTermDraft]:
        """Generate a single glossary term from an asset."""

        async with self._semaphore:
            try:
                context = self.context_builder.build_asset_context(asset, usage)
                context = self.context_builder.truncate_context(context)

                # Generate using LLM
                result = await self.llm_client.generate_term_definition(
                    asset_name=asset.name,
                    asset_type=asset.type_name,
                    description=context.get("description"),
                    columns=context.get("columns"),
                    usage_stats=context.get("usage_stats"),
                    sql_definition=context.get("sql_definition"),
                    dbt_context=context.get("dbt_context"),
                    custom_context=custom_context,
                )

                # Create draft from result
                draft = GlossaryTermDraft(
                    id=str(uuid4()),
                    name=result.get("name", asset.name),
                    definition=result.get("definition", ""),
                    short_description=result.get("short_description"),
                    examples=result.get("examples", []),
                    synonyms=result.get("synonyms", []),
                    source_assets=[asset.qualified_name],
                    confidence=result.get("confidence", "medium"),
                    status=TermStatus.PENDING_REVIEW,
                    term_type=TermType.BUSINESS_TERM,
                    target_glossary_qn=target_glossary_qn,
                    query_frequency=usage.query_frequency if usage else asset.query_count,
                    user_access_count=usage.unique_users if usage else asset.user_count,
                )

                logger.info(f"Generated term: {draft.name} (confidence: {draft.confidence})")
                return draft

            except Exception as e:
                logger.error(f"Error generating term for {asset.name}: {e}")
                return None

    async def generate_terms_batch(
        self,
        assets: List[AssetMetadata],
        usage_signals: Dict[str, UsageSignals],
        target_glossary_qn: str,
        custom_context: Optional[str] = None,
    ) -> List[GlossaryTermDraft]:
        """Generate terms for a batch of assets concurrently."""

        tasks = []
        for asset in assets:
            usage = usage_signals.get(asset.qualified_name)
            task = self.generate_term(asset, usage, target_glossary_qn, custom_context=custom_context)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        drafts = []
        for result in results:
            if isinstance(result, GlossaryTermDraft):
                drafts.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Batch generation error: {result}")

        return drafts

    async def generate_all_terms(
        self,
        assets: List[AssetMetadata],
        usage_signals: Dict[str, UsageSignals],
        target_glossary_qn: str,
        existing_term_names: Optional[set] = None,
        custom_context: Optional[str] = None,
    ) -> List[GlossaryTermDraft]:
        """Generate terms for all assets in batches with deduplication."""

        existing_lower = {n.lower() for n in (existing_term_names or set())}
        generated_names: set = set()
        all_drafts = []
        skipped_existing = 0
        skipped_duplicate = 0

        # Process in batches
        for i in range(0, len(assets), self.batch_size):
            batch = assets[i : i + self.batch_size]

            # Pre-generation dedup: skip assets whose name already exists in glossary
            filtered_batch = []
            for asset in batch:
                if asset.name.lower() in existing_lower:
                    logger.info(f"Pre-gen dedup: skipping '{asset.name}' (already exists in glossary)")
                    skipped_existing += 1
                    continue
                filtered_batch.append(asset)

            if not filtered_batch:
                continue

            logger.info(f"Processing batch {i // self.batch_size + 1}: {len(filtered_batch)} assets (after pre-gen dedup)")

            batch_drafts = await self.generate_terms_batch(
                filtered_batch, usage_signals, target_glossary_qn, custom_context=custom_context
            )

            # Within-batch dedup: skip terms with duplicate names
            for draft in batch_drafts:
                name_lower = draft.name.lower()
                if name_lower in existing_lower or name_lower in generated_names:
                    logger.info(f"Within-batch dedup: skipping duplicate term '{draft.name}'")
                    skipped_duplicate += 1
                    continue
                generated_names.add(name_lower)
                all_drafts.append(draft)

            # Small delay between batches to avoid rate limiting
            if i + self.batch_size < len(assets):
                await asyncio.sleep(1)

        if skipped_existing > 0:
            logger.info(f"Pre-gen dedup: skipped {skipped_existing} assets matching existing terms")
        if skipped_duplicate > 0:
            logger.info(f"Within-batch dedup: skipped {skipped_duplicate} duplicate term names")
        logger.info(f"Generated {len(all_drafts)} unique terms total")
        return all_drafts

    async def classify_asset_columns(
        self,
        asset: AssetMetadata,
    ) -> List[ColumnClassification]:
        """Classify columns in an asset to determine which deserve glossary terms."""

        if not asset.columns:
            return []

        # Build column data for the prompt
        columns_data = [
            {
                "name": col.name,
                "data_type": col.data_type,
                "description": col.description,
                "is_primary_key": col.is_primary_key,
                "is_foreign_key": col.is_foreign_key,
            }
            for col in asset.columns
        ]

        description = asset.description or asset.user_description

        try:
            raw_results = await self.llm_client.classify_columns(
                asset_name=asset.name,
                asset_type=asset.type_name,
                description=description,
                columns=columns_data,
            )

            classifications = []
            for item in raw_results:
                try:
                    classification = ColumnClassification(
                        column_name=item["column_name"],
                        term_type=TermType(item["term_type"]),
                        should_generate=item.get("should_generate", False),
                        reason=item.get("reason"),
                    )
                    classifications.append(classification)
                except (KeyError, ValueError) as e:
                    logger.warning(f"Skipping invalid classification entry: {e}")
                    continue

            selected = sum(1 for c in classifications if c.should_generate)
            logger.info(f"{selected}/{len(classifications)} columns selected for term generation in {asset.name}")
            return classifications

        except Exception as e:
            logger.error(f"Error classifying columns for {asset.name}: {e}")
            return []

    async def generate_column_term(
        self,
        asset: AssetMetadata,
        column: ColumnMetadata,
        term_type: TermType,
        usage: Optional[UsageSignals] = None,
        target_glossary_qn: str = "",
        custom_context: Optional[str] = None,
    ) -> Optional[GlossaryTermDraft]:
        """Generate a single glossary term for a specific column."""

        async with self._semaphore:
            try:
                context = self.context_builder.build_column_context(
                    asset, column, term_type, usage
                )

                result = await self.llm_client.generate_column_term_definition(
                    column_name=context["column_name"],
                    column_data_type=context.get("column_data_type"),
                    column_description=context.get("column_description"),
                    term_type=context["term_type"],
                    parent_asset_name=context.get("parent_asset_name"),
                    parent_asset_type=context.get("parent_asset_type"),
                    parent_description=context.get("parent_description"),
                    sibling_columns=context.get("sibling_columns"),
                    sql_definition=context.get("sql_definition"),
                    custom_context=custom_context,
                )

                draft = GlossaryTermDraft(
                    id=str(uuid4()),
                    name=result.get("name", column.name),
                    definition=result.get("definition", ""),
                    short_description=result.get("short_description"),
                    examples=result.get("examples", []),
                    synonyms=result.get("synonyms", []),
                    source_assets=[asset.qualified_name],
                    confidence=result.get("confidence", "medium"),
                    status=TermStatus.PENDING_REVIEW,
                    term_type=term_type,
                    source_column=column.name,
                    target_glossary_qn=target_glossary_qn,
                    query_frequency=usage.query_frequency if usage else asset.query_count,
                    user_access_count=usage.unique_users if usage else asset.user_count,
                )

                logger.info(f"Generated column term: {draft.name} (type: {term_type.value}, confidence: {draft.confidence})")
                return draft

            except Exception as e:
                logger.error(f"Error generating column term for {column.name} in {asset.name}: {e}")
                return None

    async def generate_column_terms_for_asset(
        self,
        asset: AssetMetadata,
        classifications: List[ColumnClassification],
        usage: Optional[UsageSignals] = None,
        target_glossary_qn: str = "",
        custom_context: Optional[str] = None,
    ) -> List[GlossaryTermDraft]:
        """Generate terms for all classified columns in an asset concurrently."""

        # Filter to only columns that should have terms generated
        to_generate = [c for c in classifications if c.should_generate]

        if not to_generate:
            return []

        # Build a lookup for column metadata
        col_lookup = {col.name: col for col in asset.columns}

        tasks = []
        for classification in to_generate:
            column = col_lookup.get(classification.column_name)
            if not column:
                logger.warning(f"Column '{classification.column_name}' not found in asset {asset.name}")
                continue

            task = self.generate_column_term(
                asset=asset,
                column=column,
                term_type=classification.term_type,
                usage=usage,
                target_glossary_qn=target_glossary_qn,
                custom_context=custom_context,
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        drafts = []
        for result in results:
            if isinstance(result, GlossaryTermDraft):
                drafts.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Column term generation error: {result}")

        return drafts
