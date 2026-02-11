"""Term generator orchestrating LLM calls for glossary generation."""

import logging
import asyncio
from typing import Dict, List, Optional
from uuid import uuid4

from app.models import AssetMetadata, GlossaryTermDraft, TermStatus, UsageSignals
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
    ) -> List[GlossaryTermDraft]:
        """Generate terms for a batch of assets concurrently."""

        tasks = []
        for asset in assets:
            usage = usage_signals.get(asset.qualified_name)
            task = self.generate_term(asset, usage, target_glossary_qn)
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
    ) -> List[GlossaryTermDraft]:
        """Generate terms for all assets in batches."""

        all_drafts = []

        # Process in batches
        for i in range(0, len(assets), self.batch_size):
            batch = assets[i : i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1} of {len(batch)} assets")

            batch_drafts = await self.generate_terms_batch(
                batch, usage_signals, target_glossary_qn
            )
            all_drafts.extend(batch_drafts)

            # Small delay between batches to avoid rate limiting
            if i + self.batch_size < len(assets):
                await asyncio.sleep(1)

        logger.info(f"Generated {len(all_drafts)} terms total")
        return all_drafts
