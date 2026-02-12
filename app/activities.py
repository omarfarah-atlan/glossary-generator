"""Workflow activities for glossary generation."""

import os
import logging
import json
from typing import Dict, List, Optional
from temporalio import activity
from dapr.clients import DaprClient

from app.models import (
    AssetMetadata,
    GlossaryTermDraft,
    TermStatus,
    UsageSignals,
    WorkflowConfig,
    BatchResult,
)
from clients.atlan_client import AtlanMetadataClient
from clients.llm_client import ClaudeClient
from clients.mdlh_client import MDLHClient
from clients.usage_client import UsageSignalClient
from generators.term_generator import TermGenerator

logger = logging.getLogger(__name__)

DAPR_STORE_NAME = "statestore"


class GlossaryActivities:
    """Activities for the glossary generation workflow."""

    def __init__(self):
        self._atlan_client: Optional[AtlanMetadataClient] = None
        self._llm_client: Optional[ClaudeClient] = None
        self._mdlh_client: Optional[MDLHClient] = None
        self._usage_client: Optional[UsageSignalClient] = None
        self._term_generator: Optional[TermGenerator] = None

    @property
    def atlan_client(self) -> AtlanMetadataClient:
        if self._atlan_client is None:
            self._atlan_client = AtlanMetadataClient()
        return self._atlan_client

    @property
    def llm_client(self) -> ClaudeClient:
        if self._llm_client is None:
            self._llm_client = ClaudeClient()
        return self._llm_client

    @property
    def mdlh_client(self) -> Optional[MDLHClient]:
        """Lazy init of MDLH client. Returns None if not configured."""
        if self._mdlh_client is None:
            client = MDLHClient()
            if client.is_configured:
                self._mdlh_client = client
            else:
                return None
        return self._mdlh_client

    @property
    def usage_client(self) -> UsageSignalClient:
        if self._usage_client is None:
            self._usage_client = UsageSignalClient()
        return self._usage_client

    @property
    def term_generator(self) -> TermGenerator:
        if self._term_generator is None:
            self._term_generator = TermGenerator(
                llm_client=self.llm_client,
                batch_size=5,
                max_concurrent=3,
            )
        return self._term_generator

    @activity.defn
    async def validate_configuration(self, config_dict: dict) -> dict:
        """Validate the workflow configuration."""
        try:
            config = WorkflowConfig(**config_dict)

            # Validate glossary exists
            exists = await self.atlan_client.validate_glossary_exists(config.target_glossary_qn)

            if not exists:
                return {
                    "valid": False,
                    "error": f"Glossary not found: {config.target_glossary_qn}",
                }

            return {"valid": True, "config": config.model_dump()}

        except Exception as e:
            logger.error(f"Configuration validation error: {e}")
            return {"valid": False, "error": str(e)}

    @activity.defn
    async def fetch_metadata(self, config_dict: dict) -> List[dict]:
        """Fetch asset metadata from MDLH or Atlan (based on USE_MDLH_PRIMARY env var)."""
        try:
            config = WorkflowConfig(**config_dict)
            
            # Check if we should use MDLH as primary source
            use_mdlh_primary = os.environ.get("USE_MDLH_PRIMARY", "false").lower() == "true"
            
            # Try MDLH first if configured and requested
            if use_mdlh_primary:
                mdlh = self.mdlh_client
                if mdlh is not None:
                    try:
                        activity.heartbeat("Fetching assets directly from MDLH (may require SSO login)...")
                        logger.info(f"Using MDLH as PRIMARY data source")
                        assets = await mdlh.fetch_assets_with_descriptions(
                            asset_types=config.asset_types,
                            max_results=config.max_assets,
                            min_popularity=config.min_popularity_score,
                            connection_qualified_name=getattr(config, 'connection_qualified_name', None),
                        )
                        logger.info(f"MDLH returned {len(assets)} assets (primary source)")
                        return [asset.model_dump() for asset in assets]
                    except Exception as e:
                        logger.error(f"MDLH primary fetch failed, falling back to Atlan SDK: {e}")

            # Fall back to Atlan SDK (original approach)
            activity.heartbeat("Fetching assets from Atlan SDK...")
            assets = await self.atlan_client.fetch_assets_with_descriptions(
                asset_types=config.asset_types,
                max_results=config.max_assets,
                min_popularity=config.min_popularity_score,
            )

            # Enrich with MDLH data if configured (when SDK is primary)
            if not use_mdlh_primary:
                mdlh = self.mdlh_client
                if mdlh is not None:
                    try:
                        activity.heartbeat("Enriching with MDLH lineage data (may require SSO login)...")
                        assets = mdlh.enrich_assets(assets)
                        logger.info("Assets enriched with MDLH data")
                    except Exception as e:
                        logger.warning(f"MDLH enrichment failed (continuing without): {e}")

            logger.info(f"Fetched {len(assets)} assets")
            return [asset.model_dump() for asset in assets]

        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return []

    @activity.defn
    async def fetch_usage_signals(self, assets_dict: List[dict]) -> Dict[str, dict]:
        """Fetch usage signals for assets."""
        try:
            assets = [AssetMetadata(**a) for a in assets_dict]
            signals = await self.usage_client.fetch_usage_signals(assets)

            return {qn: s.model_dump() for qn, s in signals.items()}

        except Exception as e:
            logger.error(f"Error fetching usage signals: {e}")
            return {}

    @activity.defn
    async def prioritize_assets(
        self,
        assets_dict: List[dict],
        usage_dict: Dict[str, dict],
        max_results: int,
    ) -> List[dict]:
        """Prioritize assets based on usage signals and metadata quality."""
        try:
            assets = [AssetMetadata(**a) for a in assets_dict]
            usage_signals = {qn: UsageSignals(**u) for qn, u in usage_dict.items()}

            prioritized = self.usage_client.prioritize_assets(
                assets, usage_signals, max_results
            )

            logger.info(f"Prioritized {len(prioritized)} assets")
            return [a.model_dump() for a in prioritized]

        except Exception as e:
            logger.error(f"Error prioritizing assets: {e}")
            return assets_dict[:max_results]

    @activity.defn
    async def generate_term_definitions(
        self,
        assets_dict: List[dict],
        usage_dict: Dict[str, dict],
        target_glossary_qn: str,
    ) -> List[dict]:
        """Generate term definitions using LLM."""
        try:
            assets = [AssetMetadata(**a) for a in assets_dict]
            usage_signals = {qn: UsageSignals(**u) for qn, u in usage_dict.items()}

            drafts = await self.term_generator.generate_all_terms(
                assets, usage_signals, target_glossary_qn
            )

            logger.info(f"Generated {len(drafts)} term definitions")
            return [d.model_dump() for d in drafts]

        except Exception as e:
            logger.error(f"Error generating definitions: {e}")
            return []

    @activity.defn
    async def save_draft_terms(
        self,
        terms_dict: List[dict],
        batch_id: str,
    ) -> dict:
        """Save draft terms to Dapr state store."""
        result = BatchResult(batch_id=batch_id)

        try:
            with DaprClient() as client:
                term_ids = []

                for term_data in terms_dict:
                    try:
                        term = GlossaryTermDraft(**term_data)
                        key = f"glossary_term_{term.id}"

                        # Convert to JSON-serializable format
                        state_value = term.model_dump(mode="json")

                        client.save_state(
                            store_name=DAPR_STORE_NAME,
                            key=key,
                            value=json.dumps(state_value),
                        )

                        term_ids.append(term.id)
                        result.terms_generated += 1

                    except Exception as e:
                        logger.error(f"Error saving term: {e}")
                        result.terms_failed += 1
                        result.errors.append(str(e))

                # Save batch index
                batch_index = {
                    "batch_id": batch_id,
                    "term_ids": term_ids,
                    "created_at": result.batch_id,
                }
                client.save_state(
                    store_name=DAPR_STORE_NAME,
                    key=f"glossary_batch_{batch_id}",
                    value=json.dumps(batch_index),
                )

                # Update master batch index so review page can find all batches
                master_key = "glossary_batch_index"
                try:
                    master_state = client.get_state(store_name=DAPR_STORE_NAME, key=master_key)
                    if master_state.data:
                        master = json.loads(master_state.data)
                    else:
                        master = {"batch_ids": []}
                except Exception:
                    master = {"batch_ids": []}

                if batch_id not in master["batch_ids"]:
                    master["batch_ids"].append(batch_id)

                client.save_state(
                    store_name=DAPR_STORE_NAME,
                    key=master_key,
                    value=json.dumps(master),
                )

                result.term_ids = term_ids

        except Exception as e:
            logger.error(f"Error connecting to Dapr: {e}")
            result.errors.append(f"Dapr connection error: {e}")

        return result.model_dump()

    @activity.defn
    async def notify_stewards(self, batch_id: str, term_count: int) -> bool:
        """Notify stewards that terms are ready for review."""
        logger.info(f"Batch {batch_id} ready for review with {term_count} terms")
        # In production, this would send notifications via email, Slack, etc.
        return True

    @activity.defn
    async def get_draft_term(self, term_id: str) -> Optional[dict]:
        """Retrieve a draft term from state store."""
        try:
            with DaprClient() as client:
                key = f"glossary_term_{term_id}"
                state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

                if state.data:
                    return json.loads(state.data)
                return None

        except Exception as e:
            logger.error(f"Error retrieving term {term_id}: {e}")
            return None

    @activity.defn
    async def update_draft_term(self, term_dict: dict) -> bool:
        """Update a draft term in state store."""
        try:
            term = GlossaryTermDraft(**term_dict)
            with DaprClient() as client:
                key = f"glossary_term_{term.id}"
                client.save_state(
                    store_name=DAPR_STORE_NAME,
                    key=key,
                    value=json.dumps(term.model_dump(mode="json")),
                )
                return True

        except Exception as e:
            logger.error(f"Error updating term: {e}")
            return False

    @activity.defn
    async def publish_terms(self, term_ids: List[str]) -> dict:
        """Publish approved terms to Atlan glossary."""
        results = {"published": 0, "failed": 0, "errors": []}

        try:
            with DaprClient() as client:
                for term_id in term_ids:
                    try:
                        # Get term from state
                        key = f"glossary_term_{term_id}"
                        state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

                        if not state.data:
                            results["failed"] += 1
                            results["errors"].append(f"Term not found: {term_id}")
                            continue

                        term_data = json.loads(state.data)
                        term = GlossaryTermDraft(**term_data)

                        # Only publish approved terms
                        if term.status != TermStatus.APPROVED:
                            results["failed"] += 1
                            results["errors"].append(f"Term not approved: {term_id}")
                            continue

                        # Create in Atlan
                        qn = await self.atlan_client.create_glossary_term(
                            term, term.target_glossary_qn
                        )

                        if qn:
                            # Update status to published
                            term.status = TermStatus.PUBLISHED
                            client.save_state(
                                store_name=DAPR_STORE_NAME,
                                key=key,
                                value=json.dumps(term.model_dump(mode="json")),
                            )
                            results["published"] += 1
                        else:
                            results["failed"] += 1
                            results["errors"].append(f"Failed to create term: {term_id}")

                    except Exception as e:
                        logger.error(f"Error publishing term {term_id}: {e}")
                        results["failed"] += 1
                        results["errors"].append(str(e))

        except Exception as e:
            logger.error(f"Error connecting to Dapr: {e}")
            results["errors"].append(f"Dapr connection error: {e}")

        return results
