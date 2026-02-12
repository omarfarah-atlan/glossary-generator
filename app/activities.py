"""Workflow activities for glossary generation."""

import os
import logging
import json
from typing import Dict, List, Optional
from temporalio import activity
from dapr.clients import DaprClient

from app.models import (
    AssetMetadata,
    ColumnClassification,
    GlossaryTermDraft,
    TermStatus,
    TermType,
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
            activity.heartbeat(f"Prioritizing {len(assets_dict)} assets by usage and metadata quality...")
            assets = [AssetMetadata(**a) for a in assets_dict]
            usage_signals = {qn: UsageSignals(**u) for qn, u in usage_dict.items()}

            prioritized = self.usage_client.prioritize_assets(
                assets, usage_signals, max_results
            )

            activity.heartbeat(f"Selected top {len(prioritized)} assets")
            logger.info(f"Prioritized {len(prioritized)} assets")
            return [a.model_dump() for a in prioritized]

        except Exception as e:
            logger.error(f"Error prioritizing assets: {e}")
            return assets_dict[:max_results]

    @activity.defn
    async def fetch_existing_terms(self, glossary_qn: str) -> List[str]:
        """Fetch existing term names from Atlan glossary for deduplication."""
        try:
            term_names = await self.atlan_client.get_glossary_terms(glossary_qn)
            logger.info(f"Fetched {len(term_names)} existing terms from glossary for dedup")
            return term_names
        except Exception as e:
            logger.error(f"Error fetching existing terms: {e}")
            return []

    @activity.defn
    async def generate_term_definitions(
        self,
        assets_dict: List[dict],
        usage_dict: Dict[str, dict],
        target_glossary_qn: str,
        existing_term_names: Optional[List[str]] = None,
        custom_context: Optional[str] = None,
        term_types: Optional[List[str]] = None,
    ) -> List[dict]:
        """Generate term definitions using LLM."""
        try:
            assets = [AssetMetadata(**a) for a in assets_dict]
            usage_signals = {qn: UsageSignals(**u) for qn, u in usage_dict.items()}

            type_label = ", ".join(term_types) if term_types else "all types"
            activity.heartbeat(f"Generating {type_label} terms for {len(assets)} assets...")

            drafts = await self.term_generator.generate_all_terms(
                assets,
                usage_signals,
                target_glossary_qn,
                existing_term_names=set(existing_term_names or []),
                custom_context=custom_context,
                term_types=term_types,
            )

            activity.heartbeat(f"Completed: generated {len(drafts)} terms")
            logger.info(f"Generated {len(drafts)} term definitions")
            return [d.model_dump() for d in drafts]

        except Exception as e:
            logger.error(f"Error generating definitions: {e}")
            return []

    def _load_existing_draft_names(self, client) -> set:
        """Load term names from all existing Dapr draft batches for cross-batch dedup."""
        existing_names = set()
        try:
            master_state = client.get_state(store_name=DAPR_STORE_NAME, key="glossary_batch_index")
            if not master_state.data:
                return existing_names
            master = json.loads(master_state.data)
            for bid in master.get("batch_ids", []):
                batch_state = client.get_state(store_name=DAPR_STORE_NAME, key=f"glossary_batch_{bid}")
                if not batch_state.data:
                    continue
                batch_info = json.loads(batch_state.data)
                for tid in batch_info.get("term_ids", []):
                    term_state = client.get_state(store_name=DAPR_STORE_NAME, key=f"glossary_term_{tid}")
                    if term_state.data:
                        term_data = json.loads(term_state.data)
                        existing_names.add(term_data.get("name", "").lower())
        except Exception as e:
            logger.warning(f"Could not load existing draft names for dedup: {e}")
        return existing_names

    @activity.defn
    async def save_draft_terms(
        self,
        terms_dict: List[dict],
        batch_id: str,
    ) -> dict:
        """Save draft terms to Dapr state store with cross-batch deduplication."""
        result = BatchResult(batch_id=batch_id)

        try:
            with DaprClient() as client:
                # Cross-batch dedup: load names from previous batches
                existing_draft_names = self._load_existing_draft_names(client)
                skipped = 0

                term_ids = []

                for term_data in terms_dict:
                    # Skip if a draft with this name already exists
                    term_name = term_data.get("name", "")
                    if term_name.lower() in existing_draft_names:
                        logger.info(f"Cross-batch dedup: skipping already-drafted term '{term_name}'")
                        skipped += 1
                        continue

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
                        existing_draft_names.add(term.name.lower())
                        result.terms_generated += 1

                    except Exception as e:
                        logger.error(f"Error saving term: {e}")
                        result.terms_failed += 1
                        result.errors.append(str(e))

                if skipped > 0:
                    logger.info(f"Cross-batch dedup: skipped {skipped} duplicate draft terms")

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
    async def classify_and_generate_column_terms(
        self,
        assets_dict: List[dict],
        usage_dict: Dict[str, dict],
        target_glossary_qn: str,
        existing_term_names: Optional[List[str]] = None,
        custom_context: Optional[str] = None,
        term_types: Optional[List[str]] = None,
    ) -> List[dict]:
        """Classify columns and generate column-level glossary terms."""
        try:
            assets = [AssetMetadata(**a) for a in assets_dict]
            usage_signals = {qn: UsageSignals(**u) for qn, u in usage_dict.items()}

            # Fetch column metadata if not already present
            assets_without_cols = [a for a in assets if not a.columns]
            if assets_without_cols:
                activity.heartbeat(f"Fetching column metadata for {len(assets_without_cols)} assets...")
                assets = await self.atlan_client.fetch_columns_for_assets(assets)

            # Which column-level types are requested
            allowed_types = set(term_types or ["metric", "dimension"])
            type_labels = {"metric": "Metrics", "dimension": "Dimensions", "business_term": "Business Terms"}

            existing_lower = {n.lower() for n in (existing_term_names or [])}
            generated_names: set = set()
            all_column_terms = []

            assets_with_columns = [a for a in assets if a.columns]
            total = len(assets_with_columns)

            for idx, asset in enumerate(assets_with_columns, 1):
                col_count = len(asset.columns)
                activity.heartbeat(
                    f"[{idx}/{total}] Classifying {col_count} columns in {asset.name}..."
                )

                # Classify columns (one LLM call per asset)
                classifications = await self.term_generator.classify_asset_columns(asset)

                if not classifications:
                    logger.info(f"[{idx}/{total}] {asset.name}: no classifications returned")
                    continue

                # Filter to only requested term types
                filtered = [
                    c for c in classifications
                    if c.should_generate and c.term_type.value in allowed_types
                ]
                skipped_type = sum(
                    1 for c in classifications
                    if c.should_generate and c.term_type.value not in allowed_types
                )

                selected = len(filtered)
                logger.info(
                    f"[{idx}/{total}] {asset.name}: {selected}/{col_count} columns selected "
                    f"for term generation (skipped {skipped_type} outside requested types)"
                )

                if not filtered:
                    continue

                activity.heartbeat(
                    f"[{idx}/{total}] Generating {selected} column terms for {asset.name}..."
                )

                # Generate terms for selected columns
                usage = usage_signals.get(asset.qualified_name)
                column_drafts = await self.term_generator.generate_column_terms_for_asset(
                    asset=asset,
                    classifications=filtered,
                    usage=usage,
                    target_glossary_qn=target_glossary_qn,
                    custom_context=custom_context,
                )

                # Deduplicate against existing and already-generated names
                added = 0
                for draft in column_drafts:
                    name_lower = draft.name.lower()
                    if name_lower in existing_lower or name_lower in generated_names:
                        logger.info(f"Column term dedup: skipping duplicate '{draft.name}'")
                        continue
                    generated_names.add(name_lower)
                    all_column_terms.append(draft)
                    added += 1

                activity.heartbeat(
                    f"[{idx}/{total}] {asset.name}: generated {added} column terms "
                    f"({len(all_column_terms)} total so far)"
                )

            # Final summary by type
            type_counts = {}
            for t in all_column_terms:
                tv = t.term_type.value
                type_counts[tv] = type_counts.get(tv, 0) + 1
            summary = ", ".join(f"{type_labels.get(k, k)}: {v}" for k, v in type_counts.items())
            logger.info(f"Generated {len(all_column_terms)} column-level terms total — {summary}")

            return [d.model_dump() for d in all_column_terms]

        except Exception as e:
            logger.error(f"Error in column term generation: {e}")
            return []

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

                        # Create in Atlan (with term type for category assignment)
                        qn = await self.atlan_client.create_glossary_term(
                            term, term.target_glossary_qn, term_type=term.term_type.value
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
