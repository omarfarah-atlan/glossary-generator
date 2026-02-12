"""Glossary generation workflow definition."""

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.activities import GlossaryActivities
    from app.models import GenerationResult, WorkflowConfig

logger = logging.getLogger(__name__)


@workflow.defn
class GlossaryGenerationWorkflow:
    """Workflow for generating glossary terms from metadata."""

    def __init__(self):
        self._status = "initializing"
        self._progress = 0
        self._status_message = "Initializing workflow..."
        self._log_entries = []

    def _log(self, message: str, step: str = ""):
        """Append a timestamped log entry and update status message."""
        self._status_message = message
        self._log_entries.append({
            "message": message,
            "step": step or self._status,
        })

    @workflow.run
    async def run(self, config_dict: dict) -> dict:
        """Execute the glossary generation workflow."""

        result = GenerationResult(
            workflow_id=workflow.info().workflow_id,
        )

        # Step 1: Validate configuration (5%)
        self._status = "validating"
        self._progress = 5
        self._log("Validating glossary configuration...", "validating")

        validation = await workflow.execute_activity(
            GlossaryActivities.validate_configuration,
            config_dict,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        if not validation.get("valid"):
            result.status = "failed"
            result.error_message = validation.get("error", "Invalid configuration")
            self._status = "failed"
            self._log(f"Failed: {result.error_message}", "failed")
            return result.model_dump()

        config = WorkflowConfig(**validation["config"])
        type_labels = {"business_term": "Business Terms", "metric": "Metrics", "dimension": "Dimensions"}
        selected_labels = ", ".join(type_labels.get(t, t) for t in config.term_types)
        # Extract short glossary name from qualified_name for cleaner logs
        glossary_short = config.target_glossary_qn.split("/")[-1] if "/" in config.target_glossary_qn else config.target_glossary_qn
        self._log(f"Config OK — {selected_labels} | Up to {config.max_terms} terms | Glossary: {glossary_short}", "validating")

        # Step 2: Fetch metadata from Atlan (15%)
        self._status = "fetching_metadata"
        self._progress = 15
        self._log(f"Searching Atlan for data assets...", "fetching_metadata")

        assets_dict = await workflow.execute_activity(
            GlossaryActivities.fetch_metadata,
            config.model_dump(),
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        if not assets_dict:
            result.status = "completed"
            result.error_message = "No assets found matching criteria"
            self._status = "completed"
            self._log("Completed: No assets found matching criteria.", "completed")
            return result.model_dump()

        self._log(f"Found {len(assets_dict)} assets from Atlan.", "fetching_metadata")

        # Step 3: Fetch usage signals (30%)
        self._status = "fetching_usage"
        self._progress = 30
        self._log(f"Fetching query & user activity for {len(assets_dict)} assets...", "fetching_usage")

        usage_dict = await workflow.execute_activity(
            GlossaryActivities.fetch_usage_signals,
            assets_dict,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._log(f"Activity data collected for {len(usage_dict)} assets.", "fetching_usage")

        # Step 4: Prioritize assets (40%)
        self._status = "prioritizing"
        self._progress = 40
        self._log(f"Ranking {len(assets_dict)} assets by popularity and metadata quality...", "prioritizing")

        prioritized = await workflow.execute_activity(
            GlossaryActivities.prioritize_assets,
            args=[assets_dict, usage_dict, config.max_assets],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        result.total_assets_processed = len(prioritized)
        self._log(f"Top {len(prioritized)} assets selected for term generation.", "prioritizing")

        # Step 4b: Fetch existing terms for deduplication (45%)
        self._progress = 45
        self._log("Checking glossary for existing terms (deduplication)...", "prioritizing")

        existing_term_names = await workflow.execute_activity(
            GlossaryActivities.fetch_existing_terms,
            config.target_glossary_qn,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        if existing_term_names:
            self._log(f"{len(existing_term_names)} existing terms found — will skip duplicates.", "prioritizing")
        else:
            self._log("No existing terms — all generated terms will be new.", "prioritizing")

        # Step 5: Generate term definitions (50%)
        self._status = "generating_definitions"
        self._progress = 50
        self._log(f"Sending {len(prioritized)} assets to AI for {selected_labels} generation...", "generating_definitions")

        terms_dict = await workflow.execute_activity(
            GlossaryActivities.generate_term_definitions,
            args=[prioritized, usage_dict, config.target_glossary_qn, existing_term_names, config.custom_context, config.term_types],
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        self._log(f"AI generated {len(terms_dict)} term definitions.", "generating_definitions")

        # Check if anything was generated
        if not terms_dict:
            result.status = "completed"
            result.error_message = "No terms generated"
            self._status = "completed"
            self._log("No terms could be generated. Try adjusting settings or selecting different types.", "completed")
            return result.model_dump()

        # Trim to max_terms limit
        if len(terms_dict) > config.max_terms:
            self._log(f"Keeping top {config.max_terms} of {len(terms_dict)} generated terms.", "generating_definitions")
            terms_dict = terms_dict[:config.max_terms]

        # Step 6: Save draft terms (85%)
        self._status = "saving_drafts"
        self._progress = 85
        self._log(f"Saving {len(terms_dict)} terms as drafts for review...", "saving_drafts")

        # Use workflow.uuid4() for Temporal-safe deterministic UUID
        batch_id = str(workflow.uuid4())
        batch_result = await workflow.execute_activity(
            GlossaryActivities.save_draft_terms,
            args=[terms_dict, batch_id],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        result.total_terms_generated = batch_result.get("terms_generated", 0)
        result.total_terms_failed = batch_result.get("terms_failed", 0)
        saved_msg = f"{result.total_terms_generated} terms saved."
        if result.total_terms_failed > 0:
            saved_msg += f" ({result.total_terms_failed} failed to save)"
        self._log(saved_msg, "saving_drafts")

        # Step 7: Notify stewards (95%)
        self._status = "notifying"
        self._progress = 95
        self._log("Finalizing and preparing review queue...", "notifying")

        await workflow.execute_activity(
            GlossaryActivities.notify_stewards,
            args=[batch_id, result.total_terms_generated],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._status = "completed"
        self._progress = 100
        result.status = "completed"
        self._log(f"Complete — {result.total_terms_generated} terms ready for review.", "completed")

        return result.model_dump()

    @workflow.query
    def get_status(self) -> str:
        """Query current workflow status."""
        return self._status

    @workflow.query
    def get_progress(self) -> int:
        """Query current workflow progress percentage."""
        return self._progress

    @workflow.query
    def get_status_message(self) -> str:
        """Query current workflow status message with details."""
        return self._status_message

    @workflow.query
    def get_log(self) -> list:
        """Query full log of workflow messages."""
        return self._log_entries


@workflow.defn
class ApprovalWorkflow:
    """Workflow for approving and publishing glossary terms."""

    @workflow.run
    async def run(self, term_ids: list, action: str = "approve") -> dict:
        """Execute the approval workflow."""

        results = {"approved": 0, "rejected": 0, "published": 0, "errors": []}

        if action == "publish":
            # Publish approved terms
            publish_result = await workflow.execute_activity(
                GlossaryActivities.publish_terms,
                term_ids,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )
            results["published"] = publish_result.get("published", 0)
            results["errors"] = publish_result.get("errors", [])

        return results
