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
        self._log(f"Configuration valid. Target glossary: {config.target_glossary_qn}", "validating")

        # Step 2: Fetch metadata from Atlan (15%)
        self._status = "fetching_metadata"
        self._progress = 15
        self._log(f"Fetching {', '.join(config.asset_types)} metadata from Atlan (max {config.max_assets})...", "fetching_metadata")

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

        self._log(f"Found {len(assets_dict)} assets.", "fetching_metadata")

        # Step 3: Fetch usage signals (30%)
        self._status = "fetching_usage"
        self._progress = 30
        self._log(f"Fetching usage signals for {len(assets_dict)} assets...", "fetching_usage")

        usage_dict = await workflow.execute_activity(
            GlossaryActivities.fetch_usage_signals,
            assets_dict,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._log(f"Usage signals fetched for {len(usage_dict)} assets.", "fetching_usage")

        # Step 4: Prioritize assets (40%)
        self._status = "prioritizing"
        self._progress = 40
        self._log(f"Prioritizing {len(assets_dict)} assets by usage and metadata quality...", "prioritizing")

        prioritized = await workflow.execute_activity(
            GlossaryActivities.prioritize_assets,
            args=[assets_dict, usage_dict, config.max_assets],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        result.total_assets_processed = len(prioritized)
        self._log(f"Prioritized top {len(prioritized)} assets for term generation.", "prioritizing")

        # Step 5: Generate term definitions (50%)
        self._status = "generating_definitions"
        self._progress = 50
        self._log(f"Generating glossary definitions for {len(prioritized)} assets using LLM... (this may take a few minutes)", "generating_definitions")

        terms_dict = await workflow.execute_activity(
            GlossaryActivities.generate_term_definitions,
            args=[prioritized, usage_dict, config.target_glossary_qn],
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        if not terms_dict:
            result.status = "completed"
            result.error_message = "No terms generated"
            self._status = "completed"
            self._log("Completed: LLM did not generate any terms.", "completed")
            return result.model_dump()

        self._log(f"Generated {len(terms_dict)} term definitions.", "generating_definitions")

        # Step 6: Save draft terms (85%)
        self._status = "saving_drafts"
        self._progress = 85
        self._log(f"Saving {len(terms_dict)} draft terms to state store...", "saving_drafts")

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
        self._log(f"Saved {result.total_terms_generated} terms ({result.total_terms_failed} failed).", "saving_drafts")

        # Step 7: Notify stewards (95%)
        self._status = "notifying"
        self._progress = 95
        self._log("Notifying data stewards for review...", "notifying")

        await workflow.execute_activity(
            GlossaryActivities.notify_stewards,
            args=[batch_id, result.total_terms_generated],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._status = "completed"
        self._progress = 100
        result.status = "completed"
        self._log(f"Done! Generated {result.total_terms_generated} glossary terms ready for review.", "completed")

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
