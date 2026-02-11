"""Glossary generation workflow definition."""

import logging
from datetime import timedelta
from uuid import uuid4

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

    @workflow.run
    async def run(self, config_dict: dict) -> dict:
        """Execute the glossary generation workflow."""

        result = GenerationResult(
            workflow_id=workflow.info().workflow_id,
        )

        # Step 1: Validate configuration
        self._status = "validating"
        self._progress = 5

        validation = await workflow.execute_activity(
            GlossaryActivities.validate_configuration,
            config_dict,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        if not validation.get("valid"):
            result.status = "failed"
            result.error_message = validation.get("error", "Invalid configuration")
            return result.model_dump()

        config = WorkflowConfig(**validation["config"])

        # Step 2: Fetch metadata from Atlan
        self._status = "fetching_metadata"
        self._progress = 15

        assets_dict = await workflow.execute_activity(
            GlossaryActivities.fetch_metadata,
            config.model_dump(),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        if not assets_dict:
            result.status = "completed"
            result.error_message = "No assets found matching criteria"
            return result.model_dump()

        # Step 3: Fetch usage signals
        self._status = "fetching_usage"
        self._progress = 30

        usage_dict = await workflow.execute_activity(
            GlossaryActivities.fetch_usage_signals,
            assets_dict,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # Step 4: Prioritize assets
        self._status = "prioritizing"
        self._progress = 40

        prioritized = await workflow.execute_activity(
            GlossaryActivities.prioritize_assets,
            args=[assets_dict, usage_dict, config.max_assets],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        result.total_assets_processed = len(prioritized)

        # Step 5: Generate term definitions (longest step)
        self._status = "generating_definitions"
        self._progress = 50

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
            return result.model_dump()

        # Step 6: Save draft terms
        self._status = "saving_drafts"
        self._progress = 85

        batch_id = str(uuid4())
        batch_result = await workflow.execute_activity(
            GlossaryActivities.save_draft_terms,
            args=[terms_dict, batch_id],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        result.total_terms_generated = batch_result.get("terms_generated", 0)
        result.total_terms_failed = batch_result.get("terms_failed", 0)

        # Step 7: Notify stewards
        self._status = "notifying"
        self._progress = 95

        await workflow.execute_activity(
            GlossaryActivities.notify_stewards,
            args=[batch_id, result.total_terms_generated],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._status = "completed"
        self._progress = 100
        result.status = "completed"

        return result.model_dump()

    @workflow.query
    def get_status(self) -> str:
        """Query current workflow status."""
        return self._status

    @workflow.query
    def get_progress(self) -> int:
        """Query current workflow progress percentage."""
        return self._progress


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
