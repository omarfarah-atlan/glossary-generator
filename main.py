"""Main entry point for the Glossary Generator application."""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from temporalio.client import Client
from temporalio.worker import Worker

from app.workflow import GlossaryGenerationWorkflow, ApprovalWorkflow
from app.activities import GlossaryActivities
from handlers.review_handler import router as review_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = "glossary-generation-queue"
SERVER_PORT = int(os.environ.get("SERVER_PORT", "3000"))


class GlossaryGeneratorApp:
    """Main application class for the Glossary Generator."""

    def __init__(self):
        self.app = FastAPI(
            title="Glossary Generator",
            description="Auto-generates business glossary terms from metadata",
            version="0.1.0",
        )
        self.temporal_client: Client = None
        self.activities = GlossaryActivities()
        self._setup_routes()

    def _setup_routes(self):
        """Configure FastAPI routes."""
        # Mount static files
        static_path = Path(__file__).parent / "frontend" / "static"
        if static_path.exists():
            self.app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

        # Include review router
        self.app.include_router(review_router)

        # Workflow trigger endpoint
        @self.app.post("/workflows/v1/start")
        async def start_workflow(config: dict):
            """Start a glossary generation workflow."""
            if not self.temporal_client:
                return {"error": "Temporal client not initialized"}

            try:
                handle = await self.temporal_client.start_workflow(
                    GlossaryGenerationWorkflow.run,
                    config,
                    id=f"glossary-gen-{config.get('target_glossary_qn', 'unknown')}-{asyncio.get_event_loop().time()}",
                    task_queue=TASK_QUEUE,
                )
                return {
                    "workflow_id": handle.id,
                    "status": "started",
                }
            except Exception as e:
                logger.error(f"Error starting workflow: {e}")
                return {"error": str(e)}

        @self.app.get("/workflows/v1/status/{workflow_id}")
        async def get_workflow_status(workflow_id: str):
            """Get workflow status."""
            if not self.temporal_client:
                return {"error": "Temporal client not initialized"}

            try:
                handle = self.temporal_client.get_workflow_handle(workflow_id)
                status = await handle.query(GlossaryGenerationWorkflow.get_status)
                progress = await handle.query(GlossaryGenerationWorkflow.get_progress)
                message = await handle.query(GlossaryGenerationWorkflow.get_status_message)
                log_entries = await handle.query(GlossaryGenerationWorkflow.get_log)
                return {"workflow_id": workflow_id, "status": status, "progress": progress, "message": message, "log_entries": log_entries}
            except Exception as e:
                return {"workflow_id": workflow_id, "error": str(e)}

        @self.app.get("/health")
        async def health():
            """Health check endpoint."""
            return {"status": "healthy"}

        @self.app.get("/api/v1/glossaries")
        async def get_glossaries():
            """Get all glossaries from Atlan."""
            try:
                glossaries = await self.activities.atlan_client.get_all_glossaries()
                return {"glossaries": glossaries}
            except Exception as e:
                logger.error(f"Error fetching glossaries: {e}")
                return {"error": str(e), "glossaries": []}

        @self.app.get("/api/v1/connectors")
        async def get_connectors():
            """Get all connector types."""
            try:
                connectors = await self.activities.atlan_client.get_connector_types()
                return {"connectors": connectors}
            except Exception as e:
                logger.error(f"Error fetching connectors: {e}")
                return {"error": str(e), "connectors": []}

        @self.app.get("/api/v1/connections")
        async def get_connections(connector_type: str = None):
            """Get all connections, optionally filtered by connector type."""
            try:
                connections = await self.activities.atlan_client.get_all_connections(connector_type)
                return {"connections": connections}
            except Exception as e:
                logger.error(f"Error fetching connections: {e}")
                return {"error": str(e), "connections": []}

    async def connect_temporal(self):
        """Connect to Temporal server."""
        try:
            self.temporal_client = await Client.connect(
                TEMPORAL_HOST,
                namespace=TEMPORAL_NAMESPACE,
            )
            logger.info(f"Connected to Temporal at {TEMPORAL_HOST}")
        except Exception as e:
            logger.error(f"Failed to connect to Temporal: {e}")
            raise

    async def run_worker(self):
        """Run the Temporal worker."""
        await self.connect_temporal()

        worker = Worker(
            self.temporal_client,
            task_queue=TASK_QUEUE,
            workflows=[GlossaryGenerationWorkflow, ApprovalWorkflow],
            activities=[
                self.activities.validate_configuration,
                self.activities.fetch_metadata,
                self.activities.fetch_usage_signals,
                self.activities.prioritize_assets,
                self.activities.generate_term_definitions,
                self.activities.save_draft_terms,
                self.activities.notify_stewards,
                self.activities.get_draft_term,
                self.activities.update_draft_term,
                self.activities.publish_terms,
            ],
        )

        logger.info(f"Starting worker on task queue: {TASK_QUEUE}")
        await worker.run()

    async def run_server(self):
        """Run the FastAPI server."""
        import uvicorn

        await self.connect_temporal()

        config = uvicorn.Config(
            app=self.app,
            host="0.0.0.0",
            port=SERVER_PORT,
            log_level="info",
        )
        server = uvicorn.Server(config)
        await server.serve()

    async def run_local(self):
        """Run both worker and server for local development."""
        await self.connect_temporal()

        # Start worker in background
        worker = Worker(
            self.temporal_client,
            task_queue=TASK_QUEUE,
            workflows=[GlossaryGenerationWorkflow, ApprovalWorkflow],
            activities=[
                self.activities.validate_configuration,
                self.activities.fetch_metadata,
                self.activities.fetch_usage_signals,
                self.activities.prioritize_assets,
                self.activities.generate_term_definitions,
                self.activities.save_draft_terms,
                self.activities.notify_stewards,
                self.activities.get_draft_term,
                self.activities.update_draft_term,
                self.activities.publish_terms,
            ],
        )

        # Run worker and server concurrently
        import uvicorn

        config = uvicorn.Config(
            app=self.app,
            host="0.0.0.0",
            port=SERVER_PORT,
            log_level="info",
        )
        server = uvicorn.Server(config)

        logger.info(f"Starting local development mode on port {SERVER_PORT}")
        await asyncio.gather(
            worker.run(),
            server.serve(),
        )


def main():
    """Main entry point."""
    mode = os.environ.get("APPLICATION_MODE", "LOCAL").upper()
    app = GlossaryGeneratorApp()

    if mode == "WORKER":
        asyncio.run(app.run_worker())
    elif mode == "SERVER":
        asyncio.run(app.run_server())
    else:
        # Local development: run both
        asyncio.run(app.run_local())


if __name__ == "__main__":
    main()
