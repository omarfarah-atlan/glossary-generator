"""HTTP handlers for the review UI."""

import json
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from dapr.clients import DaprClient

from app.models import GlossaryTermDraft, TermStatus, AppSettings

logger = logging.getLogger(__name__)

DAPR_STORE_NAME = "statestore"

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")


class ApproveRequest(BaseModel):
    """Request body for approving a term."""
    edited_definition: Optional[str] = None
    reviewer_notes: Optional[str] = None


class RejectRequest(BaseModel):
    """Request body for rejecting a term."""
    reason: str


class BulkApproveRequest(BaseModel):
    """Request body for bulk approval."""
    term_ids: List[str]


class PublishRequest(BaseModel):
    """Request body for publishing terms."""
    term_ids: List[str]


class SettingsUpdateRequest(BaseModel):
    """Request body for updating settings."""
    anthropic_api_key: Optional[str] = None
    atlan_api_key: Optional[str] = None
    atlan_base_url: Optional[str] = None
    claude_model: Optional[str] = None
    default_glossary_qn: Optional[str] = None


SETTINGS_KEY = "app_settings"


def get_all_term_ids() -> List[str]:
    """Get all term IDs from state store by scanning batch indices."""
    term_ids = []
    try:
        with DaprClient() as client:
            # Scan for batch indices (in production, use proper querying)
            # For now, we'll try common patterns
            for i in range(100):  # Check first 100 potential batches
                key = f"glossary_batch_{i}"
                try:
                    state = client.get_state(store_name=DAPR_STORE_NAME, key=key)
                    if state.data:
                        batch = json.loads(state.data)
                        term_ids.extend(batch.get("term_ids", []))
                except Exception:
                    continue
    except Exception as e:
        logger.error(f"Error scanning batches: {e}")
    return term_ids


def get_terms_by_status(status: Optional[TermStatus] = None) -> List[GlossaryTermDraft]:
    """Get all terms, optionally filtered by status."""
    terms = []
    try:
        with DaprClient() as client:
            # Get terms from known IDs (this is a simplified approach)
            # In production, maintain a proper index
            term_ids = get_all_term_ids()

            for term_id in term_ids:
                try:
                    key = f"glossary_term_{term_id}"
                    state = client.get_state(store_name=DAPR_STORE_NAME, key=key)
                    if state.data:
                        term_data = json.loads(state.data)
                        term = GlossaryTermDraft(**term_data)
                        if status is None or term.status == status:
                            terms.append(term)
                except Exception as e:
                    logger.warning(f"Error loading term {term_id}: {e}")
                    continue

    except Exception as e:
        logger.error(f"Error connecting to Dapr: {e}")

    return terms


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the workflow trigger page."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/review", response_class=HTMLResponse)
async def review_page(request: Request):
    """Render the review page."""
    return templates.TemplateResponse("review.html", {"request": request})


@router.get("/api/v1/terms")
async def list_terms(
    status: Optional[str] = None,
    confidence: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """List draft terms with optional filtering."""
    try:
        status_filter = TermStatus(status) if status else None
    except ValueError:
        status_filter = None

    terms = get_terms_by_status(status_filter)

    # Filter by confidence if specified
    if confidence:
        terms = [t for t in terms if t.confidence == confidence]

    # Apply pagination
    total = len(terms)
    terms = terms[offset : offset + limit]

    return {
        "terms": [t.model_dump(mode="json") for t in terms],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/api/v1/terms/{term_id}")
async def get_term(term_id: str):
    """Get a single term by ID."""
    try:
        with DaprClient() as client:
            key = f"glossary_term_{term_id}"
            state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

            if not state.data:
                raise HTTPException(status_code=404, detail="Term not found")

            term_data = json.loads(state.data)
            return term_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting term {term_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/terms/{term_id}/approve")
async def approve_term(term_id: str, request: ApproveRequest):
    """Approve a term with optional edits."""
    try:
        with DaprClient() as client:
            key = f"glossary_term_{term_id}"
            state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

            if not state.data:
                raise HTTPException(status_code=404, detail="Term not found")

            term_data = json.loads(state.data)
            term = GlossaryTermDraft(**term_data)

            # Update term
            term.status = TermStatus.APPROVED
            if request.edited_definition:
                term.edited_definition = request.edited_definition
            if request.reviewer_notes:
                term.reviewer_notes = request.reviewer_notes

            # Save updated term
            client.save_state(
                store_name=DAPR_STORE_NAME,
                key=key,
                value=json.dumps(term.model_dump(mode="json")),
            )

            return {"status": "approved", "term_id": term_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error approving term {term_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/terms/{term_id}/reject")
async def reject_term(term_id: str, request: RejectRequest):
    """Reject a term with a reason."""
    try:
        with DaprClient() as client:
            key = f"glossary_term_{term_id}"
            state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

            if not state.data:
                raise HTTPException(status_code=404, detail="Term not found")

            term_data = json.loads(state.data)
            term = GlossaryTermDraft(**term_data)

            # Update term
            term.status = TermStatus.REJECTED
            term.reviewer_notes = request.reason

            # Save updated term
            client.save_state(
                store_name=DAPR_STORE_NAME,
                key=key,
                value=json.dumps(term.model_dump(mode="json")),
            )

            return {"status": "rejected", "term_id": term_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error rejecting term {term_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/terms/bulk-approve")
async def bulk_approve(request: BulkApproveRequest):
    """Bulk approve multiple terms."""
    results = {"approved": 0, "failed": 0, "errors": []}

    try:
        with DaprClient() as client:
            for term_id in request.term_ids:
                try:
                    key = f"glossary_term_{term_id}"
                    state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

                    if not state.data:
                        results["failed"] += 1
                        results["errors"].append(f"Term not found: {term_id}")
                        continue

                    term_data = json.loads(state.data)
                    term = GlossaryTermDraft(**term_data)
                    term.status = TermStatus.APPROVED

                    client.save_state(
                        store_name=DAPR_STORE_NAME,
                        key=key,
                        value=json.dumps(term.model_dump(mode="json")),
                    )
                    results["approved"] += 1

                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"Error on {term_id}: {str(e)}")

    except Exception as e:
        logger.error(f"Bulk approve error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return results


@router.post("/api/v1/terms/publish")
async def publish_terms(request: PublishRequest):
    """Publish approved terms to Atlan glossary."""
    from clients.atlan_client import AtlanMetadataClient

    results = {"published": 0, "failed": 0, "errors": []}
    atlan_client = AtlanMetadataClient()

    try:
        with DaprClient() as client:
            for term_id in request.term_ids:
                try:
                    key = f"glossary_term_{term_id}"
                    state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

                    if not state.data:
                        results["failed"] += 1
                        results["errors"].append(f"Term not found: {term_id}")
                        continue

                    term_data = json.loads(state.data)
                    term = GlossaryTermDraft(**term_data)

                    if term.status != TermStatus.APPROVED:
                        results["failed"] += 1
                        results["errors"].append(f"Term not approved: {term_id}")
                        continue

                    # Create in Atlan
                    qn = await atlan_client.create_glossary_term(
                        term, term.target_glossary_qn
                    )

                    if qn:
                        term.status = TermStatus.PUBLISHED
                        client.save_state(
                            store_name=DAPR_STORE_NAME,
                            key=key,
                            value=json.dumps(term.model_dump(mode="json")),
                        )
                        results["published"] += 1
                    else:
                        results["failed"] += 1
                        results["errors"].append(f"Failed to create: {term_id}")

                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"Error on {term_id}: {str(e)}")

    except Exception as e:
        logger.error(f"Publish error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return results


@router.get("/api/v1/stats")
async def get_stats():
    """Get statistics about draft terms."""
    terms = get_terms_by_status()

    stats = {
        "total": len(terms),
        "by_status": {},
        "by_confidence": {},
    }

    for status in TermStatus:
        stats["by_status"][status.value] = sum(1 for t in terms if t.status == status)

    for confidence in ["high", "medium", "low"]:
        stats["by_confidence"][confidence] = sum(
            1 for t in terms if t.confidence == confidence
        )

    return stats


# Settings endpoints

@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """Render the settings page."""
    return templates.TemplateResponse("settings.html", {"request": request})


@router.get("/api/v1/settings")
async def get_settings():
    """Get current application settings (with masked keys)."""
    try:
        with DaprClient() as client:
            state = client.get_state(store_name=DAPR_STORE_NAME, key=SETTINGS_KEY)

            if state.data:
                settings_data = json.loads(state.data)
                settings = AppSettings(**settings_data)
            else:
                settings = AppSettings()

            return settings.to_display()

    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        # Return empty settings if Dapr is not available
        return AppSettings().to_display()


@router.post("/api/v1/settings")
async def update_settings(request: SettingsUpdateRequest):
    """Update application settings."""
    try:
        with DaprClient() as client:
            # Load existing settings
            state = client.get_state(store_name=DAPR_STORE_NAME, key=SETTINGS_KEY)

            if state.data:
                existing = AppSettings(**json.loads(state.data))
            else:
                existing = AppSettings()

            # Update only provided fields (don't overwrite with None)
            update_data = request.model_dump(exclude_none=True)

            # Special handling: if key is empty string, treat as clearing
            for key in ["anthropic_api_key", "atlan_api_key"]:
                if key in update_data and update_data[key] == "":
                    update_data[key] = None

            # Don't update keys if they look like masked values
            for key in ["anthropic_api_key", "atlan_api_key"]:
                if key in update_data:
                    val = update_data[key]
                    if val and ("..." in val or val == "****"):
                        del update_data[key]

            updated = existing.model_copy(update=update_data)

            # Save updated settings
            client.save_state(
                store_name=DAPR_STORE_NAME,
                key=SETTINGS_KEY,
                value=json.dumps(updated.model_dump()),
            )

            return {
                "status": "saved",
                "settings": updated.to_display(),
            }

    except Exception as e:
        logger.error(f"Error saving settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/settings/test-anthropic")
async def test_anthropic_connection():
    """Test the Anthropic API connection."""
    try:
        settings = await _get_settings()

        if not settings.anthropic_api_key:
            return {"success": False, "error": "Anthropic API key not configured"}

        from anthropic import Anthropic

        client = Anthropic(api_key=settings.anthropic_api_key)
        # Make a minimal API call to test
        response = client.messages.create(
            model=settings.claude_model,
            max_tokens=10,
            messages=[{"role": "user", "content": "Hi"}]
        )

        return {"success": True, "model": settings.claude_model}

    except Exception as e:
        logger.error(f"Anthropic test failed: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/v1/settings/test-atlan")
async def test_atlan_connection():
    """Test the Atlan API connection."""
    try:
        settings = await _get_settings()

        if not settings.atlan_base_url:
            return {"success": False, "error": "Atlan base URL not configured"}

        from pyatlan.client.atlan import AtlanClient

        if settings.atlan_api_key:
            client = AtlanClient(
                base_url=settings.atlan_base_url,
                api_key=settings.atlan_api_key
            )
        else:
            client = AtlanClient(base_url=settings.atlan_base_url)

        # Try to get current user to test connection
        user = client.user.get_current()

        return {"success": True, "user": user.username if user else "unknown"}

    except Exception as e:
        logger.error(f"Atlan test failed: {e}")
        return {"success": False, "error": str(e)}


async def _get_settings() -> AppSettings:
    """Helper to get settings from state store."""
    try:
        with DaprClient() as client:
            state = client.get_state(store_name=DAPR_STORE_NAME, key=SETTINGS_KEY)
            if state.data:
                return AppSettings(**json.loads(state.data))
    except Exception as e:
        logger.warning(f"Could not load settings from Dapr: {e}")

    return AppSettings()


def get_settings_sync() -> AppSettings:
    """Synchronous helper to get settings from state store."""
    try:
        with DaprClient() as client:
            state = client.get_state(store_name=DAPR_STORE_NAME, key=SETTINGS_KEY)
            if state.data:
                return AppSettings(**json.loads(state.data))
    except Exception as e:
        logger.warning(f"Could not load settings from Dapr: {e}")

    return AppSettings()
