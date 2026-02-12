"""HTTP handlers for the review UI."""

import json
import logging
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.models import GlossaryTermDraft, TermStatus, AppSettings
from app.settings_store import load_settings, save_settings

logger = logging.getLogger(__name__)

DAPR_STORE_NAME = "statestore"

# Dapr availability tracking for fast-fail
_dapr_available: Optional[bool] = None
_dapr_check_timestamp: Optional[datetime] = None
_DAPR_RETRY_INTERVAL = timedelta(minutes=5)


def _get_dapr_client():
    """Get DaprClient with fast-fail if known unavailable."""
    global _dapr_available, _dapr_check_timestamp

    # Fast-fail if known unavailable and within retry interval
    if _dapr_available is False:
        if _dapr_check_timestamp and datetime.now() - _dapr_check_timestamp < _DAPR_RETRY_INTERVAL:
            return None

    try:
        from dapr.clients import DaprClient
        return DaprClient()
    except Exception:
        return None


def _mark_dapr_available(available: bool):
    """Mark Dapr availability status."""
    global _dapr_available, _dapr_check_timestamp
    _dapr_available = available
    _dapr_check_timestamp = datetime.now()

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
    llm_proxy_url: Optional[str] = None
    claude_model: Optional[str] = None
    default_glossary_qn: Optional[str] = None


def get_all_term_ids() -> List[str]:
    """Get all term IDs from state store by scanning batch indices."""
    term_ids = []

    client = _get_dapr_client()
    if client is None:
        return term_ids

    try:
        from dapr.clients import DaprClient
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
        _mark_dapr_available(True)
    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr unavailable for batch scan: {type(e).__name__}")
    return term_ids


def get_terms_by_status(status: Optional[TermStatus] = None) -> List[GlossaryTermDraft]:
    """Get all terms, optionally filtered by status."""
    terms = []

    # Get term IDs first (uses fast-fail internally)
    term_ids = get_all_term_ids()
    if not term_ids:
        return terms

    client = _get_dapr_client()
    if client is None:
        return terms

    try:
        from dapr.clients import DaprClient
        with DaprClient() as client:
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
        _mark_dapr_available(True)
    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr unavailable for term loading: {type(e).__name__}")

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
    if _get_dapr_client() is None:
        raise HTTPException(status_code=503, detail="State store unavailable")

    try:
        from dapr.clients import DaprClient
        with DaprClient() as client:
            key = f"glossary_term_{term_id}"
            state = client.get_state(store_name=DAPR_STORE_NAME, key=key)

            if not state.data:
                raise HTTPException(status_code=404, detail="Term not found")

            _mark_dapr_available(True)
            term_data = json.loads(state.data)
            return term_data

    except HTTPException:
        raise
    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr error getting term: {type(e).__name__}")
        raise HTTPException(status_code=503, detail="State store unavailable")


@router.post("/api/v1/terms/{term_id}/approve")
async def approve_term(term_id: str, request: ApproveRequest):
    """Approve a term with optional edits."""
    if _get_dapr_client() is None:
        raise HTTPException(status_code=503, detail="State store unavailable")

    try:
        from dapr.clients import DaprClient
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

            _mark_dapr_available(True)
            return {"status": "approved", "term_id": term_id}

    except HTTPException:
        raise
    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr error approving term: {type(e).__name__}")
        raise HTTPException(status_code=503, detail="State store unavailable")


@router.post("/api/v1/terms/{term_id}/reject")
async def reject_term(term_id: str, request: RejectRequest):
    """Reject a term with a reason."""
    if _get_dapr_client() is None:
        raise HTTPException(status_code=503, detail="State store unavailable")

    try:
        from dapr.clients import DaprClient
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

            _mark_dapr_available(True)
            return {"status": "rejected", "term_id": term_id}

    except HTTPException:
        raise
    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr error rejecting term: {type(e).__name__}")
        raise HTTPException(status_code=503, detail="State store unavailable")


@router.post("/api/v1/terms/bulk-approve")
async def bulk_approve(request: BulkApproveRequest):
    """Bulk approve multiple terms."""
    results = {"approved": 0, "failed": 0, "errors": []}

    if _get_dapr_client() is None:
        raise HTTPException(status_code=503, detail="State store unavailable")

    try:
        from dapr.clients import DaprClient
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

            _mark_dapr_available(True)

    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr error in bulk approve: {type(e).__name__}")
        raise HTTPException(status_code=503, detail="State store unavailable")

    return results


@router.post("/api/v1/terms/publish")
async def publish_terms(request: PublishRequest):
    """Publish approved terms to Atlan glossary."""
    from clients.atlan_client import AtlanMetadataClient

    results = {"published": 0, "failed": 0, "errors": []}

    if _get_dapr_client() is None:
        raise HTTPException(status_code=503, detail="State store unavailable")

    atlan_client = AtlanMetadataClient()

    try:
        from dapr.clients import DaprClient
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

            _mark_dapr_available(True)

    except Exception as e:
        _mark_dapr_available(False)
        logger.debug(f"Dapr error in publish: {type(e).__name__}")
        raise HTTPException(status_code=503, detail="State store unavailable")

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
        settings = load_settings()
        return settings.to_display()
    except Exception as e:
        logger.error(f"Error getting settings: {e}")
        return AppSettings().to_display()


@router.post("/api/v1/settings")
async def update_settings(request: SettingsUpdateRequest):
    """Update application settings (persisted to file and Dapr)."""
    try:
        # Load existing settings
        existing = load_settings()

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

        # Save to both file and Dapr for persistence
        if save_settings(updated):
            return {
                "status": "saved",
                "settings": updated.to_display(),
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to save settings")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/settings/test-anthropic")
async def test_anthropic_connection():
    """Test the Anthropic API connection via Atlan's LLM proxy."""
    try:
        settings = load_settings()

        if not settings.anthropic_api_key:
            return {"success": False, "error": "Anthropic API key not configured"}

        from anthropic import Anthropic

        # Use Atlan's LLM proxy as the base URL
        client = Anthropic(
            api_key=settings.anthropic_api_key,
            base_url=settings.llm_proxy_url
        )
        # Make a minimal API call to test
        response = client.messages.create(
            model=settings.claude_model,
            max_tokens=10,
            messages=[{"role": "user", "content": "Hi"}]
        )

        return {"success": True, "model": settings.claude_model, "proxy": settings.llm_proxy_url}

    except Exception as e:
        logger.error(f"Anthropic test failed: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/v1/settings/test-atlan")
async def test_atlan_connection():
    """Test the Atlan API connection."""
    try:
        settings = load_settings()

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


def get_settings_sync() -> AppSettings:
    """Synchronous helper to get settings (for backward compatibility)."""
    return load_settings()
