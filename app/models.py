"""Data models for the Glossary Generator application."""

from enum import Enum
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import uuid


def _utcnow() -> datetime:
    """Get current UTC time in a way compatible with Temporal sandbox."""
    return datetime.now(timezone.utc)


class TermStatus(str, Enum):
    """Status of a glossary term draft."""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    PUBLISHED = "published"


class GlossaryTermDraft(BaseModel):
    """A draft glossary term generated from metadata."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    definition: str
    short_description: Optional[str] = None
    examples: List[str] = Field(default_factory=list)
    synonyms: List[str] = Field(default_factory=list)
    source_assets: List[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "medium"
    status: TermStatus = TermStatus.DRAFT
    target_glossary_qn: str
    query_frequency: int = 0
    user_access_count: int = 0
    edited_definition: Optional[str] = None
    reviewer_notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def get_final_definition(self) -> str:
        """Return the edited definition if available, otherwise the generated one."""
        return self.edited_definition or self.definition


class AssetMetadata(BaseModel):
    """Metadata extracted from an Atlan asset."""

    qualified_name: str
    name: str
    type_name: str
    description: Optional[str] = None
    user_description: Optional[str] = None
    columns: List["ColumnMetadata"] = Field(default_factory=list)
    popularity_score: float = 0.0
    view_count: int = 0
    query_count: int = 0
    user_count: int = 0
    tags: List[str] = Field(default_factory=list)
    classifications: List[str] = Field(default_factory=list)
    owner: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None


class ColumnMetadata(BaseModel):
    """Metadata for a column within a table."""

    name: str
    data_type: Optional[str] = None
    description: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_nullable: bool = True


class UsageSignals(BaseModel):
    """Usage signals for an asset."""

    qualified_name: str
    query_frequency: int = 0
    unique_users: int = 0
    last_accessed: Optional[datetime] = None
    popularity_score: float = 0.0


class WorkflowConfig(BaseModel):
    """Configuration for the glossary generation workflow."""

    target_glossary_qn: str
    asset_types: List[str] = Field(default=["Table", "View", "MaterializedView"])
    max_assets: int = 100
    min_popularity_score: float = 0.0
    batch_size: int = 10
    include_columns: bool = True


class BatchResult(BaseModel):
    """Result of processing a batch of assets."""

    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    terms_generated: int = 0
    terms_failed: int = 0
    term_ids: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class GenerationResult(BaseModel):
    """Overall result of the glossary generation workflow."""

    workflow_id: str
    total_assets_processed: int = 0
    total_terms_generated: int = 0
    total_terms_failed: int = 0
    batches: List[BatchResult] = Field(default_factory=list)
    status: str = "completed"
    error_message: Optional[str] = None


class AppSettings(BaseModel):
    """Application settings stored in state store."""

    anthropic_api_key: Optional[str] = None
    atlan_api_key: Optional[str] = None
    atlan_base_url: Optional[str] = None
    llm_proxy_url: str = "https://llmproxy.atlan.dev"
    claude_model: str = "claude-sonnet-4.5"
    default_glossary_qn: Optional[str] = None

    def is_configured(self) -> bool:
        """Check if required settings are configured."""
        return bool(self.anthropic_api_key and self.atlan_base_url)

    def mask_key(self, key: Optional[str]) -> Optional[str]:
        """Mask an API key for display."""
        if not key:
            return None
        if len(key) <= 8:
            return "****"
        return f"{key[:4]}...{key[-4:]}"

    def to_display(self) -> dict:
        """Return settings with masked keys for UI display."""
        return {
            "anthropic_api_key": self.mask_key(self.anthropic_api_key),
            "atlan_api_key": self.mask_key(self.atlan_api_key),
            "atlan_base_url": self.atlan_base_url,
            "llm_proxy_url": self.llm_proxy_url,
            "claude_model": self.claude_model,
            "default_glossary_qn": self.default_glossary_qn,
            "is_configured": self.is_configured(),
        }


# Update forward references
AssetMetadata.model_rebuild()
