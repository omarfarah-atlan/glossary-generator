"""Unit tests for the data models."""

import pytest
from datetime import datetime

from app.models import (
    GlossaryTermDraft,
    TermStatus,
    AssetMetadata,
    ColumnMetadata,
    UsageSignals,
    WorkflowConfig,
    BatchResult,
    GenerationResult,
)


class TestGlossaryTermDraft:
    """Tests for the GlossaryTermDraft model."""

    def test_create_minimal(self):
        """Test creating a minimal term draft."""
        term = GlossaryTermDraft(
            name="Test Term",
            definition="A test definition",
            target_glossary_qn="test/glossary",
        )

        assert term.name == "Test Term"
        assert term.definition == "A test definition"
        assert term.status == TermStatus.DRAFT
        assert term.confidence == "medium"
        assert term.id is not None

    def test_get_final_definition_no_edit(self):
        """Test that original definition is returned when no edit."""
        term = GlossaryTermDraft(
            name="Test",
            definition="Original definition",
            target_glossary_qn="test/glossary",
        )

        assert term.get_final_definition() == "Original definition"

    def test_get_final_definition_with_edit(self):
        """Test that edited definition is returned when present."""
        term = GlossaryTermDraft(
            name="Test",
            definition="Original definition",
            edited_definition="Edited definition",
            target_glossary_qn="test/glossary",
        )

        assert term.get_final_definition() == "Edited definition"

    def test_status_transitions(self):
        """Test status can be changed."""
        term = GlossaryTermDraft(
            name="Test",
            definition="Test",
            target_glossary_qn="test/glossary",
        )

        assert term.status == TermStatus.DRAFT

        term.status = TermStatus.PENDING_REVIEW
        assert term.status == TermStatus.PENDING_REVIEW

        term.status = TermStatus.APPROVED
        assert term.status == TermStatus.APPROVED


class TestAssetMetadata:
    """Tests for the AssetMetadata model."""

    def test_create_with_columns(self):
        """Test creating asset metadata with columns."""
        asset = AssetMetadata(
            qualified_name="db/schema/table",
            name="users",
            type_name="Table",
            columns=[
                ColumnMetadata(name="id", data_type="INTEGER"),
                ColumnMetadata(name="name", data_type="VARCHAR"),
            ],
        )

        assert len(asset.columns) == 2
        assert asset.columns[0].name == "id"

    def test_default_values(self):
        """Test default values are set correctly."""
        asset = AssetMetadata(
            qualified_name="test",
            name="test",
            type_name="Table",
        )

        assert asset.popularity_score == 0.0
        assert asset.view_count == 0
        assert asset.tags == []
        assert asset.columns == []


class TestUsageSignals:
    """Tests for the UsageSignals model."""

    def test_create_with_signals(self):
        """Test creating usage signals."""
        signals = UsageSignals(
            qualified_name="test",
            query_frequency=100,
            unique_users=25,
            popularity_score=0.85,
        )

        assert signals.query_frequency == 100
        assert signals.unique_users == 25
        assert signals.popularity_score == 0.85


class TestWorkflowConfig:
    """Tests for the WorkflowConfig model."""

    def test_default_values(self):
        """Test default configuration values."""
        config = WorkflowConfig(target_glossary_qn="test/glossary")

        assert config.max_assets == 100
        assert "Table" in config.asset_types
        assert config.batch_size == 10

    def test_custom_values(self):
        """Test custom configuration values."""
        config = WorkflowConfig(
            target_glossary_qn="test/glossary",
            max_assets=50,
            asset_types=["View"],
            batch_size=5,
        )

        assert config.max_assets == 50
        assert config.asset_types == ["View"]
        assert config.batch_size == 5


class TestBatchResult:
    """Tests for the BatchResult model."""

    def test_create_result(self):
        """Test creating a batch result."""
        result = BatchResult()

        assert result.terms_generated == 0
        assert result.terms_failed == 0
        assert result.term_ids == []
        assert result.errors == []


class TestGenerationResult:
    """Tests for the GenerationResult model."""

    def test_create_result(self):
        """Test creating a generation result."""
        result = GenerationResult(workflow_id="test-123")

        assert result.workflow_id == "test-123"
        assert result.status == "completed"
        assert result.total_terms_generated == 0
