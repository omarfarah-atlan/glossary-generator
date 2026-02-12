"""Local test script for DAX formula processing without Atlan."""

import os
import asyncio
from dotenv import load_dotenv
from app.models import AssetMetadata, ColumnMetadata
from generators.context_builder import ContextBuilder
from generators.prompts import PromptTemplates
from clients.llm_client import ClaudeClient

# Load environment variables
load_dotenv()


async def test_dax_formula_generation():
    """Test DAX formula processing with a sample PowerBI Measure."""

    # Get DAX formula from environment
    dax_formula = os.getenv("DAX_FORMULA", "[Qty] * [Unit Price]")

    print("=" * 80)
    print("Testing PowerBI Measure DAX Formula Processing")
    print("=" * 80)
    print(f"\nDAX Formula: {dax_formula}\n")

    # Create a sample PowerBI Measure asset
    sample_measure = AssetMetadata(
        qualified_name="default/powerbi/workspace1/dataset1/total_revenue",
        name="Total Revenue",
        type_name="PowerBIMeasure",
        description="Calculates total revenue from quantity and unit price",
        dax_expression=dax_formula,
        dataset_qualified_name="default/powerbi/workspace1/dataset1",
        workspace_qualified_name="default/powerbi/workspace1",
        popularity_score=0.85,
        view_count=150,
        tags=["finance", "revenue", "kpi"],
        columns=[
            ColumnMetadata(
                name="Qty",
                data_type="Integer",
                description="Quantity of items sold"
            ),
            ColumnMetadata(
                name="Unit Price",
                data_type="Currency",
                description="Price per unit"
            )
        ]
    )

    # Build context
    context_builder = ContextBuilder()
    context = context_builder.build_asset_context(sample_measure)

    print("Context Built:")
    print(f"  - Name: {context['name']}")
    print(f"  - Type: {context['type']}")
    print(f"  - DAX Expression: {context.get('dax_expression')}")
    print(f"  - Dataset: {context.get('dataset')}")
    print(f"  - Workspace: {context.get('workspace')}")
    print()

    # Generate prompt
    prompt = PromptTemplates.term_definition_prompt(
        asset_name=sample_measure.name,
        asset_type=sample_measure.type_name,
        description=sample_measure.description,
        columns=context.get("columns"),
        usage_stats=context.get("usage_stats"),
        dax_expression=context.get("dax_expression")
    )

    print("Generated Prompt Preview:")
    print("-" * 80)
    print(prompt[:500] + "...")
    print("-" * 80)
    print()

    # Generate term definition using Claude
    try:
        llm_client = ClaudeClient()
        print("Generating business glossary term with Claude AI...")
        print()

        result = await llm_client.generate_term_definition(
            asset_name=sample_measure.name,
            asset_type=sample_measure.type_name,
            description=sample_measure.description,
            columns=context.get("columns"),
            usage_stats=context.get("usage_stats"),
            dax_expression=context.get("dax_expression")
        )

        print("✓ Successfully Generated Glossary Term:")
        print("=" * 80)
        print(f"Name: {result.get('name')}")
        print(f"\nDefinition:\n{result.get('definition')}")
        print(f"\nShort Description:\n{result.get('short_description')}")
        print(f"\nExamples:\n" + "\n".join(f"  - {ex}" for ex in result.get('examples', [])))
        print(f"\nSynonyms: {', '.join(result.get('synonyms', []))}")
        print(f"\nConfidence: {result.get('confidence')}")
        print("=" * 80)

    except Exception as e:
        print(f"✗ Error generating term: {e}")
        print("\nMake sure you have:")
        print("  1. ANTHROPIC_API_KEY set in .env")
        print("  2. Active internet connection")
        print("  3. Valid Claude API key with credits")


async def test_complex_dax():
    """Test with a more complex DAX formula."""

    complex_dax = """
    Total Revenue YTD =
    CALCULATE(
        SUMX(
            Sales,
            Sales[Quantity] * RELATED(Product[UnitPrice])
        ),
        DATESYTD('Date'[Date])
    )
    """

    print("\n\nTesting Complex DAX Formula:")
    print("=" * 80)
    print(complex_dax)
    print("=" * 80)

    sample_measure = AssetMetadata(
        qualified_name="default/powerbi/workspace1/dataset1/revenue_ytd",
        name="Total Revenue YTD",
        type_name="PowerBIMeasure",
        description="Year-to-date total revenue",
        dax_expression=complex_dax,
        dataset_qualified_name="default/powerbi/workspace1/dataset1",
        workspace_qualified_name="default/powerbi/workspace1",
        popularity_score=0.92,
        view_count=250,
        tags=["finance", "revenue", "ytd", "kpi"]
    )

    try:
        llm_client = ClaudeClient()
        context_builder = ContextBuilder()
        context = context_builder.build_asset_context(sample_measure)

        result = await llm_client.generate_term_definition(
            asset_name=sample_measure.name,
            asset_type=sample_measure.type_name,
            description=sample_measure.description,
            dax_expression=context.get("dax_expression")
        )

        print("\n✓ Successfully Generated Term for Complex DAX:")
        print(f"Name: {result.get('name')}")
        print(f"\nDefinition:\n{result.get('definition')}")
        print(f"\nConfidence: {result.get('confidence')}")
        print("=" * 80)

    except Exception as e:
        print(f"✗ Error: {e}")


async def main():
    """Run all tests."""
    print("\n🚀 Starting PowerBI DAX Formula Testing\n")

    # Test basic DAX formula
    await test_dax_formula_generation()

    # Test complex DAX formula
    await test_complex_dax()

    print("\n✓ All tests completed!\n")


if __name__ == "__main__":
    asyncio.run(main())
