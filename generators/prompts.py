"""Prompt templates for LLM-based term generation."""

from typing import List, Optional


class PromptTemplates:
    """Templates for generating glossary term definitions."""

    @staticmethod
    def term_definition_prompt(
        asset_name: str,
        asset_type: str,
        description: Optional[str] = None,
        columns: Optional[List[dict]] = None,
        usage_stats: Optional[dict] = None,
        dax_expression: Optional[str] = None
    ) -> str:
        """Generate a prompt for creating a glossary term definition."""

        prompt = f"""You are a data steward helping to create a business glossary. Generate a comprehensive business glossary term definition for the following data asset.

## Asset Information
- **Name**: {asset_name}
- **Type**: {asset_type}
"""

        if description:
            prompt += f"- **Existing Description**: {description}\n"

        # Add DAX expression for PowerBI measures
        if dax_expression and asset_type == "PowerBIMeasure":
            prompt += f"\n## DAX Expression\n```dax\n{dax_expression}\n```\n"
            prompt += "\nThis is a Power BI measure. Focus on what business metric this calculates based on the DAX formula.\n"

        if columns:
            prompt += "\n## Columns\n"
            for col in columns[:20]:  # Limit to 20 columns
                col_desc = f"  - {col.get('name', 'unknown')}"
                if col.get('data_type'):
                    col_desc += f" ({col['data_type']})"
                if col.get('description'):
                    col_desc += f": {col['description']}"
                prompt += col_desc + "\n"

        if usage_stats:
            prompt += f"""
## Usage Statistics
- Query Frequency: {usage_stats.get('query_frequency', 'Unknown')}
- Unique Users: {usage_stats.get('unique_users', 'Unknown')}
- Popularity Score: {usage_stats.get('popularity_score', 'Unknown')}
"""

        # Add measure-specific instructions
        if asset_type == "PowerBIMeasure":
            prompt += """
## Instructions
Based on the information above, generate a business glossary term definition for this Power BI measure. Focus on:
1. What business metric or KPI this measure calculates
2. How the DAX formula defines the calculation logic
3. How business users would interpret and use this metric
4. The business context and decision-making this metric supports

Respond with a JSON object in this exact format:
{
    "name": "Business-friendly metric name (e.g., 'Total Revenue', 'Customer Lifetime Value')",
    "definition": "A comprehensive 2-4 sentence definition explaining what business metric this calculates, how it's computed (in business terms), and its significance",
    "short_description": "A one-sentence summary of the metric",
    "examples": ["Example business use case 1", "Example business use case 2"],
    "synonyms": ["Alternative metric name 1", "Alternative metric name 2"],
    "confidence": "high|medium|low"
}

Set confidence based on:
- "high": Clear DAX formula with descriptive measure name and/or description
- "medium": DAX formula present but limited context
- "low": Complex DAX formula with limited information to infer business meaning

Respond ONLY with the JSON object, no additional text."""
        else:
            prompt += """
## Instructions
Based on the information above, generate a business glossary term definition. Focus on:
1. What this data represents in business terms
2. How it might be used by analysts and business users
3. Key concepts and relationships

Respond with a JSON object in this exact format:
{
    "name": "Business-friendly term name (convert technical names to readable format)",
    "definition": "A comprehensive 2-4 sentence definition explaining what this data represents and its business significance",
    "short_description": "A one-sentence summary",
    "examples": ["Example use case 1", "Example use case 2"],
    "synonyms": ["Alternative term 1", "Alternative term 2"],
    "confidence": "high|medium|low"
}

Set confidence based on:
- "high": Clear existing description and good metadata
- "medium": Some context available but not comprehensive
- "low": Limited information, mostly inferred

Respond ONLY with the JSON object, no additional text."""

        return prompt

    @staticmethod
    def batch_definition_prompt(
        assets: List[dict]
    ) -> str:
        """Generate a prompt for batch processing multiple assets."""

        prompt = """You are a data steward helping to create a business glossary. Generate business glossary term definitions for the following data assets.

## Assets
"""
        for i, asset in enumerate(assets, 1):
            prompt += f"""
### Asset {i}: {asset.get('name', 'Unknown')}
- Type: {asset.get('type', 'Unknown')}
"""
            if asset.get('description'):
                prompt += f"- Description: {asset['description']}\n"
            if asset.get('columns'):
                prompt += f"- Columns: {', '.join(c.get('name', '') for c in asset['columns'][:10])}\n"

        prompt += """
## Instructions
For each asset, generate a business glossary term. Respond with a JSON array where each element has this format:
{
    "asset_name": "Original asset name",
    "name": "Business-friendly term name",
    "definition": "2-4 sentence definition",
    "short_description": "One-sentence summary",
    "examples": ["Example 1", "Example 2"],
    "synonyms": ["Synonym 1"],
    "confidence": "high|medium|low"
}

Respond ONLY with the JSON array, no additional text."""

        return prompt

    @staticmethod
    def refinement_prompt(
        original_definition: str,
        feedback: str
    ) -> str:
        """Generate a prompt for refining a definition based on feedback."""

        return f"""You are a data steward refining a glossary term definition.

## Original Definition
{original_definition}

## Feedback
{feedback}

## Instructions
Improve the definition based on the feedback. Maintain the same JSON format:
{{
    "definition": "Improved definition",
    "short_description": "Updated one-sentence summary"
}}

Respond ONLY with the JSON object."""
