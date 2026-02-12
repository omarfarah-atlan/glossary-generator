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
        sql_definition: Optional[str] = None,
        dbt_context: Optional[dict] = None,
        custom_context: Optional[str] = None,
        term_types: Optional[List[str]] = None,
    ) -> str:
        """Generate a prompt for creating a glossary term definition."""

        prompt = f"""You are a data steward helping to create a business glossary. Generate a comprehensive business glossary term definition for the following data asset.

## Asset Information
- **Name**: {asset_name}
- **Type**: {asset_type}
"""

        if description:
            prompt += f"- **Existing Description**: {description}\n"

        if columns:
            prompt += "\n## Columns\n"
            for col in columns[:20]:  # Limit to 20 columns
                col_desc = f"  - {col.get('name', 'unknown')}"
                if col.get('data_type'):
                    col_desc += f" ({col['data_type']})"
                if col.get('description'):
                    col_desc += f": {col['description']}"
                prompt += col_desc + "\n"

        if sql_definition:
            prompt += f"""
## SQL Transformation
The following SQL shows how this asset is constructed. Use it to explain the transformation logic in business terms:
```sql
{sql_definition}
```
"""

        if dbt_context:
            prompt += "\n## dbt Transformation Context\n"
            if dbt_context.get('model_name'):
                prompt += f"- **dbt Model**: {dbt_context['model_name']}\n"
            if dbt_context.get('materialization_type'):
                prompt += f"- **Materialization**: {dbt_context['materialization_type']}\n"
            if dbt_context.get('raw_sql'):
                prompt += f"""
### dbt SQL
```sql
{dbt_context['raw_sql']}
```
"""

        if usage_stats:
            prompt += f"""
## Usage Statistics
- Query Frequency: {usage_stats.get('query_frequency', 'Unknown')}
- Unique Users: {usage_stats.get('unique_users', 'Unknown')}
- Popularity Score: {usage_stats.get('popularity_score', 'Unknown')}
"""

        if custom_context:
            prompt += f"""
## Additional Context (User-Provided)
{custom_context}
"""

        # Build term type guidance based on requested types
        requested = term_types or ["business_term", "metric", "dimension"]
        type_guidance = []
        if "business_term" in requested:
            type_guidance.append("""- **business_term**: A business concept derived from this asset. Define what the concept means to the organization, its role in business processes, and how business users would understand it. Example: Table "DIM_CUSTOMER" → name: "Customer", definition: "A Customer is an individual or organization that has purchased or registered for products and services...".""")
        if "metric" in requested:
            type_guidance.append("""- **metric**: A measurable business value or KPI represented by this asset. Focus on what is being measured, the calculation method, units, aggregation, and business targets. Example: Table "MONTHLY_REVENUE" → name: "Monthly Revenue", definition: "Monthly Revenue is the total income generated from all sales transactions within a calendar month, measured in the organization's base currency...".""")
        if "dimension" in requested:
            type_guidance.append("""- **dimension**: A categorical attribute used to segment, filter, or group data in analysis. Focus on the set of possible values, hierarchies, and how analysts use it. Example: Table "EMPLOYEE_DIMENSION" → name: "Employee", definition: "An Employee is a person engaged by the organization in a professional capacity. Employees are categorized by department, role, tenure, and geographic location for workforce analysis...".""")

        type_list = "\n".join(type_guidance)
        type_values = "|".join(requested)

        prompt += f"""
## Instructions
Generate a business glossary term that describes the BUSINESS CONCEPT behind this data asset — NOT the database object itself.

### Term Type
Classify this term as the most appropriate type from the requested types below, and structure the definition accordingly:
{type_list}

Pick the single best-fitting type. If the asset name contains hints like "dim", "dimension", "fact", "metric", "kpi", "revenue", "count", "rate", use those to guide your choice.

### Naming Rules (CRITICAL)
- The term name must be a clean, singular business concept: "Customer", "Revenue", "Order", "Employee"
- NEVER mirror the table/view name directly. "DIM_CUSTOMER" → "Customer", "FACT_ORDERS" → "Order", "employee_dimension" → "Employee"
- Strip ALL technical suffixes/prefixes: dim, dimension, fact, table, view, vw, tbl, stg, raw, _v, _t, src, base, mart, int
- Use singular form: "Customers" → "Customer", "Invoices" → "Invoice"
- Use title case: "monthly revenue" → "Monthly Revenue"
- The name should be what a business user would search for in a glossary

### Definition Rules
- NEVER say "this table", "this view", "this dataset", "this data asset", or "stores data about"
- Define the BUSINESS CONCEPT as if you were writing a dictionary entry
- Start with "A [concept] is..." or "The [concept] represents..."
- Explain what it means in the business, not how it is stored technically
- If SQL or columns provide context, explain the business logic in plain language

Respond with a JSON object in this exact format:
{{
    "name": "Clean singular business concept name",
    "term_type": "{type_values}",
    "definition": "2-4 sentence business concept definition (never reference the table/view)",
    "short_description": "One-sentence summary of the business concept",
    "examples": ["Example use case 1", "Example use case 2"],
    "synonyms": ["Alternative term 1", "Alternative term 2"],
    "confidence": "high|medium|low",
    "reasoning": "1-2 sentences explaining why you chose this name, type, and confidence level. Mention which metadata signals (description, columns, SQL, usage stats, naming patterns) most influenced your decisions."
}}

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
    def column_classification_prompt(
        asset_name: str,
        asset_type: str,
        description: Optional[str] = None,
        columns: Optional[List[dict]] = None,
    ) -> str:
        """Generate a prompt to classify columns and decide which deserve their own glossary terms."""

        prompt = f"""You are a data steward classifying columns in a data asset to determine which ones deserve their own business glossary terms.

## Asset Information
- **Name**: {asset_name}
- **Type**: {asset_type}
"""

        if description:
            prompt += f"- **Description**: {description}\n"

        if columns:
            prompt += "\n## Columns\n"
            for col in columns:
                col_line = f"  - **{col.get('name', 'unknown')}**"
                if col.get('data_type'):
                    col_line += f" ({col['data_type']})"
                flags = []
                if col.get('is_primary_key'):
                    flags.append("PK")
                if col.get('is_foreign_key'):
                    flags.append("FK")
                if flags:
                    col_line += f" [{', '.join(flags)}]"
                if col.get('description'):
                    col_line += f": {col['description']}"
                prompt += col_line + "\n"

        prompt += """
## Classification Rules
Classify each column into one of these term types:

- **metric**: Numeric, aggregatable values and KPIs — revenue, count, amount, rate, score, total, sum, average, conversion_rate, retention, nps_score, churn_rate, growth_rate
- **dimension**: Categorical or grouping attributes — status, region, type, segment, category, country, department
- **business_term**: Significant business concepts that don't fit the above categories

## Instructions
- Set `should_generate=true` for columns that represent meaningful business concepts worth documenting
- Set `should_generate=false` for purely technical columns (IDs, timestamps like created_at/updated_at, foreign keys, audit fields, hash columns)
- Typically 30-50% of columns deserve a term
- Provide a brief reason for each classification decision

Respond with a JSON array in this exact format:
[
    {
        "column_name": "column_name_here",
        "term_type": "metric|dimension|business_term",
        "should_generate": true,
        "reason": "Brief explanation"
    }
]

Respond ONLY with the JSON array, no additional text."""

        return prompt

    @staticmethod
    def column_term_definition_prompt(
        column_name: str,
        column_data_type: Optional[str] = None,
        column_description: Optional[str] = None,
        term_type: str = "business_term",
        parent_asset_name: Optional[str] = None,
        parent_asset_type: Optional[str] = None,
        parent_description: Optional[str] = None,
        sibling_columns: Optional[List[dict]] = None,
        sql_definition: Optional[str] = None,
        custom_context: Optional[str] = None,
    ) -> str:
        """Generate a prompt for creating a glossary term definition for a specific column."""

        prompt = f"""You are a data steward creating a business glossary term for a specific column/field in a data asset.

## Column Information
- **Column Name**: {column_name}
- **Term Type**: {term_type}
"""

        if column_data_type:
            prompt += f"- **Data Type**: {column_data_type}\n"
        if column_description:
            prompt += f"- **Existing Description**: {column_description}\n"

        if parent_asset_name:
            prompt += f"\n## Parent Asset\n- **Name**: {parent_asset_name}\n"
            if parent_asset_type:
                prompt += f"- **Type**: {parent_asset_type}\n"
            if parent_description:
                prompt += f"- **Description**: {parent_description}\n"

        if sibling_columns:
            prompt += "\n## Related Columns (same table)\n"
            for col in sibling_columns[:15]:
                col_line = f"  - {col.get('name', 'unknown')}"
                if col.get('data_type'):
                    col_line += f" ({col['data_type']})"
                if col.get('description'):
                    col_line += f": {col['description']}"
                prompt += col_line + "\n"

        if sql_definition:
            prompt += f"""
## SQL Definition
```sql
{sql_definition}
```
"""

        if custom_context:
            prompt += f"""
## Additional Context
{custom_context}
"""

        # Type-specific emphasis
        if term_type == "metric":
            prompt += """
## Type-Specific Guidance (Metric)
Focus your definition on:
- The calculation formula or how this value is derived
- Units of measurement (dollars, percentage, count, etc.)
- Aggregation method (sum, average, count, etc.)
- Grain/granularity (per-user, per-day, per-transaction, etc.)
- Related metrics that provide context
- Business thresholds or targets if inferrable
"""
        elif term_type == "dimension":
            prompt += """
## Type-Specific Guidance (Dimension)
Focus your definition on:
- The set of possible values or categories (if inferrable)
- Hierarchies this dimension belongs to (e.g., city → region → country)
- How this dimension is used for filtering or grouping in analysis
- Business meaning of key values
"""
        else:
            prompt += """
## Type-Specific Guidance (Business Term)
Focus your definition on:
- The business concept this column represents
- How it relates to business processes or workflows
- Its significance for business users and analysts
"""

        prompt += """
## Instructions
Generate a business glossary term for this column that describes the BUSINESS CONCEPT — not the column itself.

### Naming Rules
- Use a clean, singular business concept name: "total_revenue" → "Total Revenue", "customer_segment" → "Customer Segment"
- Strip technical prefixes/suffixes: _id, _key, _code, _flag, _ind, fk_, pk_
- Use title case
- The name should be what a business user would look up in a glossary

### Definition Rules
- NEVER say "this column", "this field", "this attribute", or "stores the value of"
- Define the BUSINESS CONCEPT: what it means to the business, not where it is stored
- For metrics: explain what is being measured, how it's calculated, and what units it uses
- For dimensions: explain what categories or groupings it represents and how it's used in analysis

Respond with a JSON object in this exact format:
{
    "name": "Clean business concept name in title case",
    "definition": "2-4 sentence business concept definition (never reference the column/field)",
    "short_description": "One-sentence summary",
    "examples": ["Example use case 1", "Example use case 2"],
    "synonyms": ["Alternative term 1"],
    "confidence": "high|medium|low"
}

Respond ONLY with the JSON object, no additional text."""

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
