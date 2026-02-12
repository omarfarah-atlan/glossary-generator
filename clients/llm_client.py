"""Claude API client via LiteLLM proxy (OpenAI-compatible endpoint)."""

import os
import json
import logging
from typing import Optional
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class ClaudeClient:
    """Client for interacting with Claude via Atlan's LiteLLM proxy.

    llmproxy.atlan.dev is an OpenAI-compatible endpoint that routes to Claude.
    Uses /v1/chat/completions endpoint with Claude model names.
    """

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, base_url: Optional[str] = None):
        # Load settings from persistent store (file + Dapr)
        from app.settings_store import load_settings
        settings = load_settings()

        self.api_key = api_key or settings.anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.model = model or settings.claude_model or "claude-sonnet-4.5"
        self.base_url = base_url or settings.llm_proxy_url or os.environ.get("LLM_PROXY_URL") or "https://llmproxy.atlan.dev"

        if not self.api_key:
            raise ValueError("LLM API key not configured. Set it in Settings or ANTHROPIC_API_KEY environment variable.")

        logger.info(f"Initializing LLM client with proxy: {self.base_url}, model: {self.model}")
        # Use OpenAI client with LiteLLM proxy base URL
        self._client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

    async def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        """Generate text from a prompt using Claude via LiteLLM."""
        try:
            response = await self._client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error generating text with Claude: {e}")
            raise

    async def generate_json(self, prompt: str, max_tokens: int = 2000) -> dict:
        """Generate JSON from a prompt using Claude via LiteLLM."""
        try:
            response = await self._client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.choices[0].message.content

            # Extract JSON from the response
            json_start = text.find("{")
            json_end = text.rfind("}") + 1
            if json_start != -1 and json_end > json_start:
                json_str = text[json_start:json_end]
                return json.loads(json_str)
            else:
                raise ValueError("No valid JSON found in response")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from Claude response: {e}")
            raise
        except Exception as e:
            logger.error(f"Error generating JSON with Claude: {e}")
            raise

    async def generate_json_array(self, prompt: str, max_tokens: int = 4000) -> list:
        """Generate a JSON array from a prompt using Claude via LiteLLM."""
        try:
            response = await self._client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.choices[0].message.content

            # Extract JSON array from the response
            json_start = text.find("[")
            json_end = text.rfind("]") + 1
            if json_start != -1 and json_end > json_start:
                json_str = text[json_start:json_end]
                return json.loads(json_str)
            else:
                raise ValueError("No valid JSON array found in response")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON array from Claude response: {e}")
            raise
        except Exception as e:
            logger.error(f"Error generating JSON array with Claude: {e}")
            raise

    async def classify_columns(
        self,
        asset_name: str,
        asset_type: str,
        description: Optional[str] = None,
        columns: Optional[list] = None,
    ) -> list:
        """Classify columns for an asset to determine which deserve glossary terms."""
        from generators.prompts import PromptTemplates

        prompt = PromptTemplates.column_classification_prompt(
            asset_name=asset_name,
            asset_type=asset_type,
            description=description,
            columns=columns,
        )

        return await self.generate_json_array(prompt)

    async def generate_column_term_definition(
        self,
        column_name: str,
        column_data_type: Optional[str] = None,
        column_description: Optional[str] = None,
        term_type: str = "business_term",
        parent_asset_name: Optional[str] = None,
        parent_asset_type: Optional[str] = None,
        parent_description: Optional[str] = None,
        sibling_columns: Optional[list] = None,
        sql_definition: Optional[str] = None,
        custom_context: Optional[str] = None,
    ) -> dict:
        """Generate a glossary term definition for a specific column."""
        from generators.prompts import PromptTemplates

        prompt = PromptTemplates.column_term_definition_prompt(
            column_name=column_name,
            column_data_type=column_data_type,
            column_description=column_description,
            term_type=term_type,
            parent_asset_name=parent_asset_name,
            parent_asset_type=parent_asset_type,
            parent_description=parent_description,
            sibling_columns=sibling_columns,
            sql_definition=sql_definition,
            custom_context=custom_context,
        )

        return await self.generate_json(prompt)

    async def generate_term_definition(
        self,
        asset_name: str,
        asset_type: str,
        description: Optional[str] = None,
        columns: Optional[list] = None,
        usage_stats: Optional[dict] = None,
        sql_definition: Optional[str] = None,
        dbt_context: Optional[dict] = None,
        custom_context: Optional[str] = None,
    ) -> dict:
        """Generate a glossary term definition for an asset."""
        from generators.prompts import PromptTemplates

        prompt = PromptTemplates.term_definition_prompt(
            asset_name=asset_name,
            asset_type=asset_type,
            description=description,
            columns=columns,
            usage_stats=usage_stats,
            sql_definition=sql_definition,
            dbt_context=dbt_context,
            custom_context=custom_context,
        )

        return await self.generate_json(prompt)
