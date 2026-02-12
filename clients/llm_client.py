"""Claude API client for generating glossary term definitions."""

import os
import json
import logging
from typing import Optional
from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)


def _get_settings_from_store() -> dict:
    """Load settings from Dapr state store."""
    try:
        from dapr.clients import DaprClient
        with DaprClient() as client:
            state = client.get_state(store_name="statestore", key="app_settings")
            if state.data:
                return json.loads(state.data)
    except Exception as e:
        logger.debug(f"Could not load settings from Dapr: {e}")
    return {}


class ClaudeClient:
    """Client for interacting with Claude API to generate term definitions."""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        # Try to load from state store first, then fall back to env vars
        settings = _get_settings_from_store()

        self.api_key = api_key or settings.get("anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY")
        self.model = model or settings.get("claude_model") or "claude-sonnet-4-20250514"

        if not self.api_key:
            raise ValueError("Anthropic API key not configured. Set it in Settings or ANTHROPIC_API_KEY environment variable.")

        self._client = AsyncAnthropic(api_key=self.api_key)

    async def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        """Generate text from a prompt using Claude."""
        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Error generating text with Claude: {e}")
            raise

    async def generate_json(self, prompt: str, max_tokens: int = 2000) -> dict:
        """Generate JSON from a prompt using Claude."""
        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text

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

    async def generate_term_definition(
        self,
        asset_name: str,
        asset_type: str,
        description: Optional[str] = None,
        columns: Optional[list] = None,
        usage_stats: Optional[dict] = None,
        dax_expression: Optional[str] = None
    ) -> dict:
        """Generate a glossary term definition for an asset."""
        from generators.prompts import PromptTemplates

        prompt = PromptTemplates.term_definition_prompt(
            asset_name=asset_name,
            asset_type=asset_type,
            description=description,
            columns=columns,
            usage_stats=usage_stats,
            dax_expression=dax_expression
        )

        return await self.generate_json(prompt)
