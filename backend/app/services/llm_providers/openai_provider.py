import logging
from typing import Optional

import httpx

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OpenAIProvider(BaseLLMProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o", base_url: str = "https://api.openai.com/v1"):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    async def analyze(self, prompt: str, system_prompt: str = "") -> str:
        # Synchronous OpenAI analysis was retired.  All paid news and calendar
        # work must pass through the durable Responses worker and its budgets.
        raise RuntimeError("persistent_analysis_job_required")

    async def is_available(self) -> bool:
        if not self.api_key:
            return False
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(
                    f"{self.base_url}/models",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
                return response.status_code == 200
        except Exception as e:
            logger.debug(f"OpenAI availability check failed: {e}")
            return False
