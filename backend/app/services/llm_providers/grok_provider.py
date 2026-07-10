import logging
import re

import httpx

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)


class GrokProvider(BaseLLMProvider):
    def __init__(self, api_key: str, model: str = "grok-4", base_url: str = "https://api.x.ai/v1"):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    async def analyze(self, prompt: str, system_prompt: str = "") -> str:
        if not self.api_key:
            raise ValueError("Grok API key not configured")

        # Build messages with system role
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        result = await self._call(messages)

        # Strip <think>...</think> tags from reasoning models
        result = re.sub(r'<think>.*?</think>', '', result, flags=re.DOTALL).strip()
        return result

    async def _call(self, messages: list[dict]) -> str:
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.3,
            "stream": False,
        }

        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

            # Fallback: if proxy rejects system role (403/502), merge into user message and retry
            if response.status_code in (403, 502) and any(m["role"] == "system" for m in messages):
                logger.warning(
                    "Grok proxy rejected system role (HTTP %s); retrying with merged user message",
                    response.status_code,
                )
                system_text = next((m["content"] for m in messages if m["role"] == "system"), "")
                user_text = next((m["content"] for m in messages if m["role"] == "user"), "")
                merged_messages = [{"role": "user", "content": f"{system_text}\n\n{user_text}"}]
                payload["messages"] = merged_messages

                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )

            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

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
            logger.debug(f"Grok availability check failed: {e}")
            return False
