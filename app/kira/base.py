import os

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

class KiraAI:
    def __init__(
        self,
        model: str = "kira-3.5-flash",
    ):
        self.api_key = os.getenv("KIRA_API_KEY")
        self.base_url = os.getenv("KIRA_BASE_URL")
        self.model = model

        if not self.api_key:
            raise ValueError(
                "KIRA_API_KEY is not configured. "
                "Add KIRA_API_KEY to the .env file or environment."
            )

        if not self.base_url:
            raise ValueError(
                "KIRA_BASE_URL is not configured. "
                "Add KIRA_BASE_URL to the .env file or environment."
            )

        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
        )

    def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
        max_tokens: int | None = None,
    ) -> str:
        params = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
            "temperature": temperature,
        }

        if max_tokens is not None:
            params["max_tokens"] = max_tokens

        completion = self.client.chat.completions.create(**params)

        return completion.choices[0].message.content or ""
    
kira_ai = KiraAI()