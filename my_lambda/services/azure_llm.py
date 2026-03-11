# services/azure_llm.py

import time
from openai import AzureOpenAI, RateLimitError
from config import AZURE_API_KEY, AZURE_ENDPOINT, AZURE_API_VERSION, AZURE_DEPLOYMENT
from pydantic import BaseModel, Field

class AzureLLMAgent:
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=AZURE_API_KEY,
            azure_endpoint=AZURE_ENDPOINT,
            api_version=AZURE_API_VERSION
        )
        self.model = AZURE_DEPLOYMENT

    def complete(self, prompt: str) -> str:
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an AI data extractor."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1024,
                temperature=0.2
            )
            return response.choices[0].message.content.strip()

        except RateLimitError:
            print("⚠️ Rate limit hit. Retrying after 5 seconds...")
            time.sleep(5)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an AI data extractor."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1024,
                temperature=0.2
            )
            return response.choices[0].message.content.strip()

        except Exception as e:
            print(f"❌ LLM Error: {e}")
            return "NA"


class RequestedField(BaseModel):
    field_name: str = Field(..., alias="fieldName")
    field_datatype: str = Field(..., alias="fieldDataType")
    field_desc: str = Field(..., alias="fieldDescription")
