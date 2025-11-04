import boto3
from app.config import settings
from botocore.exceptions import ClientError
import time
import random

_bedrock = boto3.client("bedrock-runtime", region_name=settings.AWS_REGION)

def converse(messages: list, system_prompt: str | None = None, tools: list | None = None,
             model_id: str | None = None, max_tokens: int = 400, temperature: float = 0.4):
    req = {
        "modelId": model_id or settings.BEDROCK_MODEL_ID,
        "messages": messages,
        "inferenceConfig": {"maxTokens": max_tokens, "temperature": temperature},
    }
    if system_prompt:
        req["system"] = [{"text": system_prompt}]
    if tools:
        req["toolConfig"] = {"tools": tools}
    # Retry on throttling with exponential backoff + jitter
    retries = 5
    for i in range(retries):
        try:
            return _bedrock.converse(**req)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ThrottlingException" and i < retries - 1:
                wait = min(2 ** i, 16) + random.uniform(0, 0.3)
                time.sleep(wait)
                continue
            raise

def extract_text(resp: dict) -> str:
    out = resp.get("output", {}).get("message", {}).get("content", [])
    parts: list[str] = []
    for c in out:
        if isinstance(c, dict) and "text" in c:
            parts.append(c.get("text", ""))
    return "".join(parts).strip()

def get_tool_uses(resp: dict) -> list[dict]:
    out = resp.get("output", {}).get("message", {}).get("content", [])
    uses: list[dict] = []
    for c in out:
        tu = c.get("toolUse") if isinstance(c, dict) else None
        if tu:
            uses.append(tu)
    return uses
