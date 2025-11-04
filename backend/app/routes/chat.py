from fastapi import APIRouter
from pydantic import BaseModel
from app.db.mongo_client import get_db
from app.services.bedrock_client import converse, extract_text
from app.config import settings
from botocore.exceptions import ClientError

router = APIRouter(prefix="/v1/chat", tags=["chat"])

SYSTEM_PROMPT = (
    "You are a friendly, concise booking assistant. "
    "Extract service, location, and time; ask one clarifying question when needed."
)

class ChatIn(BaseModel):
    session_id: str
    user_id: str
    message: str
    lat: float | None = None
    lng: float | None = None

class ChatOut(BaseModel):
    reply: str

@router.post("", response_model=ChatOut)
def chat(in_: ChatIn):
    db = get_db()
    db.conversations.update_one(
        {"session_id": in_.session_id},
        {"$setOnInsert": {"session_id": in_.session_id},
         "$push": {"messages": {"role": "user", "content": [{"text": in_.message}]}}},
        upsert=True,
    )

    messages = [{"role": "user", "content": [{"text": in_.message}]}]
    # Try configured model, then fall back to haiku and titan-lite for dev resilience
    model_candidates = [
        settings.BEDROCK_MODEL_ID,
        "anthropic.claude-3-haiku-20240307",
        "amazon.titan-text-lite-v1",
    ]
    reply = None
    for mid in model_candidates:
        try:
            resp = converse(messages=messages, system_prompt=SYSTEM_PROMPT, model_id=mid)
            reply = extract_text(resp)
            if reply:
                break
        except ClientError:
            continue
        except Exception:
            continue
    if not reply:
        reply = "I'm having trouble reaching the AI right now. Please try again shortly."

    db.conversations.update_one(
        {"session_id": in_.session_id},
        {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
    )
    return ChatOut(reply=reply)
