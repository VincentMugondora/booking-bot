from fastapi import APIRouter
import logging
from pydantic import BaseModel
from app.db.mongo_client import get_db
from app.services.bedrock_client import converse, extract_text
from app.config import settings
from botocore.exceptions import ClientError

router = APIRouter(prefix="/v1/chat", tags=["chat"])
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a friendly, concise booking assistant. "
    "Extract service, location, and time; ask one clarifying question when needed."
)

def local_reply(text: str) -> str:
    t = (text or "").lower()
    intents = ["plumber", "cleaner", "electrician", "gardener", "painter"]
    if any(w in t for w in intents):
        need_loc = not any(w in t for w in [" near ", " location", " address", " in "])
        need_time = not any(w in t for w in [" today", " tomorrow", " am", " pm", ":", " at "])
        prompts = []
        if need_loc:
            prompts.append("your location")
        if need_time:
            prompts.append("preferred date/time")
        if prompts:
            if len(prompts) == 2:
                return "Got it. Please share your location and preferred date/time."
            return f"Got it. Please share {prompts[0]}."
        return "Great. Any budget range I should consider before I suggest options?"
    return "How can I help with booking today? Share the service, location, and preferred time."

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
    if settings.USE_LOCAL_LLM:
        reply = local_reply(in_.message)
    else:
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
            except ClientError as e:
                logger.warning("Bedrock client error for model %s: %s", mid, e, exc_info=True)
                continue
            except Exception as e:
                logger.exception("Unexpected Bedrock error for model %s: %s", mid, e)
                continue
        if not reply:
            logger.error("All Bedrock model attempts failed: %s", model_candidates)
            reply = local_reply(in_.message)

    db.conversations.update_one(
        {"session_id": in_.session_id},
        {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
    )
    return ChatOut(reply=reply)
