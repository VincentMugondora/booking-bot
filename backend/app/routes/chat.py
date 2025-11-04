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

def _variants(mid: str) -> list[str]:
    if not mid:
        return []
    c = [mid]
    if mid.endswith("-v1:0"):
        c.append(mid[:-5])
    else:
        c.append(f"{mid}-v1:0")
    return c

def _extract_phone(s: str | None) -> str | None:
    if not s:
        return None
    v = s
    if "@" in v:
        v = v.split("@", 1)[0]
    v = "".join(ch for ch in v if (ch.isdigit() or ch == "+"))
    return v if any(c.isdigit() for c in v) else None

class ChatIn(BaseModel):
    session_id: str
    user_id: str
    message: str
    lat: float | None = None
    lng: float | None = None
    fast: bool | None = False

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
    phone = _extract_phone(getattr(in_, "user_id", None)) or _extract_phone(getattr(in_, "session_id", None))
    if phone:
        users = db.users
        u = users.find_one({"phone": phone})
        if not u:
            users.insert_one({"phone": phone, "reg_step": 1})
            reply = "Welcome! What's your full name?"
            db.conversations.update_one(
                {"session_id": in_.session_id},
                {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
            )
            return ChatOut(reply=reply)
        step = int(u.get("reg_step", 0) or 0)
        if step == 1:
            users.update_one({"_id": u["_id"]}, {"$set": {"name": in_.message.strip(), "reg_step": 2}})
            reply = "Thanks! Where are you located?"
            db.conversations.update_one(
                {"session_id": in_.session_id},
                {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
            )
            return ChatOut(reply=reply)
        if step == 2:
            users.update_one({"_id": u["_id"]}, {"$set": {"location": in_.message.strip(), "reg_step": 3}})
            reply = "Do you agree to our policy? (yes/no)"
            db.conversations.update_one(
                {"session_id": in_.session_id},
                {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
            )
            return ChatOut(reply=reply)
        if step == 3:
            ans = (in_.message or "").strip().lower()
            if ans in ("yes", "y"):
                users.update_one({"_id": u["_id"]}, {"$set": {"policy_agreed": True, "reg_step": 0}})
                reply = "Registration complete! How can I help you today?"
            else:
                reply = "You need to agree to the policy to continue."
            db.conversations.update_one(
                {"session_id": in_.session_id},
                {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
            )
            return ChatOut(reply=reply)
    fast_mode = bool(getattr(in_, "fast", False))
    max_tokens = 120 if fast_mode else 400
    temperature = 0.3 if fast_mode else 0.4

    if settings.USE_LOCAL_LLM:
        reply = local_reply(in_.message)
    else:
        if fast_mode:
            _mc = _variants(settings.BEDROCK_FAST_MODEL_ID)
        else:
            _mc = _variants(settings.BEDROCK_MODEL_ID) + _variants("anthropic.claude-3-haiku-20240307")
        # de-duplicate while preserving order
        seen = set()
        model_candidates = []
        for m in _mc:
            if m and m not in seen:
                model_candidates.append(m)
                seen.add(m)
        reply = None
        logger.info("Trying Bedrock model candidates: %s (fast=%s, max_tokens=%s)", model_candidates, fast_mode, max_tokens)
        for mid in model_candidates:
            try:
                resp = converse(messages=messages, system_prompt=SYSTEM_PROMPT, model_id=mid, max_tokens=max_tokens, temperature=temperature)
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
