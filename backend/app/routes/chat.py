from fastapi import APIRouter
from pydantic import BaseModel
from app.db.mongo_client import get_db
from app.services.bedrock_client import converse, extract_text

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
    resp = converse(messages=messages, system_prompt=SYSTEM_PROMPT)
    reply = extract_text(resp) or "Sorry, I had trouble understanding."

    db.conversations.update_one(
        {"session_id": in_.session_id},
        {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
    )
    return ChatOut(reply=reply)
