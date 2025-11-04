from fastapi import APIRouter
import logging
from pydantic import BaseModel
from app.db.mongo_client import get_db
from app.services.bedrock_client import converse, extract_text
from app.config import settings
from botocore.exceptions import ClientError
from app.services.geocode import reverse_geocode
from datetime import datetime
import uuid

router = APIRouter(prefix="/v1/chat", tags=["chat"])
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are Hustlr, a friendly, concise WhatsApp booking assistant. "
    "Always sound warm and professional with brief, helpful replies and tasteful emojis when appropriate. "
    "Goals: help with local service bookings, provider onboarding, and simple confirmations. "
    "Policy: never invent actions you did not perform; if you proposed a time or booking, clearly state it's tentative unless confirmed by the system. "
    "When collecting registration info, ask one question at a time and keep it short."
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

def _log_and_close(db, conv_id: str, phone: str | None) -> None:
    conv = db.conversations.find_one({"session_id": conv_id})
    if not conv:
        return
    lines = [
        f"=== Hustlr Conversation {conv.get('session_id')} ===",
        f"Phone: {phone}",
        f"Started: {conv.get('started_at')}",
        "Transcript:",
    ]
    for m in conv.get("messages", []):
        role = m.get("role") or "assistant"
        texts = []
        for c in m.get("content", []):
            if isinstance(c, dict) and "text" in c:
                texts.append(c.get("text", ""))
        lines.append(f"{role.upper()}: {' '.join(texts).strip()}")
    transcript = "\n".join(lines)
    # Print to terminal and also log, to guarantee visibility
    print(transcript)
    logging.getLogger(__name__).info(transcript)
    db.conversations.update_one(
        {"session_id": conv_id},
        {"$set": {"status": "closed", "ended_at": datetime.utcnow().isoformat()}},
    )

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
    phone = _extract_phone(getattr(in_, "user_id", None)) or _extract_phone(getattr(in_, "session_id", None))
    conv_id: str
    if phone:
        open_conv = db.conversations.find_one({"phone": phone, "status": {"$ne": "closed"}})
        if open_conv:
            conv_id = open_conv.get("session_id")
        else:
            conv_id = str(uuid.uuid4())
            db.conversations.insert_one({
                "session_id": conv_id,
                "phone": phone,
                "status": "open",
                "started_at": datetime.utcnow().isoformat(),
                "messages": [],
            })
    else:
        conv_id = in_.session_id
        db.conversations.update_one(
            {"session_id": conv_id},
            {"$setOnInsert": {"session_id": conv_id, "status": "open", "started_at": datetime.utcnow().isoformat()}},
            upsert=True,
        )

    db.conversations.update_one(
        {"session_id": conv_id},
        {"$push": {"messages": {"role": "user", "content": [{"text": in_.message}]}}},
        upsert=True,
    )

    messages = [{"role": "user", "content": [{"text": in_.message}]}]
    if phone:
        users = db.users
        u = users.find_one({"phone": phone})
        if not u:
            users.insert_one({"phone": phone, "policy_agreed": False})
            u = users.find_one({"phone": phone})
        # If a WhatsApp location attachment was sent, capture coordinates immediately
        has_coords = (getattr(in_, "lat", None) is not None and getattr(in_, "lng", None) is not None)
        lower_msg = (in_.message or "").strip().lower()
        if has_coords and not u.get("location"):
            label = reverse_geocode(in_.lat, in_.lng) or f"{in_.lat},{in_.lng}"
            users.update_one({"_id": u["_id"]}, {"$set": {"coords": {"type": "Point", "coordinates": [in_.lng, in_.lat]}, "location": label}})
            u = users.find_one({"_id": u["_id"]})
        # If we previously asked for a field, try to store the answer now
        pending = (u or {}).get("pending_field")
        if pending == "name" and in_.message.strip():
            users.update_one({"_id": u["_id"]}, {"$set": {"name": in_.message.strip()}, "$unset": {"pending_field": ""}})
            u = users.find_one({"_id": u["_id"]})
        elif pending == "location" and (has_coords or in_.message.strip()):
            if has_coords:
                label = reverse_geocode(in_.lat, in_.lng) or f"{in_.lat},{in_.lng}"
                users.update_one({"_id": u["_id"]}, {"$set": {"coords": {"type": "Point", "coordinates": [in_.lng, in_.lat]}, "location": label}, "$unset": {"pending_field": ""}})
            else:
                users.update_one({"_id": u["_id"]}, {"$set": {"location": in_.message.strip()}, "$unset": {"pending_field": ""}})
            u = users.find_one({"_id": u["_id"]})
        elif pending == "policy":
            ans = (in_.message or "").strip().lower()
            if ans in ("yes", "y", "agree", "i agree"):
                users.update_one({"_id": u["_id"]}, {"$set": {"policy_agreed": True}, "$unset": {"pending_field": ""}})
                # Auto-close registration conversation with a friendly message
                reg_done = "✅ Thank you! Your registration is now complete. How can I assist you today?"
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": reg_done}]}}})
                _log_and_close(db, conv_id, phone)
                return ChatOut(reply=reg_done)
            u = users.find_one({"_id": u["_id"]})

        # Commands
        if lower_msg.startswith('/'):
            cmd = lower_msg.split()[0]
            # /reset: reset user profile and pending flows
            if cmd == '/reset':
                users.update_one({"_id": u["_id"]}, {
                    "$set": {"name": None, "location": None, "policy_agreed": False},
                    "$unset": {"pending_field": "", "coords": ""}
                })
                db.providers.update_one({"phone": phone}, {"$unset": {"pending_field": ""}}, upsert=False)
                pr = "Your profile has been reset. Let's start over — what's your full name?"
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                return ChatOut(reply=pr)
            # /end: close current conversation and print transcript
            if cmd == '/end':
                pr = "Conversation closed. Send a new message to start a fresh conversation."
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _log_and_close(db, conv_id, phone)
                return ChatOut(reply=pr)
            # /profile: show user profile
            if cmd == '/profile':
                u = users.find_one({"phone": phone}) or {}
                pr = (f"Profile\n"
                      f"Name: {u.get('name') or '-'}\n"
                      f"Location: {u.get('location') or '-'}\n"
                      f"Policy agreed: {'Yes' if u.get('policy_agreed') else 'No'}\n"
                      f"Phone: {phone}")
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                return ChatOut(reply=pr)
            # /provider status: show provider record
            if lower_msg.startswith('/provider status'):
                pdoc = db.providers.find_one({"phone": phone}) or {}
                if not pdoc:
                    pr = "No provider profile found. Say 'register as a provider' to start."
                else:
                    pr = ("Provider Status\n"
                          f"Name: {pdoc.get('name','-')}\nService: {pdoc.get('service_type','-')}\n"
                          f"Coverage: {pdoc.get('coverage','-')}\nActive: {'Yes' if pdoc.get('active') else 'No'}\n"
                          f"Policy agreed: {'Yes' if pdoc.get('policy_agreed') else 'No'}\n"
                          f"Provider ID: {str(pdoc.get('_id'))}")
                db.conversations.update_one({"session_id": in_.session_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                return ChatOut(reply=pr)
            # /bookings: list next 5 bookings
            if cmd == '/bookings':
                items = []
                # as user (by phone)
                for b in db.bookings.find({"user_id": phone}).sort("start", 1).limit(5):
                    items.append({"role": "user", "start": b.get("start"), "end": b.get("end"), "provider_id": b.get("provider_id")})
                # as provider (by provider _id)
                pdoc = db.providers.find_one({"phone": phone})
                if pdoc:
                    pid = str(pdoc.get("_id"))
                    for b in db.bookings.find({"provider_id": pid}).sort("start", 1).limit(5):
                        items.append({"role": "provider", "start": b.get("start"), "end": b.get("end"), "user_id": b.get("user_id")})
                if not items:
                    pr = "No upcoming bookings found."
                else:
                    lines = ["Upcoming bookings:"]
                    for it in items[:5]:
                        s = it.get("start"); e = it.get("end")
                        siso = s.isoformat() if hasattr(s, 'isoformat') else str(s)
                        eiso = e.isoformat() if hasattr(e, 'isoformat') else str(e)
                        if it.get("role") == 'user':
                            lines.append(f"You booked provider {it.get('provider_id')} from {siso} to {eiso}")
                        else:
                            lines.append(f"Client {it.get('user_id')} from {siso} to {eiso}")
                    pr = "\n".join(lines)
                db.conversations.update_one({"session_id": in_.session_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                return ChatOut(reply=pr)

        # Compute missing fields
        missing: list[str] = []
        if not u.get("name"):
            missing.append("name")
        if not u.get("location"):
            missing.append("location")
        if not u.get("policy_agreed", False):
            missing.append("policy")

        if missing:
            next_field = missing[0]
            prompt_text = (
                f"The user is registering. Their information so far:\n"
                f"Name: {u.get('name')}\n"
                f"Location: {u.get('location')}\n"
                f"Policy agreed: {u.get('policy_agreed', False)}\n\n"
                f"Ask the user for the next missing field ({next_field}) in a friendly, brief way. "
                f"Include a short hint with the policy URL when asking for policy. Customer policy: {settings.POLICY_URL}. "
                f"Respond only with the question to send to the user."
            )
            reg_messages = [{"role": "user", "content": [{"text": prompt_text}]}]
            try:
                resp = converse(
                    messages=reg_messages,
                    system_prompt=SYSTEM_PROMPT,
                    model_id=settings.BEDROCK_FAST_MODEL_ID,
                    max_tokens=80,
                    temperature=0.2,
                )
                reg_reply = extract_text(resp) or (
                    "What's your full name?" if next_field == "name" else (
                        "Where are you located?" if next_field == "location" else f"Do you agree to our policy? (yes/no) {settings.POLICY_URL}"
                    )
                )
            except Exception:
                reg_reply = (
                    "What's your full name?" if next_field == "name" else (
                        "Where are you located?" if next_field == "location" else f"Do you agree to our policy? (yes/no) {settings.POLICY_URL}"
                    )
                )
            users.update_one({"_id": u["_id"]}, {"$set": {"pending_field": next_field}})
            db.conversations.update_one(
                {"session_id": conv_id},
                {"$push": {"messages": {"role": "assistant", "content": [{"text": reg_reply}]}}},
            )
            return ChatOut(reply=reg_reply)
    # Provider onboarding flow (after user registration is complete)
    if phone:
        providers = db.providers
        p = providers.find_one({"phone": phone})
        lower_msg = (in_.message or "").strip().lower()
        start_provider = ("register" in lower_msg and "provider" in lower_msg)
        pending_p = (p or {}).get("pending_field") if p else None
        has_coords = (getattr(in_, "lat", None) is not None and getattr(in_, "lng", None) is not None)

        if start_provider and not p:
            res = providers.insert_one({"phone": phone, "active": False, "policy_agreed": False, "pending_field": "name"})
            p = providers.find_one({"_id": res.inserted_id})
            pr = "Great! Let's get you registered as a service provider. What's your full name?"
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            return ChatOut(reply=pr)
        if p and pending_p == "name" and in_.message.strip():
            providers.update_one({"_id": p["_id"]}, {"$set": {"name": in_.message.strip(), "pending_field": "service_type"}})
            pr = "Thanks! What type of service do you provide? (e.g., plumbing, electrical, cleaning)"
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            return ChatOut(reply=pr)
        if p and pending_p == "service_type" and in_.message.strip():
            providers.update_one({"_id": p["_id"]}, {"$set": {"service_type": in_.message.strip()}, "${unset}": {}})
            providers.update_one({"_id": p["_id"]}, {"$set": {"pending_field": "coverage"}})
            pr = f"Where is your service located or what area do you cover? (send city/suburb or share current location). Provider policy: {settings.PROVIDER_POLICY_URL}"
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            return ChatOut(reply=pr)
        if p and pending_p == "coverage" and (has_coords or in_.message.strip()):
            if has_coords:
                label = reverse_geocode(in_.lat, in_.lng) or f"{in_.lat},{in_.lng}"
                providers.update_one({"_id": p["_id"]}, {"$set": {"coverage": label, "coverage_coords": {"type": "Point", "coordinates": [in_.lng, in_.lat]}, "pending_field": "policy"}})
            else:
                providers.update_one({"_id": p["_id"]}, {"$set": {"coverage": in_.message.strip(), "pending_field": "policy"}})
            pr = "Do you agree to our service provider policy and terms? (yes/no)"
            db.conversations.update_one({"session_id": in_.session_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            return ChatOut(reply=pr)
        if p and pending_p == "policy":
            ans = lower_msg
            if ans in ("yes", "y", "agree", "i agree"):
                providers.update_one({"_id": p["_id"]}, {"$set": {"policy_agreed": True, "pending_field": "activate"}})
                p = providers.find_one({"_id": p["_id"]})
                prov_id = str(p["_id"]) if p else ""
                pr = ("✅ Thank you! You're now registered as a service provider.\n"
                      f"Provider Name: {p.get('name','')}\nService Type: {p.get('service_type','')}\nCoverage: {p.get('coverage','')}\nPolicy Agreed: Yes\nProvider ID: {prov_id}\n\n"
                      "Would you like to go live and start receiving booking requests now? (yes/no)")
                db.conversations.update_one({"session_id": in_.session_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                return ChatOut(reply=pr)
            else:
                pr = "You need to agree to the provider policy to continue. Do you agree? (yes/no)"
                db.conversations.update_one({"session_id": in_.session_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                return ChatOut(reply=pr)
        if p and pending_p == "activate":
            if lower_msg in ("yes", "y"):
                providers.update_one({"_id": p["_id"]}, {"$set": {"active": True}, "$unset": {"pending_field": ""}})
                pr = "Fantastic! 🎉 You are now live as a provider. You'll receive booking requests here."
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _log_and_close(db, conv_id, phone)
                return ChatOut(reply=pr)
            elif lower_msg in ("no", "n"):
                providers.update_one({"_id": p["_id"]}, {"$set": {"active": False}, "$unset": {"pending_field": ""}})
                pr = "No problem. You're registered but not live. Say 'go live' anytime to start receiving requests."
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _log_and_close(db, conv_id, phone)
                return ChatOut(reply=pr)
            elif "go live" in lower_msg:
                providers.update_one({"_id": p["_id"]}, {"$set": {"active": True}, "$unset": {"pending_field": ""}})
                pr = "You're now live and ready to receive bookings!"
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _log_and_close(db, conv_id, phone)
                return ChatOut(reply=pr)

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
        {"session_id": conv_id},
        {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
    )
    return ChatOut(reply=reply)
