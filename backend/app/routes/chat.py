from fastapi import APIRouter
import logging
from pydantic import BaseModel
from app.db.mongo_client import get_db
from app.services.bedrock_client import converse, extract_text
from app.config import settings
from botocore.exceptions import ClientError
from app.services.geocode import reverse_geocode
from datetime import datetime, timedelta
import uuid
import re

router = APIRouter(prefix="/v1/chat", tags=["chat"])
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are Hustlr, a friendly, concise WhatsApp booking assistant. "
    "Always sound warm and professional with brief, helpful replies and tasteful emojis when appropriate. "
    "Goals: help with local service bookings, provider onboarding, and simple confirmations. "
    "Policy: never invent actions you did not perform; if you proposed a time or booking, clearly state it's tentative unless confirmed by the system. "
    "When collecting registration info, ask one question at a time and keep it short. "
    "For all conversations: reply naturally in plain language (never mention internal fields like provider_id). "
    "Extract service, task (issue), and date/time when the user provides them; if something is missing, ask for it briefly and kindly. "
    "Use the user's saved/default address given in context if available; do not ask for address if one is provided in context. "
    "When listing choices (like providers or addresses), keep the numbering provided in the context and ask the user to reply with a number or say 'recommend'. "
    "When a booking is finalized and confirmed, include the token CONFIRMED in the reply."
)

def local_reply(text: str, has_location: bool = False) -> str:
    t = (text or "").lower()
    intents = ["plumber", "cleaner", "electrician", "gardener", "painter"]
    if any(w in t for w in intents):
        need_loc = (not has_location) and not any(w in t for w in [" near ", " location", " address", " in "])
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

def _norm_tokens(s: str) -> set[str]:
    t = re.findall(r"[a-z]+", (s or "").lower())
    out: set[str] = set()
    for w in t:
        for suf in ("ers", "ments", "ment", "ing", "ers", "er", "ed", "s"):
            if w.endswith(suf) and len(w) > len(suf) + 2:
                w = w[: -len(suf)]
                break
        out.add(w)
    return out

def _parse_natural_datetime(text: str) -> datetime | None:
    t = (text or "").lower()
    base = None
    now = datetime.utcnow()
    if "tomorrow" in t:
        base = now + timedelta(days=1)
    elif "today" in t:
        base = now
    # try hh:mm(am/pm)
    hour = None
    minute = 0
    ampm = None
    m = re.search(r"\b(\d{1,2}):(\d{2})\s*(am|pm)?\b", t)
    if m:
        hour = int(m.group(1)); minute = int(m.group(2)); ampm = m.group(3)
    else:
        # 300pm, 1230pm etc
        m = re.search(r"\b(\d{3,4})\s*(am|pm)\b", t)
        if m:
            num = m.group(1); ampm = m.group(2)
            if len(num) == 3:
                hour = int(num[0]); minute = int(num[1:])
            else:
                hour = int(num[:-2]); minute = int(num[-2:])
        else:
            # 3 pm or 3pm
            m = re.search(r"\b(\d{1,2})\s*(am|pm)\b", t)
            if m:
                hour = int(m.group(1)); minute = 0; ampm = m.group(2)
    if hour is None:
        return None
    if ampm:
        if ampm == "pm" and hour != 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
    if not base:
        return None
    return base.replace(hour=hour, minute=minute, second=0, microsecond=0)

def _extract_booking_entities(text: str, user_location: str | None) -> tuple[dict, list[str]]:
    t = text or ""
    tl = t.lower()
    services = ["plumber", "electrician", "cleaner", "painter", "gardener", "handyman"]
    service = next((s for s in services if s in tl), None)
    dt = _parse_natural_datetime(tl)
    issue = None
    m = re.search(r"\bfor\s+(.+?)(?:\s+at\b|\s+on\b|$)", tl)
    if m:
        issue = m.group(1).strip()
    else:
        m = re.search(r"(leak\w.*|broken[^.]*|not working[^.]*|clog\w[^.]*)", tl)
        if m:
            issue = m.group(1).strip()
    address = None
    m = re.search(r"(\d+\s+[A-Za-z][\w\s\-]*)[, ]+([A-Za-z][\w\-]+)[, ]+([A-Za-z][\w\-]+)", t)
    if m:
        address = {"street": m.group(1).strip(), "suburb": m.group(2).strip(), "city": m.group(3).strip()}
    elif user_location:
        parts = [p.strip() for p in user_location.split(',') if p.strip()]
        if len(parts) >= 2:
            address = {"street": None, "suburb": parts[-2], "city": parts[-1]}
        elif len(parts) == 1:
            address = {"street": None, "suburb": None, "city": parts[0]}
    result: dict = {}
    if service:
        result["service"] = service
    if issue:
        result["issue"] = issue
    if dt:
        result["date_time"] = dt.isoformat()
    if address:
        result["address"] = address
    required = ["service", "issue", "date_time", "address"]
    missing = [k for k in required if k not in result]
    return result, missing

def _get_user_coords(u: dict | None):
    if not u:
        return None
    c = (u or {}).get("coords") or {}
    if not isinstance(c, dict):
        return None
    coords = c.get("coordinates")
    if isinstance(coords, list) and len(coords) == 2:
        return (coords[0], coords[1])
    return None

def _eta_from_meters(distance_m: float | None, speed_kph: float = 35.0) -> int | None:
    if not distance_m or distance_m <= 0:
        return None
    km = distance_m / 1000.0
    minutes = km / speed_kph * 60.0
    return int(round(minutes))

def _get_default_address(u: dict | None) -> dict | None:
    if not u:
        return None
    addrs = u.get("addresses")
    if isinstance(addrs, list) and addrs:
        def_addr = None
        for a in addrs:
            if isinstance(a, dict) and a.get("is_default"):
                def_addr = a
                break
        if not def_addr:
            def_addr = addrs[0] if isinstance(addrs[0], dict) else None
        if def_addr:
            street = def_addr.get("street")
            suburb = def_addr.get("suburb")
            city = def_addr.get("city")
            if street or suburb or city:
                return {"street": street, "suburb": suburb, "city": city}
    return None

def _is_provider_available(db, provider_id: str, start_dt: datetime, end_dt: datetime) -> bool:
    if not provider_id or not start_dt or not end_dt:
        return True
    conflict = db.bookings.find_one({
        "provider_id": provider_id,
        "$or": [
            {"start": {"$lt": end_dt}, "end": {"$gt": start_dt}}
        ]
    })
    return conflict is None

def _find_nearby_providers(db, service: str, u: dict | None, desired_start: datetime | None = None, slot_minutes: int = 60, max_distance_m: int = 30000) -> list[dict]:
    if not service:
        return []
    user_coords = _get_user_coords(u)
    qry = {"active": True, "service_type": service}
    results: list[dict] = []
    if user_coords:
        lng, lat = user_coords
        pipeline = [
            {"$geoNear": {
                "near": {"type": "Point", "coordinates": [lng, lat]},
                "distanceField": "distance_m",
                "spherical": True,
                "maxDistance": max_distance_m,
                "query": qry,
            }},
            {"$limit": 10},
        ]
        results = list(db.providers.aggregate(pipeline))
        if not results:
            # fallback to non-geo active list if no nearby results
            results = list(db.providers.find(qry).limit(10))
    else:
        # no coords — basic list
        results = list(db.providers.find(qry).limit(10))

    # annotate and rank
    desired_end = (desired_start + timedelta(minutes=slot_minutes)) if desired_start else None
    enriched = []
    for pv in results:
        dist_m = pv.get("distance_m") if isinstance(pv.get("distance_m"), (int, float)) else None
        eta_min = _eta_from_meters(dist_m)
        rating = pv.get("rating") or 0
        prov_id = str(pv.get("_id")) if pv.get("_id") else None
        available = True
        if desired_start and prov_id:
            available = _is_provider_available(db, prov_id, desired_start, desired_end)
        enriched.append({
            **pv,
            "distance_m": dist_m,
            "eta_min": eta_min,
            "rating": rating,
            "available": available,
        })
    # sort: available desc, rating desc, distance asc
    def sort_key(p):
        return (
            0 if p.get("available") else 1,
            -(p.get("rating") or 0),
            p.get("distance_m") if isinstance(p.get("distance_m"), (int, float)) else float('inf')
        )
    enriched.sort(key=sort_key)
    return enriched[:5]

def _llm_natural_reply(context_text: str, fast: bool = True, max_tokens: int = 180) -> str:
    """Ask the LLM to produce a single friendly, conversational reply based on the provided context."""
    try:
        _mc = _variants(settings.BEDROCK_FAST_MODEL_ID) if fast else (_variants(settings.BEDROCK_MODEL_ID) + _variants("anthropic.claude-3-haiku-20240307"))
        # de-duplicate while preserving order
        seen = set(); model_candidates = []
        for m in _mc:
            if m and m not in seen:
                model_candidates.append(m); seen.add(m)
        messages = [{"role": "user", "content": [{"text": context_text}]}]
        for mid in model_candidates:
            try:
                resp = converse(messages=messages, system_prompt=SYSTEM_PROMPT, model_id=mid, max_tokens=max_tokens, temperature=0.3 if fast else 0.5)
                reply = extract_text(resp)
                if reply:
                    return reply
            except Exception:
                continue
    except Exception:
        pass
    # fallback
    return "Could you share a bit more so I can help? For example, the task and a suitable time."

def _print_msg(conv_id: str, phone: str | None, role: str, text: str) -> None:
    try:
        ts = datetime.utcnow().isoformat()
        line = f"{ts} Hustlr [{phone}|{conv_id}] {role.upper()}: {text}"
        print(line)
        logging.getLogger(__name__).info(line)
    except Exception:
        pass

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
    _print_msg(conv_id, phone, "user", in_.message or "")

    messages = [{"role": "user", "content": [{"text": in_.message}]}]
    user_location = None
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
        elif (not has_coords) and (not u.get("location")):
            c = (u or {}).get("coords") or {}
            coords = c.get("coordinates") if isinstance(c, dict) else None
            if isinstance(coords, list) and len(coords) == 2:
                lng, lat = coords[0], coords[1]
                label = reverse_geocode(lat, lng) or f"{lat},{lng}"
                users.update_one({"_id": u["_id"]}, {"$set": {"location": label}})
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
        user_location = (u or {}).get("location")

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
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)
            # /end: close current conversation and print transcript
            if cmd == '/end':
                pr = "Conversation closed. Send a new message to start a fresh conversation."
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
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
                _print_msg(conv_id, phone, "assistant", pr)
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
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
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
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)

        # Compute missing fields
        missing: list[str] = []
        if not u.get("name"):
            missing.append("name")
        if not (u.get("location") or u.get("coords")):
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
            _print_msg(conv_id, phone, "assistant", reg_reply)
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
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)
        if p and pending_p == "name" and in_.message.strip():
            providers.update_one({"_id": p["_id"]}, {"$set": {"name": in_.message.strip(), "pending_field": "service_type"}})
            pr = "Thanks! What type of service do you provide? (e.g., plumbing, electrical, cleaning)"
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)
        if p and pending_p == "service_type" and in_.message.strip():
            providers.update_one({"_id": p["_id"]}, {"$set": {"service_type": in_.message.strip()}})
            providers.update_one({"_id": p["_id"]}, {"$set": {"pending_field": "coverage"}})
            pr = f"Where is your service located or what area do you cover? (send city/suburb or share current location). Provider policy: {settings.PROVIDER_POLICY_URL}"
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)
        if p and pending_p == "coverage" and (has_coords or in_.message.strip()):
            if has_coords:
                label = reverse_geocode(in_.lat, in_.lng) or f"{in_.lat},{in_.lng}"
                providers.update_one({"_id": p["_id"]}, {"$set": {"coverage": label, "coverage_coords": {"type": "Point", "coordinates": [in_.lng, in_.lat]}, "pending_field": "policy"}})
            else:
                providers.update_one({"_id": p["_id"]}, {"$set": {"coverage": in_.message.strip(), "pending_field": "policy"}})
            pr = "Do you agree to our service provider policy and terms? (yes/no)"
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
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
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)
            else:
                pr = "You need to agree to the provider policy to continue. Do you agree? (yes/no)"
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
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

    # Booking extraction and confirmation flow
    if phone:
        conv_doc = db.conversations.find_one({"session_id": conv_id}) or {}
        draft = conv_doc.get("booking_draft") or {}
        bstate = conv_doc.get("booking_state")
        prov_opts = conv_doc.get("provider_options") or []
        addr_opts = conv_doc.get("address_options") or []
        lm = (in_.message or "").strip().lower()
        confirm_words = ("yes", "y", "confirm", "ok", "okay", "go ahead", "sure")

        # Address selection handling (before provider selection)
        if (bstate == "awaiting_address_choice" or (addr_opts and not draft.get("address"))):
            chosen = None
            m_sel = re.search(r"\b([1-9])\b", lm)
            if m_sel:
                idx = int(m_sel.group(1)) - 1
                if 0 <= idx < len(addr_opts):
                    chosen = addr_opts[idx]
            if chosen:
                merged = dict(draft)
                merged["address"] = {"street": chosen.get("street"), "suburb": chosen.get("suburb"), "city": chosen.get("city")}
                db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_draft": merged}, "$unset": {"address_options": ""}})
                # continue flow below to compute missing fields
                draft = merged
                bstate = None

        # Auto-fill address from user's saved addresses when missing
        if not draft.get("address"):
            u_doc = db.users.find_one({"phone": phone}) or None
            def_addr = _get_default_address(u_doc)
            # if multiple addresses and no default, ask to choose
            addrs = (u_doc or {}).get("addresses") if u_doc else None
            if def_addr:
                draft["address"] = def_addr
            elif isinstance(addrs, list) and len(addrs) > 1:
                lines = ["Please choose an address:"]
                opts = []
                for i, a in enumerate(addrs[:9], start=1):
                    if not isinstance(a, dict):
                        continue
                    street = a.get("street") or ""
                    suburb = a.get("suburb") or ""
                    city = a.get("city") or ""
                    label = ", ".join([p for p in [street, suburb, city] if p]) or "(Unnamed)"
                    lines.append(f"{i}. {label}")
                    opts.append({"street": street, "suburb": suburb, "city": city})
                lines.append("Reply with 1-" + str(len(opts)) + " to pick your address.")
                pr = "\n".join(lines)
                db.conversations.update_one({"session_id": conv_id}, {"$set": {"address_options": opts, "booking_draft": draft, "booking_state": "awaiting_address_choice"}})
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)

        # Provider selection handling (before confirmation)
        if (bstate == "awaiting_provider_choice" or (prov_opts and not draft.get("provider_id"))):
            chosen = None
            if "recommend" in lm or "choose for me" in lm or "pick for me" in lm:
                chosen = prov_opts[0] if prov_opts else None
            else:
                m_sel = re.search(r"\b([1-9])\b", lm)
                if m_sel:
                    idx = int(m_sel.group(1)) - 1
                    if 0 <= idx < len(prov_opts):
                        chosen = prov_opts[idx]
            if chosen:
                merged = dict(draft)
                merged["provider_id"] = str(chosen.get("_id"))
                merged["provider_name"] = chosen.get("name") or "Provider"
                db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_draft": merged}, "$unset": {"provider_options": ""}})
                # Now decide next prompt
                required = ["service", "issue", "date_time", "provider_id"]
                missing = [k for k in required if not merged.get(k)]
                if not missing and merged.get("issue"):
                    addr = merged.get("address") or {}
                    addr_str = ", ".join([p for p in [addr.get("street"), addr.get("suburb"), addr.get("city")] if p])
                    dt_str = merged.get("date_time")
                    try:
                        dt_disp = datetime.fromisoformat(dt_str).strftime('%Y-%m-%d %I:%M %p') if dt_str else dt_str
                    except Exception:
                        dt_disp = dt_str
                    pr = f"Got it! Booking {merged.get('provider_name')} for '{merged.get('issue')}' on {dt_disp} at {addr_str}. Confirm?"
                    db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_state": "awaiting_confirm"}})
                    db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                    _print_msg(conv_id, phone, "assistant", pr)
                    return ChatOut(reply=pr)
                else:
                    pr = "Great, I selected " + (merged.get("provider_name") or "a provider") + ". Could you provide: " + ", ".join(missing) + "?"
                    db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_draft": merged, "booking_state": "collecting"}})
                    db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                    _print_msg(conv_id, phone, "assistant", pr)
                    return ChatOut(reply=pr)

        if bstate == "awaiting_confirm":
            if any((lm == w) or lm.startswith(w) for w in confirm_words):
                merged = draft or {}
                if ("address" not in merged or not merged.get("address")) and user_location:
                    parts = [p.strip() for p in user_location.split(",") if p.strip()]
                    addr = {"street": None, "suburb": parts[-2] if len(parts) >= 2 else None, "city": parts[-1] if parts else None}
                    merged["address"] = addr
                dt_str = merged.get("date_time")
                try:
                    start_dt = datetime.fromisoformat(dt_str) if dt_str else None
                except Exception:
                    start_dt = None
                slot_minutes = 60
                end_dt = (start_dt + timedelta(minutes=slot_minutes)) if start_dt else None
                provider_id = merged.get("provider_id")
                provider_name = merged.get("provider_name") or "Provider"
                u_doc = db.users.find_one({"phone": phone}) or None
                # Re-check availability and auto-assign alternative if needed
                if start_dt and provider_id and not _is_provider_available(db, provider_id, start_dt, end_dt):
                    alternatives = _find_nearby_providers(db, merged.get("service"), u_doc, desired_start=start_dt, slot_minutes=slot_minutes)
                    alt = next((p for p in alternatives if p.get("available") and str(p.get("_id")) != provider_id), None)
                    if alt:
                        provider_id = str(alt.get("_id"))
                        provider_name = alt.get("name") or provider_name
                    else:
                        pr = "No providers are available at that time. Please share another time that works for you."
                        db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_draft": merged, "booking_state": "collecting"}})
                        db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                        _print_msg(conv_id, phone, "assistant", pr)
                        return ChatOut(reply=pr)
                # Insert into bookings collection
                booking_doc = {
                    "user_id": phone,
                    "provider_id": provider_id,
                    "start": start_dt,
                    "end": end_dt,
                    "notes": merged.get("issue"),
                    "service": merged.get("service"),
                    "address": merged.get("address"),
                    "created_at": datetime.utcnow(),
                }
                db.bookings.insert_one(booking_doc)
                db.conversations.update_one({"session_id": conv_id}, {"$unset": {"booking_draft": "", "booking_state": "", "provider_options": ""}})
                # Confirmation message
                try:
                    dt_disp = start_dt.strftime('%Y-%m-%d %I:%M %p') if start_dt else dt_str
                except Exception:
                    dt_disp = dt_str
                addr = merged.get("address") or {}
                addr_str = ", ".join([p for p in [addr.get("street"), addr.get("suburb"), addr.get("city")] if p])
                pr = f"Done! Booked {provider_name} for {dt_disp} at {addr_str}."
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)
            elif lm in ("no", "n", "cancel", "stop"):
                db.conversations.update_one({"session_id": conv_id}, {"$unset": {"booking_draft": "", "booking_state": ""}})
                pr = "Okay, cancelled the booking."
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)

        extracted, _ = _extract_booking_entities(in_.message, user_location)
        merged = dict(draft)
        for k, v in (extracted or {}).items():
            if v and (k not in merged or not merged.get(k)):
                merged[k] = v
        # Fallback: infer service by matching DB service_type strings in user message (fuzzy token match)
        if not merged.get("service"):
            try:
                svc_vals = [s for s in db.providers.distinct("service_type") if isinstance(s, str) and s]
            except Exception:
                svc_vals = []
            msg_tokens = _norm_tokens(in_.message or "")
            cand = None
            best_score = 0
            for st in svc_vals:
                st_tokens = _norm_tokens(st)
                score = len(msg_tokens & st_tokens)
                if score > best_score or (score == best_score and cand and len(st) > len(cand)):
                    best_score = score
            parts = [f"- {('what service' if fld=='service' else ('what you need done' if fld=='issue' else 'when'))}: {help_text.get(fld)}" for fld in missing]
            extra = []
            if (not merged.get("provider_id")) and merged.get("service"):
                extra.append("I'll list nearby providers so you can pick a number or say 'recommend'.")
            pr = "Here\u2019s what I need:\n" + "\n".join(parts)
            if extra:
                pr += "\n" + " ".join(extra)
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)
        # Friendly greeting handler
        if not merged.get("service") and any(g in lm for g in ("hi", "hello", "hey", "hie", "morning", "afternoon", "evening")):
            # Let the LLM craft a friendly greeting asking for service and preferred time in one sentence
            context_text = "The user greeted you. Reply with a short friendly message asking for service and preferred time in one sentence."
            pr = _llm_natural_reply(context_text, fast=True, max_tokens=60)
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)
        # If service identified but no provider chosen, list nearby options
        if merged.get("service") and not merged.get("provider_id") and not prov_opts:
            u_doc = db.users.find_one({"phone": phone}) or None
            # derive desired start if available to evaluate availability/ETA ranking
            desired_start = None
            try:
                if merged.get("date_time"):
                    desired_start = datetime.fromisoformat(merged.get("date_time"))
            except Exception:
                desired_start = None
            provs = _find_nearby_providers(db, merged.get("service"), u_doc, desired_start=desired_start)
            if provs:
                lines = ["Here are nearby " + merged.get("service") + "s:"]
                opts = []
                for i, pv in enumerate(provs[:5], start=1):
                    nm = pv.get("name") or "Provider"
                    area = pv.get("coverage") or pv.get("coverage_label") or pv.get("service_type")
                    dist_m = pv.get("distance_m") if isinstance(pv.get("distance_m"), (int, float)) else None
                    dist_str = f"{dist_m/1000:.1f} km" if dist_m else None
                    eta = pv.get("eta_min")
                    eta_str = f"~{eta} min" if isinstance(eta, int) else None
                    rating = pv.get("rating")
                    rating_str = f"★{float(rating):.1f}" if isinstance(rating, (int, float)) and rating > 0 else None
                    avail = pv.get("available")
                    avail_str = "Available" if avail else ("Busy" if avail is not None else None)
                    parts = [p for p in [area, dist_str, eta_str, rating_str, avail_str] if p]
                    meta = " • ".join(parts) if parts else area
                    lines.append(f"{i}. {nm} — {meta}")
                    opts.append({
                        "_id": pv.get("_id"),
                        "name": nm,
                        "coverage": area,
                        "distance_m": dist_m,
                        "eta_min": eta if isinstance(eta, int) else None,
                        "rating": float(rating) if isinstance(rating, (int, float)) else None,
                        "available": bool(avail) if isinstance(avail, bool) else avail,
                    })
                lines.append("Reply with 1-" + str(len(opts)) + " to choose, or say 'recommend'.")
                # Let the LLM present the provider list naturally and ask for a number or 'recommend'
                context_text = "Provide a short reply listing the providers below and ask the user to reply with a number or say 'recommend'.\n" + "\n".join(lines)
                pr = _llm_natural_reply(context_text, fast=True, max_tokens=160)
                db.conversations.update_one({"session_id": conv_id}, {"$set": {"provider_options": opts, "booking_draft": merged, "booking_state": "awaiting_provider_choice"}})
                db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
                _print_msg(conv_id, phone, "assistant", pr)
                return ChatOut(reply=pr)
        if merged and merged != draft:
            db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_draft": merged, "booking_state": "collecting"}})
        if merged and not missing and merged.get("issue"):
            addr = merged.get("address") or {}
            addr_str = ", ".join([p for p in [addr.get("street"), addr.get("suburb"), addr.get("city")] if p])
            dt_str = merged.get("date_time")
            try:
                dt_disp = datetime.fromisoformat(dt_str).strftime('%Y-%m-%d %I:%M %p') if dt_str else dt_str
            except Exception:
                dt_disp = dt_str
            prov_name = merged.get("provider_name") or "a provider"
            pr = f"Got it! Booking {prov_name} for '{merged.get('issue')}' on {dt_disp} at {addr_str}. Confirm?"
            db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_state": "awaiting_confirm"}})
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)
        elif extracted and missing:
            # Ask the LLM to request missing info naturally based on known details
            ctx = ["Compose a short, friendly reply asking only for the missing details."]
            if merged.get("service"):
                ctx.append(f"- service: {merged.get('service')}")
            if merged.get("issue"):
                ctx.append(f"- task: {merged.get('issue')}")
            if merged.get("date_time"):
                ctx.append(f"- date_time: {merged.get('date_time')}")
            addr = merged.get("address") or {}
            if any(addr.get(k) for k in ("street","suburb","city")):
                addr_str = ", ".join([p for p in [addr.get("street"), addr.get("suburb"), addr.get("city")] if p])
                ctx.append(f"- address: {addr_str}")
            ctx.append("Missing: " + ", ".join(missing))
            context_text = "\n".join(ctx)
            pr = _llm_natural_reply(context_text, fast=True, max_tokens=140)
            db.conversations.update_one({"session_id": conv_id}, {"$set": {"booking_draft": merged, "booking_state": "collecting"}})
            db.conversations.update_one({"session_id": conv_id}, {"$push": {"messages": {"role": "assistant", "content": [{"text": pr}]}}})
            _print_msg(conv_id, phone, "assistant", pr)
            return ChatOut(reply=pr)

    fast_mode = bool(getattr(in_, "fast", False))
    max_tokens = 120 if fast_mode else 400
    temperature = 0.3 if fast_mode else 0.4

    if settings.USE_LOCAL_LLM:
        reply = local_reply(in_.message, has_location=bool(user_location))
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
        llm_messages = messages
        if user_location:
            llm_messages = [{"role": "user", "content": [{"text": f"[Context] User location: {user_location}"}]}] + messages
        for mid in model_candidates:
            try:
                resp = converse(messages=llm_messages, system_prompt=SYSTEM_PROMPT, model_id=mid, max_tokens=max_tokens, temperature=temperature)
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
            reply = local_reply(in_.message, has_location=bool(user_location))

    db.conversations.update_one(
        {"session_id": conv_id},
        {"$push": {"messages": {"role": "assistant", "content": [{"text": reply}]}}},
    )
    return ChatOut(reply=reply)
