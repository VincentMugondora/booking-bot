from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any
from datetime import datetime
from app.db.mongo_client import get_db
from pydantic import BaseModel

router = APIRouter(prefix="/v1/providers", tags=["providers"])

def _parse_dt(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def _serialize_provider(doc: Dict[str, Any]) -> Dict[str, Any]:
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if isinstance(doc.get("availability"), list):
        for slot in doc["availability"]:
            if isinstance(slot.get("start"), datetime):
                slot["start"] = slot["start"].isoformat()
            if isinstance(slot.get("end"), datetime):
                slot["end"] = slot["end"].isoformat()
    return doc

class Slot(BaseModel):
    start: str
    end: str

class ProviderIn(BaseModel):
    name: str
    services: List[str]
    email: str | None = None
    phone: str | None = None
    lat: float | None = None
    lng: float | None = None
    address: str | None = None
    availability: List[Slot] | None = None

@router.post("/register")
def register_provider(p: ProviderIn) -> Dict[str, Any]:
    db = get_db()
    doc: Dict[str, Any] = {
        "name": p.name,
        "services": p.services,
    }
    if p.email:
        doc["email"] = p.email
    if p.phone:
        doc["phone"] = p.phone
    if p.address:
        doc["address"] = p.address
    if p.lat is not None and p.lng is not None:
        doc["location"] = {"type": "Point", "coordinates": [p.lng, p.lat]}
    if p.availability:
        slots: List[Dict[str, Any]] = []
        for s in p.availability:
            start_dt = _parse_dt(s.start)
            end_dt = _parse_dt(s.end)
            slots.append({"start": start_dt, "end": end_dt})
        doc["availability"] = slots

    filt: Dict[str, Any] | None = None
    if p.email:
        filt = {"email": p.email}
    elif p.phone:
        filt = {"phone": p.phone}

    if filt:
        existing = db.providers.find_one(filt)
        if existing:
            db.providers.update_one({"_id": existing["_id"]}, {"$set": doc})
            out = db.providers.find_one({"_id": existing["_id"]})
            return _serialize_provider(out)
        else:
            ins = doc.copy()
            ins.update(filt)
            res = db.providers.insert_one(ins)
            out = db.providers.find_one({"_id": res.inserted_id})
            return _serialize_provider(out)
    else:
        res = db.providers.insert_one(doc)
        out = db.providers.find_one({"_id": res.inserted_id})
        return _serialize_provider(out)

@router.get("/search")
def search(
    service: str,
    lat: float,
    lng: float,
    start: str,
    end: str,
    max_km: int = 30,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    db = get_db()
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    query = {
        "services": service,
        "availability": {"$elemMatch": {"start": {"$lte": start_dt}, "end": {"$gte": end_dt}}},
        "location": {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": [lng, lat]},
                "$maxDistance": max_km * 1000,
            }
        },
    }
    cursor = db.providers.find(query).limit(limit)
    return [_serialize_provider(d) for d in cursor]
