from fastapi import APIRouter, Query
from typing import List, Dict, Any
from datetime import datetime
from app.db.mongo_client import get_db

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
