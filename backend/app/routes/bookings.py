from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from app.db.mongo_client import get_db

router = APIRouter(prefix="/v1/bookings", tags=["bookings"])

class BookingIn(BaseModel):
    user_id: str
    provider_id: str
    start: datetime
    end: datetime
    notes: str | None = None

@router.post("")
def create_booking(in_: BookingIn):
    db = get_db()
    conflict = db.bookings.find_one({
        "provider_id": in_.provider_id,
        "$or": [
            {"start": {"$lt": in_.end}, "end": {"$gt": in_.start}}
        ]
    })
    if conflict:
        raise HTTPException(status_code=409, detail="Time slot already booked")
    doc = in_.model_dump()
    res = db.bookings.insert_one(doc)
    return {"_id": str(res.inserted_id)}
