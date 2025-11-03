from pymongo import MongoClient, ASCENDING, GEOSPHERE
from app.config import settings

_client: MongoClient | None = None
_db = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(settings.MONGODB_URI)
    return _client

def get_db():
    global _db
    if _db is None:
        _db = get_client()[settings.MONGODB_DB]
    return _db

def init_indexes() -> None:
    db = get_db()
    db.providers.create_index([("location", GEOSPHERE)])
    db.providers.create_index([("services", ASCENDING)])
    db.bookings.create_index([("provider_id", ASCENDING), ("start", ASCENDING)])
    db.conversations.create_index([("session_id", ASCENDING)], unique=True)
