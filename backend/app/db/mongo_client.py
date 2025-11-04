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
    # legacy indexes
    db.providers.create_index([("location", GEOSPHERE)])
    db.providers.create_index([("services", ASCENDING)])
    # active schema indexes
    db.providers.create_index([("coverage_coords", GEOSPHERE)])
    db.providers.create_index([("service_type", ASCENDING)])
    db.bookings.create_index([("provider_id", ASCENDING), ("start", ASCENDING)])
    db.conversations.create_index([("session_id", ASCENDING)], unique=True)
    db.users.create_index([("phone", ASCENDING)], unique=True)
    db.providers.create_index([("phone", ASCENDING)], unique=True)
