from fastapi import FastAPI
from app.db.mongo_client import init_indexes
from app.routes.chat import router as chat_router
from app.routes.providers import router as providers_router
from app.routes.bookings import router as bookings_router

app = FastAPI(title="Booking Assistant API")
app.include_router(chat_router)
app.include_router(providers_router)
app.include_router(bookings_router)

@app.get("/health")
def health():
    return {"ok": True}

@app.on_event("startup")
def startup():
    init_indexes()
