from fastapi import FastAPI
from app.db.mongo_client import init_indexes
from app.routes.chat import router as chat_router
from app.routes.providers import router as providers_router
from app.routes.bookings import router as bookings_router
from fastapi.middleware.cors import CORSMiddleware
from app.routes.models import router as models_router
import logging
import os
from logging.handlers import RotatingFileHandler
from app.config import settings

def configure_logging():
    if settings.LOG_TO_FILE:
        try:
            os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)
        except Exception:
            pass
        root = logging.getLogger()
        # avoid duplicate handlers on reload
        if not any(isinstance(h, RotatingFileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(settings.LOG_FILE) for h in root.handlers):
            handler = RotatingFileHandler(settings.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding='utf-8')
            fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
            handler.setFormatter(fmt)
            root.setLevel(logging.INFO)
            root.addHandler(handler)

configure_logging()

app = FastAPI(title="Booking Assistant API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(chat_router)
app.include_router(providers_router)
app.include_router(bookings_router)
app.include_router(models_router)

@app.get("/health")
def health():
    return {"ok": True}

@app.on_event("startup")
def startup():
    init_indexes()
