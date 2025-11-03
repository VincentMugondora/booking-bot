from fastapi import FastAPI
from app.db.mongo_client import init_indexes
from app.routes.chat import router as chat_router
from app.routes.providers import router as providers_router
from app.routes.bookings import router as bookings_router
from fastapi.middleware.cors import CORSMiddleware

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

@app.get("/health")
def health():
    return {"ok": True}

@app.on_event("startup")
def startup():
    init_indexes()
init_indexes()
