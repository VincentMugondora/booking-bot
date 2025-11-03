from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    def __init__(self) -> None:
        self.MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.MONGODB_DB = os.getenv("MONGODB_DB", "booking_ai")
        self.AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
        self.BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229")
        self.JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
        self.JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
        self.PORT = int(os.getenv("PORT", "8000"))

settings = Settings()
