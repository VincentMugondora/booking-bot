from fastapi import APIRouter, HTTPException
import boto3
from botocore.exceptions import ClientError
from app.config import settings

router = APIRouter(prefix="/v1/models", tags=["models"])

@router.get("/list")
def list_models():
    """List available Bedrock foundation models in the configured region."""
    try:
        bedrock = boto3.client("bedrock", region_name=settings.AWS_REGION)
        out = bedrock.list_foundation_models()
        models = out.get("modelSummaries", [])
        # Return a compact view
        return [
            {
                "modelId": m.get("modelId"),
                "modelName": m.get("modelName"),
                "providerName": m.get("providerName"),
                "inputModalities": m.get("inputModalities"),
                "outputModalities": m.get("outputModalities"),
            }
            for m in models
        ]
    except ClientError as e:
        raise HTTPException(status_code=502, detail=f"Bedrock list models failed: {e}")

@router.get("/get")
def get_model(modelId: str):
    """Fetch details for a specific Bedrock foundation model by ID."""
    try:
        bedrock = boto3.client("bedrock", region_name=settings.AWS_REGION)
        out = bedrock.get_foundation_model(modelIdentifier=modelId)
        return out.get("modelDetails", out)
    except ClientError as e:
        raise HTTPException(status_code=404, detail=f"Model not found or not accessible: {e}")
