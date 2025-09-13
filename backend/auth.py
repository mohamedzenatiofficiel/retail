import os
from fastapi import Header, HTTPException
from typing import Optional

FAKE_API_KEY = os.getenv("FAKE_API_KEY", "FAKE_KEY_123")

def require_api_key(authorization: Optional[str]):
    if not authorization or authorization != f"{FAKE_API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized (missing or wrong ApiKey)")
