import os
from fastapi import HTTPException
from typing import Optional

FAKE_API_KEY = os.getenv("FAKE_API_KEY", "FAKE_KEY_123")

def require_api_key(authorization: Optional[str]):
    """
    Vérifie que le client a bien fourni une API Key valide dans le header "Authorization".

    Args:
        authorization (Optional[str]): valeur du header "Authorization".
                                       Ex : "API_KEY"

    Raises:
        HTTPException: 401 Unauthorized si :
            - le header est absent
            - ou la valeur ne correspond pas à la clé attendue
    """
    if not authorization or authorization != f"{FAKE_API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")
