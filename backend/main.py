from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import FastAPI, Query, Security
from fastapi.security.api_key import APIKeyHeader

from .auth import require_api_key
from .data import CUSTOMERS, PRODUCTS
from .models import ListResponse, Sale, SaleLine

app = FastAPI(
    title="Stack Labs Retail API (Mock)",
    version="0.1.0",
    description=(
        "API mock locale pour l'exercice Retail "
        "(products, customers, sales incrémental)."
    ),
)

# --- Schéma d'auth pour Swagger UI ---
# Header 'Authorization' à renseigner via le bouton "Authorize".
# Valeur attendue : "ApiKey FAKE_KEY_123"
API_KEY_HEADER_NAME = "Authorization"
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@app.get(
    "/products",
    response_model=ListResponse,
    summary="Products",
    tags=["catalog"],
)
def get_products(
    limit: int = Query(
        10,
        ge=1,
        le=250,
        description="Items per page (max 250).",
    ),
    api_key: Optional[str] = Security(api_key_header),
):
    require_api_key(api_key)
    items = [p.model_dump() for p in PRODUCTS[:limit]]
    return {"items": items, "total_items": len(PRODUCTS)}


@app.get(
    "/customers",
    response_model=ListResponse,
    summary="Customers",
    tags=["catalog"],
)
def get_customers(
    limit: int = Query(
        10,
        ge=1,
        le=250,
        description="Items per page (max 250).",
    ),
    api_key: Optional[str] = Security(api_key_header),
):
    require_api_key(api_key)
    items = [c.model_dump() for c in CUSTOMERS[:limit]]
    return {"items": items, "total_items": len(CUSTOMERS)}


@app.get(
    "/sales",
    response_model=ListResponse,
    summary="Sales (incremental)",
    tags=["transactions"],
)
def get_sales(
    start_sales_id: int = Query(
        1,
        ge=1,
        description="Start from this auto-incremental sales id.",
    ),
    limit: int = Query(
        10,
        ge=1,
        le=250,
        description="Items per page (max 250).",
    ),
    api_key: Optional[str] = Security(api_key_header),
):
    require_api_key(api_key)

    items: List[Dict] = []
    for sid in range(start_sales_id, start_sales_id + limit):
        line = SaleLine(product_sku="SKU000001", quantity=2, amount=59.8)
        sale = Sale(
            id=sid,
            datetime=now_iso(),
            total_amount=119.6,
            items=[line],
            customer_id="CS000001",
        )
        items.append(sale.model_dump())

    return {"items": items, "total_items": 123456}
