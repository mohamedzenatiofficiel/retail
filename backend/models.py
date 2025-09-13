from pydantic import BaseModel, Field, EmailStr
from typing import List

class Product(BaseModel):
    product_sku: str = Field(..., example="SKU000001")
    description: str = Field(..., example="Wireless Mouse")
    unit_amount: float = Field(..., ge=0, example=29.9)
    supplier: str = Field(..., example="AcmeCorp")

class Customer(BaseModel):
    customer_id: str = Field(..., example="CS000001")
    emails: List[EmailStr] = Field(default_factory=list, example=["alice@example.com"])
    phone_numbers: List[str] = Field(default_factory=list, example=["+33612345678"])

class SaleLine(BaseModel):
    product_sku: str = Field(..., example="SKU000001")
    quantity: int = Field(..., ge=1, example=2)
    amount: float = Field(..., ge=0, example=59.8)

class Sale(BaseModel):
    id: int = Field(..., ge=1, example=1001)
    datetime: str = Field(..., example="2024-07-18T13:23:28Z")
    total_amount: float = Field(..., ge=0, example=119.6)
    items: List[SaleLine]
    customer_id: str = Field(..., example="CS000001")

class ListResponse(BaseModel):
    items: List[dict]
    total_items: int = Field(..., ge=0)
