from pydantic import BaseModel, Field, EmailStr
from typing import List

# --- Produit ---
class Product(BaseModel):
    product_sku: str = Field(..., example="SKU000001")   # Identifiant unique produit
    description: str = Field(..., example="Wireless Mouse")  # Description du produit
    unit_amount: float = Field(..., ge=0, example=29.9)  # Prix unitaire
    supplier: str = Field(..., example="AcmeCorp")       # Fournisseur

# --- Client ---
class Customer(BaseModel):
    customer_id: str = Field(..., example="CS000001")    # Identifiant unique client
    emails: List[EmailStr] = Field(                      # Liste d'emails
        default_factory=list, 
        example=["alice@example.com"]
    )
    phone_numbers: List[str] = Field(                    # Liste de numéros de téléphone
        default_factory=list, 
        example=["+33612345678"]
    )

# --- Ligne d'une vente ---
class SaleLine(BaseModel):
    product_sku: str = Field(..., example="SKU000001")   # Référence produit vendu
    quantity: int = Field(..., ge=1, example=2)          # Quantité
    amount: float = Field(..., ge=0, example=59.8)       # Montant ligne

# --- Vente (transaction complète) ---
class Sale(BaseModel):
    id: int = Field(..., ge=1, example=1001)             # Identifiant unique de la vente
    datetime: str = Field(..., example="2024-07-18T13:23:28Z")  # Date/heure ISO
    total_amount: float = Field(..., ge=0, example=119.6)        # Montant total de la vente
    items: List[SaleLine]                                # Liste des lignes de vente
    customer_id: str = Field(..., example="CS000001")    # Identifiant du client associé

# --- Réponse générique paginée ---
class ListResponse(BaseModel):
    items: List[dict]                                    # Liste d’objets (produits, clients ou ventes)
    total_items: int = Field(..., ge=0)                  # Nombre total d’objets disponibles
