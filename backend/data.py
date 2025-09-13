from .models import Product, Customer

# --- Liste de 200 produits factices ---
PRODUCTS = [
    Product(
        product_sku=f"SKU{i:06d}",           # Identifiant produit, ex : "SKU000001"
        description=f"Product {i}",          # Nom/description : "Product 1", "Product 2", ...
        unit_amount=round(10 + i * 0.1, 2),  # Prix unitaire simulé : 10.1, 10.2, etc.
        supplier="AcmeCorp"                  # Fournisseur fictif
    )
    for i in range(1, 201)  # Génère 200 produits (id de 1 à 200)
]

# --- Liste de 50 clients factices ---
CUSTOMERS = [
    Customer(
        customer_id=f"CS{i:06d}",                # Identifiant client, ex : "CS000001"
        emails=[f"user{i}@example.com"],         # Adresse mail fictive unique par client
        phone_numbers=[f"+33612{i:04d}"]         # Numéro de téléphone fictif
    )
    for i in range(1, 51)   # Génère 50 clients (id de 1 à 50)
]