# data/products.py
import os
import requests
from dotenv import load_dotenv

# Charger .env si présent
load_dotenv()

def main():
    base_url = os.getenv("API_BASE_URL")
    api_key = os.getenv("API_KEY")

    if not base_url or not api_key:
        raise RuntimeError("API_BASE_URL ou API_KEY manquant dans .env")

    url = base_url.rstrip("/") + "/products"
    params = {"limit": 5}  # pour tester avec 5 produits
    headers = {"Authorization": f"{api_key}"}

    response = requests.get(url, params=params, headers=headers, timeout=10)

    if response.status_code == 401:
        raise RuntimeError("Unauthorized : vérifie ton API_KEY")
    response.raise_for_status()

    data = response.json()
    print(data)  # Affiche brut pour l’instant

if __name__ == "__main__":
    main()
