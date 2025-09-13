import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main():
    # 1) Récupération de la config 
    base_url = os.getenv("API_BASE_URL")
    api_key = os.getenv("API_KEY")
    if not base_url or not api_key:
        raise RuntimeError("API_BASE_URL ou API_KEY manquants (.env / environnement)")

    # 2) Préparation de la requête HTTP vers /customers
    url = base_url.rstrip("/") + "/customers"
    params = {"limit": 250}
    headers = {
        "Authorization": f"{api_key}",
        "Accept": "application/json",
    }
    # 3) Appel API (timeout pour éviter de bloquer indéfiniment)
    r = requests.get(url, params=params, headers=headers, timeout=30)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized : vérifie API_KEY et le header 'Authorization'")
    r.raise_for_status()

    # 4) Parsing JSON + validation basique de la structure attendue
    payload = r.json()
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("Réponse inattendue : 'items' n'est pas une liste")
    
    # 5) Passage en DataFrame (pandas) pour manipuler facilement
    df = pd.DataFrame(items)
    if df.empty:
        print("Aucun client renvoyé, rien à écrire.")
        return

    # Colonnes techniques
    df["_ingestion_ts"] = now_iso()
    df["_batch_id"] = str(uuid.uuid4())

    # Écriture NDJSON
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = Path("data/bronze/customers") / f"{ts}.ndjson"
    df.to_json(out_path, orient="records", lines=True, force_ascii=False)

    print(f"OK : {len(df)} lignes écrites -> {out_path}")
    print(df.head())


if __name__ == "__main__":
    main()
