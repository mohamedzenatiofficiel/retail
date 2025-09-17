import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# 0) Charger les variables d'environnement (.env)
load_dotenv()


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT_DIR / "data"))

# état incrémental (table meta.ingestion_state)
#      -> on lit/écrit last_sales_id ici
from loaders.state_store import get_last_sales_id, set_last_sales_id


def now_iso():
    """Horodatage technique en UTC (sans microsecondes)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_sales_from_api(start_sales_id: int, limit: int):
    """
    1) Appel HTTP vers /sales avec authentification ApiKey.

    - start_sales_id : id de vente (inclus) à partir duquel on récupère
    - limit          : taille de page (max 250)
    """
    base_url = os.getenv("API_BASE_URL")
    api_key = os.getenv("API_KEY")
    if not base_url or not api_key:
        raise RuntimeError("API_BASE_URL ou API_KEY manquants (.env / environnement)")

    url = base_url.rstrip("/") + "/sales"
    params = {"start_sales_id": start_sales_id, "limit": limit}
    headers = {"Authorization": f"{api_key}", "Accept": "application/json"}

    r = requests.get(url, params=params, headers=headers, timeout=30)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized : vérifie API_KEY et le header 'Authorization'")
    r.raise_for_status()

    payload = r.json()
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("Réponse inattendue : 'items' n'est pas une liste")
    return items


def normalize_sales(
    items: List[Dict[str, Any]],
    batch_id: str,
    ingestion_ts: str,
):
    """
    2) Normalisation en 2 DataFrames :
       - df_customer : 1 ligne par vente  (id, datetime, total_amount, customer_id, _ingestion_ts, _batch_id)
       - df_product  : 1 ligne par ligne (sale_id, line_no, product_sku, quantity, amount, _ingestion_ts, _batch_id)
    """
    if not items:
        return pd.DataFrame(), pd.DataFrame()

    # 2.1) Header des ventes
    df_customer = pd.DataFrame(
        {
            "id": [it.get("id") for it in items],
            "datetime": [it.get("datetime") for it in items],
            "total_amount": [it.get("total_amount") for it in items],
            "customer_id": [it.get("customer_id") for it in items],
        }
    )
    df_customer["_ingestion_ts"] = ingestion_ts
    df_customer["_batch_id"] = batch_id

    # 2.2) Lignes (items), on ajoute un line_no séquentiel par vente
    rows = []
    for it in items:
        sale_id = it.get("id")
        for idx, li in enumerate(it.get("items", []) or [], start=1):
            rows.append(
                {
                    "sale_id": sale_id,
                    "line_no": idx,
                    "product_sku": li.get("product_sku"),
                    "quantity": li.get("quantity"),
                    "amount": li.get("amount"),
                    "_ingestion_ts": ingestion_ts,
                    "_batch_id": batch_id,
                }
            )
    df_product = pd.DataFrame(rows)

    return df_customer, df_product


def main():
    """
    3) Pipeline incrémental /sales :
       a) Lire last_sales_id dans meta.ingestion_state (ou 1 si None)
       b) Appeler /sales?start_sales_id=...&limit=...
       c) Normaliser (2 DF)
       d) Écrire 2 NDJSON en bronze/sales_customer & bronze/sales_product
       e) Mettre à jour last_sales_id = max_id + 1
    """
    # a) Lire l'état (None si premier run)
    last_id = get_last_sales_id()
    start_sales_id = 1 if last_id is None else int(last_id)

    # b) Paramètres de page
    limit = int(os.getenv("SALES_LIMIT", "250"))

    # c) Appel API
    items = get_sales_from_api(start_sales_id=start_sales_id, limit=limit)
    if not items:
        print(f"Aucune vente renvoyée (start_sales_id={start_sales_id}). Rien à écrire, état inchangé.")
        return

    # d) Colonnes techniques de batch
    ingestion_ts = now_iso()
    batch_id = str(uuid.uuid4())

    # e) Normalisation -> 2 DataFrames
    df_customer, df_product = normalize_sales(items, batch_id=batch_id, ingestion_ts=ingestion_ts)
    if df_customer.empty and df_product.empty:
        print("Après normalisation, données vides. Rien à écrire.")
        return

    # f) Chemins de sortie NDJSON (⚠️ dossiers supposés existants)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_customer = DATA_DIR / "bronze" / "sales_customer" / f"{ts}.ndjson"
    out_product = DATA_DIR / "bronze" / "sales_product" / f"{ts}.ndjson"

    if not df_customer.empty:
        df_customer.to_json(out_customer, orient="records", lines=True, force_ascii=False)
        print(f"OK : {len(df_customer)} ventes -> {out_customer}")
    if not df_product.empty:
        df_product.to_json(out_product, orient="records", lines=True, force_ascii=False)
        print(f"OK : {len(df_product)} lignes -> {out_product}")

    # g) Avancer l'état : on enregistre "la prochaine valeur à lire"
    max_id = int(df_customer["id"].max())
    next_start = max_id + 1
    set_last_sales_id(next_start)

    print(
        f"Batch ID : {batch_id} | Ingestion : {ingestion_ts} | "
        f"start_sales_id={start_sales_id} -> next_start={next_start}"
    )


if __name__ == "__main__":
    main()
