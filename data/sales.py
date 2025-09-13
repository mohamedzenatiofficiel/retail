import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_sales_from_api(start_sales_id: int, limit: int):
    
    # 1) Récupère les ventes depuis l'API /sales avec authentification ApiKey.
    
    base_url = os.getenv("API_BASE_URL")
    api_key = os.getenv("API_KEY")
    if not base_url or not api_key:
        raise RuntimeError("API_BASE_URL ou API_KEY manquants (.env / environnement)")

    url = base_url.rstrip("/") + "/sales"
    params = {"start_sales_id": start_sales_id, "limit": limit}
    headers = {
        "Authorization": f"{api_key}",
        "Accept": "application/json",
    }

    r = requests.get(url, params=params, headers=headers, timeout=30)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized : vérifie API_KEY et le header 'Authorization'")
    r.raise_for_status()

    payload = r.json()
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("Réponse inattendue : 'items' n'est pas une liste")
    return items


def normalize_sales(items: List[Dict[str, Any]], batch_id: str, ingestion_ts: str, ):
    """
    3) Normalisation des ventes en 2 DataFrames :
      - sales_customer : id, datetime, total_amount, customer_id, _ingestion_ts, _batch_id
      - sales_product  : sale_id, line_no, product_sku, quantity, amount, _ingestion_ts, _batch_id
    """
    if not items:
        return pd.DataFrame(), pd.DataFrame()

    df_customer = pd.DataFrame(
        [
            {
                "id": it.get("id"),
                "datetime": it.get("datetime"),
                "total_amount": it.get("total_amount"),
                "customer_id": it.get("customer_id"),
            }
            for it in items
        ]
    )
    df_customer["_ingestion_ts"] = ingestion_ts
    df_customer["_batch_id"] = batch_id


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
    4) Fonction principale :
       - Récupère les ventes via l'API
       - Normalise les données
       - Sauvegarde 2 fichiers NDJSON (customer + product)
    """
    start_sales_id = int(os.getenv("START_SALES_ID", "1"))
    limit = int(os.getenv("SALES_LIMIT", "250"))

    items = get_sales_from_api(start_sales_id=start_sales_id, limit=limit)
    if not items:
        print("Aucune vente renvoyée, rien à écrire.")
        return

    ingestion_ts = now_iso()
    batch_id = str(uuid.uuid4())

    df_customer, df_product = normalize_sales(items, batch_id=batch_id, ingestion_ts=ingestion_ts)
    if df_customer.empty and df_product.empty:
        print("Après normalisation, données vides. Rien à écrire.")
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_customer = Path("data/bronze/sales_customer") / f"{ts}.ndjson"
    out_product = Path("data/bronze/sales_product") / f"{ts}.ndjson"

    if not df_customer.empty:
        df_customer.to_json(out_customer, orient="records", lines=True, force_ascii=False)
        print(f"OK : {len(df_customer)} ventes -> {out_customer}")
    if not df_product.empty:
        df_product.to_json(out_product, orient="records", lines=True, force_ascii=False)
        print(f"OK : {len(df_product)} lignes -> {out_product}")

    print(f"Batch ID : {batch_id} | Ingestion : {ingestion_ts} | start_sales_id={start_sales_id}")


if __name__ == "__main__":
    main()
