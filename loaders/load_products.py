# Objectif : charger le dernier NDJSON de Bronze (products) vers Silver (Postgres)
# Stratégie : staging temporaire + upsert (INSERT ... ON CONFLICT DO UPDATE)

from sqlalchemy import text

from .config import get_engine, read_latest_ndjson


def main():
    # 1) Lire le dernier NDJSON de products (Bronze)
    df, f = read_latest_ndjson("bronze/products")

    # 2) Ouvrir une connexion/transaction à Postgres
    engine = get_engine()
    with engine.begin() as conn:
        # 3) Créer une table temporaire calquée sur la table cible
        conn.execute(
            text("CREATE TEMP TABLE tmp_products (LIKE silver.products INCLUDING ALL);")
        )

        # 4) Charger le DataFrame dans la table temporaire
        df.to_sql("tmp_products", con=conn, if_exists="append", index=False)

        # 5) Upsert : fusionner staging -> table finale (clé = product_sku)
        conn.execute(
            text(
                """
                INSERT INTO silver.products AS t
                  (product_sku, description, unit_amount, supplier, _ingestion_ts, _batch_id)
                SELECT product_sku, description, unit_amount, supplier, _ingestion_ts, _batch_id
                FROM tmp_products
                ON CONFLICT (product_sku) DO UPDATE SET
                  description   = EXCLUDED.description,
                  unit_amount   = EXCLUDED.unit_amount,
                  supplier      = EXCLUDED.supplier,
                  _ingestion_ts = EXCLUDED._ingestion_ts,
                  _batch_id     = EXCLUDED._batch_id;
                """
            )
        )

    # 6) Log de succès
    print(f"OK : {len(df)} lignes chargées → silver.products (source: {f.name})")


if __name__ == "__main__":
    main()
