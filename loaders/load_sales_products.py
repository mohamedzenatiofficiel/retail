# Objectif :
#  - charger le dernier NDJSON Bronze (sales_product) vers Silver
# Détail :
#  - 1 ligne = 1 produit vendu dans une vente
#  - clé primaire composite = (sale_id, line_no)
from sqlalchemy import text

from .config import get_engine, read_latest_ndjson


def main():
    # 1) Lire le dernier NDJSON de sales_product (Bronze)
    df, f = read_latest_ndjson("data/bronze/sales_product")

    # 2) Ouvrir une transaction
    engine = get_engine()
    with engine.begin() as conn:
        # 3) Table temporaire calquée sur la table cible
        conn.execute(
            text(
                "CREATE TEMP TABLE tmp_sales_product (LIKE silver.sales_product INCLUDING ALL);"
            )
        )

        # 4) Charger le DataFrame dans la table temporaire
        df.to_sql("tmp_sales_product", con=conn, if_exists="append", index=False)

        # 5) Upsert : fusionner staging -> table finale (clé composite)
        conn.execute(
            text(
                """
                INSERT INTO silver.sales_product AS t
                  (sale_id, line_no, product_sku, quantity, amount, _ingestion_ts, _batch_id)
                SELECT sale_id, line_no, product_sku, quantity, amount, _ingestion_ts, _batch_id
                FROM tmp_sales_product
                ON CONFLICT (sale_id, line_no) DO UPDATE SET
                  product_sku   = EXCLUDED.product_sku,
                  quantity      = EXCLUDED.quantity,
                  amount        = EXCLUDED.amount,
                  _ingestion_ts = EXCLUDED._ingestion_ts,
                  _batch_id     = EXCLUDED._batch_id;
                """
            )
        )

    # 6) Log
    print(
        f"OK : {len(df)} lignes chargées → silver.sales_product (source: {f.name})"
    )


if __name__ == "__main__":
    main()
