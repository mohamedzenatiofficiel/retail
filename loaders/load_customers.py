# Objectif : charger le dernier NDJSON de Bronze (customers) vers Silver (Postgres)
# Points spécifiques :
#  - emails et phone_numbers sont des tableaux (TEXT[]) côté Postgres
from sqlalchemy import text

from .config import get_engine, read_latest_ndjson


def main():
    # 1) Lire le dernier NDJSON de customers (Bronze)
    df, f = read_latest_ndjson("data/bronze/customers")

    # 2) Ouvrir une transaction
    engine = get_engine()
    with engine.begin() as conn:
        # 3) Table temporaire avec la même structure que la table cible
        conn.execute(
            text("CREATE TEMP TABLE tmp_customers (LIKE silver.customers INCLUDING ALL);")
        )

        # 4) Charger le DataFrame dans la table temporaire
        df.to_sql("tmp_customers", con=conn, if_exists="append", index=False)

        # 5) Upsert : fusionner staging -> table finale (clé = customer_id)
        conn.execute(
            text(
                """
                INSERT INTO silver.customers AS t
                  (customer_id, emails, phone_numbers, _ingestion_ts, _batch_id)
                SELECT customer_id, emails, phone_numbers, _ingestion_ts, _batch_id
                FROM tmp_customers
                ON CONFLICT (customer_id) DO UPDATE SET
                  emails        = EXCLUDED.emails,
                  phone_numbers = EXCLUDED.phone_numbers,
                  _ingestion_ts = EXCLUDED._ingestion_ts,
                  _batch_id     = EXCLUDED._batch_id;
                """
            )
        )

    # 6) Log
    print(f"OK : {len(df)} lignes chargées → silver.customers (source: {f.name})")


if __name__ == "__main__":
    main()
