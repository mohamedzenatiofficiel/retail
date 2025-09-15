# Objectif :
#  - charger le dernier NDJSON Bronze (sales_customer) vers Silver
#  - mettre à jour le checkpoint meta.ingestion_state (last_sales_id)
#
# Pourquoi :
#  - sales_customer contient 1 ligne par vente (id, datetime, total_amount, customer_id)
#  - on mémorise le max(id) pour l'ingestion incrémentale /sales (prochain start_sales_id)

from sqlalchemy import text

from .config import get_engine, read_latest_ndjson


def main():
    # 1) Lire le dernier NDJSON de sales_customer (Bronze)
    df, f = read_latest_ndjson("bronze/sales_customer")

    # 2) Ouvrir une transaction
    engine = get_engine()
    with engine.begin() as conn:
        # 3) Table temporaire (même structure) pour staging
        conn.execute(
            text(
                "CREATE TEMP TABLE tmp_sales_customer (LIKE silver.sales_customer INCLUDING ALL);"
            )
        )

        # 4) Charger le DataFrame dans la table temporaire
        df.to_sql("tmp_sales_customer", con=conn, if_exists="append", index=False)

        # 5) Upsert : fusionner staging -> table finale (clé = id)
        conn.execute(
            text(
                """
                INSERT INTO silver.sales_customer AS t
                  (id, datetime, total_amount, customer_id, _ingestion_ts, _batch_id)
                SELECT id, datetime, total_amount, customer_id, _ingestion_ts, _batch_id
                FROM tmp_sales_customer
                ON CONFLICT (id) DO UPDATE SET
                  datetime      = EXCLUDED.datetime,
                  total_amount  = EXCLUDED.total_amount,
                  customer_id   = EXCLUDED.customer_id,
                  _ingestion_ts = EXCLUDED._ingestion_ts,
                  _batch_id     = EXCLUDED._batch_id;
                """
            )
        )

        # 6) Mettre à jour le checkpoint meta (dernier id de vente ingéré)
        if not df.empty:
            max_id = int(df["id"].max())

            # - INSERT une 1ère fois si 'sales' n'existe pas dans meta
            # - sinon UPDATE en gardant le max existant (GREATEST)
            conn.execute(
                text(
                    """
                    INSERT INTO meta.ingestion_state (source_name, last_sales_id, last_run_ts, note)
                    VALUES ('sales', :max_id, NOW(), 'upsert')
                    ON CONFLICT (source_name)
                    DO UPDATE SET
                      last_sales_id = GREATEST(meta.ingestion_state.last_sales_id, EXCLUDED.last_sales_id),
                      last_run_ts   = NOW(),
                      note          = 'upsert';
                    """
                ),
                {"max_id": max_id},
            )

    # 7) Log
    print(
        f"OK : {len(df)} ventes chargées → silver.sales_customer (source: {f.name})"
    )


if __name__ == "__main__":
    main()
