from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Pour pouvoir importer tes modules (montés dans /opt/retail)
import sys
if "/opt/retail" not in sys.path:
    sys.path.insert(0, "/opt/retail")


# --- Helpers: créer les dossiers bronze si manquants (évite les erreurs Pandas) ---
def ensure_bronze_dirs():
    """
    Crée (si besoin) les répertoires Bronze attendus par les scripts d'ingestion.
    """
    base = Path("/opt/retail/data/bronze")
    for sub in ("products", "customers", "sales_customer", "sales_product"):
        (base / sub).mkdir(parents=True, exist_ok=True)
    print("Bronze folders OK:", [p.name for p in (base).iterdir()])


# ---- Callables qui appellent tes scripts existants ----
def run_products_bronze():
    # data/products.py : écrit NDJSON en bronze/products
    from data.products import main as products_main
    products_main()

def run_customers_bronze():
    from data.customers import main as customers_main
    customers_main()

def run_sales_bronze():
    # ⚠️ version incrémentale (lit/maj meta.ingestion_state)
    from data.sales import main as sales_main
    sales_main()

def load_products_silver():
    from loaders.load_products import main as load_products_main
    load_products_main()

def load_customers_silver():
    from loaders.load_customers import main as load_customers_main
    load_customers_main()

def load_sales_customer_silver():
    from loaders.load_sales_customers import main as load_sc_main
    load_sc_main()

def load_sales_product_silver():
    from loaders.load_sales_products import main as load_sp_main
    load_sp_main()


# ---- DAG ----
default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_etl_hourly",
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 * * * *",  # toutes les heures (minute 0)
    catchup=False,
    tags=["retail", "bronze", "silver"],
) as dag:

    # 0) S'assurer que les dossiers existent (avant toute écriture)
    t_prepare_dirs = PythonOperator(
        task_id="prepare_bronze_dirs",
        python_callable=ensure_bronze_dirs,
    )

    # 1) Bronze: API -> NDJSON
    t_products_bronze = PythonOperator(
        task_id="bronze_products",
        python_callable=run_products_bronze,
    )
    t_customers_bronze = PythonOperator(
        task_id="bronze_customers",
        python_callable=run_customers_bronze,
    )
    t_sales_bronze = PythonOperator(
        task_id="bronze_sales",
        python_callable=run_sales_bronze,
    )

    # 2) Silver: NDJSON -> Postgres
    t_load_products = PythonOperator(
        task_id="silver_load_products",
        python_callable=load_products_silver,
    )
    t_load_customers = PythonOperator(
        task_id="silver_load_customers",
        python_callable=load_customers_silver,
    )
    t_load_sales_customer = PythonOperator(
        task_id="silver_load_sales_customer",
        python_callable=load_sales_customer_silver,
    )
    t_load_sales_product = PythonOperator(
        task_id="silver_load_sales_product",
        python_callable=load_sales_product_silver,
    )

    # Tâches dbt (Gold + tests)
    DBT_PROJECT_DIR = "/opt/retail"
    DBT_PROFILES_DIR = "/opt/retail/dbt"
    dbt_env = {
        "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",

        # ⇩⇩ ADAPTE ces valeurs à ton infra ⇩⇩
        "POSTGRES_HOST": "postgres",   # <- nom du service Docker ou hostname réel, PAS "localhost"
        "POSTGRES_USER": "retail",
        "POSTGRES_PASSWORD": "retail",
        "POSTGRES_DB": "retail",
        "POSTGRES_PORT": "5432",
    }
    DBT = "python -m dbt.cli.main"

    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f'{DBT} deps --project-dir "{DBT_PROJECT_DIR}"',
        env=dbt_env,
    )

    # 1er run : dimensions + fait sales_item
    t_dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=(
            f'{DBT} run --project-dir "{DBT_PROJECT_DIR}" '
            " --select dim_customer dim_product sales_item"
        ),
        env=dbt_env,
    )

    t_dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            "cd /opt/retail && "
            "python -m dbt.cli.main run "
            "--project-dir /opt/retail "
            "--profiles-dir /opt/retail/dbt "
            "--select mart_daily_kpis "
            "--target dev --fail-fast"
        ),
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f'{DBT} test --project-dir "{DBT_PROJECT_DIR}"',
        env=dbt_env,
    )


    # Dépendances
    t_prepare_dirs >> [t_products_bronze, t_customers_bronze, t_sales_bronze]
    t_products_bronze >> t_load_products
    t_customers_bronze >> t_load_customers
    t_sales_bronze >> [t_load_sales_customer, t_load_sales_product]


    # Tous les Silver doivent finir avant dbt
    [t_load_products, t_load_customers, t_load_sales_customer, t_load_sales_product] >> t_dbt_deps
    t_dbt_deps >> t_dbt_run_core >> t_dbt_run_mart >> t_dbt_test