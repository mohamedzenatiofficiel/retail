from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Pour pouvoir importer tes modules (montés dans /opt/retail)
import sys
if "/opt/retail" not in sys.path:
    sys.path.insert(0, "/opt/retail")

# ---- Callables qui appellent tes scripts existants ----
def run_products_bronze():
    # data/products.py : écrit NDJSON en bronze/products
    from data.products import main as products_main
    products_main()

def run_customers_bronze():
    from data.customers import main as customers_main
    customers_main()

def run_sales_bronze():
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

    # Dépendances
    t_products_bronze >> t_load_products
    t_customers_bronze >> t_load_customers
    t_sales_bronze >> [t_load_sales_customer, t_load_sales_product]
