# Retail ETL Pipeline

## Objectif
Mettre en place un pipeline **ETL complet** (API → Bronze → Silver → Gold) orchestré avec **Airflow** et transformé avec **dbt**.  


## Architecture

### 1. **Bronze**
- Données brutes récupérées depuis l’API (auth via API Key).
- Stockage sous forme de fichiers **NDJSON** :
  - `customers`
  - `products`
  - `sales_customer`
  - `sales_product`
- Ingestion incrémentale basée sur `last_sales_id` stocké dans la table `meta.ingestion_state` (Postgres).

### 2. **Silver**
- Chargement des NDJSON Bronze dans des tables **Postgres** :
  - `silver.sales_customer`
  - `silver.sales_product`
  - `silver.products`
  - `silver.customers`
- Format structuré et prêt à être exploité par dbt.

### 3. **Gold**
Transformations gérées par **dbt** :
- `dim_customer` : table de dimension clients.  
- `dim_product` : table de dimension produits.  
- `sales_item` : table de faits (grain = `sale_id + line_no`).  
- `mart_sales_by_customer` : ventes agrégées par client et par jour.


## Orchestration Airflow

Le DAG `retail_etl_hourly` orchestre toutes les étapes :

1. **Bronze** : ingestion API → NDJSON.  
2. **Silver** : chargement NDJSON → Postgres.  
3. **Gold (dbt)** :
   - `dbt_deps` : installation des dépendances dbt.  
   - `dbt_run_core` : exécution des modèles principaux (`dim_customer`, `dim_product`, `sales_item`).  
   - `dbt_run_mart` : exécution du mart (`mart_sales_by_customer`).  
   - `dbt_test` : exécution des tests de qualité dbt (ex. `unique` et `not_null`).  

### Planification
- Le DAG est configuré pour s’exécuter **toutes les heures** (`@hourly`).  



## Lancement

### 1. Démarrer l’infra
```bash
docker compose up -d --build
```


## Architecture du projet
```
retail/
│
├── airflow/               # DAGs, logs, plugins
│   └── dags/retail_ETL.py
│
├── data/                  # Scripts de collecte Bronze
│   └── sales.py
│   └── customers.py
│   └── products.py
├── loaders/               # chargement des données
│   └── config.py
│   └── load_all_silver.py
│   └── load_customers.py
│   └── load_products.py
│   └── load_sales_customers.py
│   └── load_sales_products.py
│   └── state_store.py
│
├── dbt/                   # Projet dbt
│   ├── models/
│   │   ├── gold/
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_product.sql
│   │   │   ├── sales_item.sql
│   │   │   └── mart_sales_by_customer.sql
│
├── docker-compose.yml
├── requirements.txt
└── README.md 
└── dbt_project.yml
```