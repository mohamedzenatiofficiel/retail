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


## API FastAPI

L’API backend FastAPI expose les données sources utilisées dans la couche Bronze :

/customers → données clients

/products → données produits

/sales → données de ventes (paginations supportées)

Ces endpoints sont consommés automatiquement par Airflow lors de l’ingestion en Bronze.


## Orchestration Airflow

Le DAG `retail_etl_hourly` orchestre toutes les étapes :

1. **Bronze** : ingestion API → NDJSON.  
2. **Silver** : chargement NDJSON → Postgres.  
3. **Gold (dbt)** :
   - `dbt_deps` : installation des dépendances dbt.  
   - `dbt_run_core` : exécution des modèles principaux (`dim_customer`, `dim_product`, `sales_item`).  
   - `dbt_run_mart` : exécution du mart (`mart_sales_by_customer`).  
   - `dbt_test` : exécution des tests de qualité dbt (ex. `unique` et `not_null`).  

### Tests dbt implémentés

unique et not_null sur gold.sales_item.sales_item_id.

### Planification
- Le DAG est configuré pour s’exécuter **toutes les heures** (`@hourly`).  



## Lancement

Définir la variable d’environnement pour dbt

Sous PowerShell (Windows) :

$env:DBT_PROFILES_DIR = "$PWD\dbt"

### 1. Démarrer l’infra

Démarrer l'API :
```
py -3.11 -m uvicorn backend.main:app --reload --port 4010
```

Sur un second terminal :
```bash

docker compose up -d postgres pgadmin airflow-init

docker compose up -d airflow-webserver airflow-scheduler
```
### 2. Interfaces disponibles

```
Airflow : http://localhost:8080

utilisateur : admin@example.com
mot de passe : admin

PgAdmin : http://localhost:5050

utilisateur : admin
mot de passe : admin

Postgres DB (accès direct) :

host : postgres

port : 5432

user : retail

password : retail

database : retail
```

## 3. Documentation dbt
```
dbt docs generate
dbt docs serve --host 127.0.0.1 --port 8081
```


## 4. Arrêter l’infra
```
docker compose down -v       # stoppe + supprime les volumes (DB, logs, etc.)
```

## Architecture du projet
```
retail/
│
├── backend/               # FastAPI
│   └── auth.py
│   └── data.py
│   └── main.py
│   └── models.py
│
├── airflow/               # DAGs, logs, plugins
│   └── dags/retail_ETL.py
│
├── data/                  # Scripts de collecte Bronze
│   └── sales.py
│   └── customers.py
│   └── products.py
│
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
├── sql/   
│   └── schema_tables.sql
│
├── docker-compose.yml
├── requirements.txt
└── README.md 
└── dbt_project.yml
```