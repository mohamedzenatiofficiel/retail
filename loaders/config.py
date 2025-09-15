# Objectif : fonctions utilitaires partagées par tous les loaders
# - lire les variables d'environnement
# - ouvrir une connexion SQLAlchemy à Postgres
# - retrouver et lire le dernier fichier NDJSON d'un dossier

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    # 1) Construire l'URL de connexion Postgres à partir des variables d'env
    user = os.getenv("POSTGRES_USER", "retail")
    pwd = os.getenv("POSTGRES_PASSWORD", "retail")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "retail")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

    # 2) Créer l'engine SQLAlchemy (objet de connexion réutilisable)
    return create_engine(url)


# Ancre à la racine du projet 
ROOT_DIR = Path(__file__).resolve().parents[1]

# Emplacement des données
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT_DIR / "data"))

def latest_ndjson(subpath: str) -> Path:
    # 3) Récupérer le dernier fichier .ndjson d'un dossier
    folder = (DATA_DIR / subpath)
    files = sorted(folder.glob("*.ndjson"))
    if not files:
        raise FileNotFoundError(f"Aucun NDJSON trouvé dans : {folder}")
    return files[-1]


def read_latest_ndjson(subpath: str):
    # 4) Trouver le dernier NDJSON et le charger en DataFrame pandas
    f = latest_ndjson(subpath)
    df = pd.read_json(f, lines=True)
    return df, f
