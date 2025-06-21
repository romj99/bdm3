import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

DATA_PATHS = {
    "landing": os.path.join(BASE_DIR, "data_zones/01_landing"),
    "formatted": os.path.join(BASE_DIR, "data_zones/02_formatted"),
    "exploitation": os.path.join(BASE_DIR, "data_zones/03_exploitation"),
}

MLFLOW_URI = "http://localhost:5001"
AIRFLOW_URI = "http://localhost:8080"
