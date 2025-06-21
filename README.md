<h1 align="center">🚀 BDM3: Data Lake Architecture with Spark, MLflow & Airflow</h1>
<p align="center">
  <b>Julian Romero & Moritz Peist</b><br>
  Barcelona School of Economics · 2025
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python">
  <img src="https://img.shields.io/badge/PySpark-4.0-orange?logo=apachespark&logoColor=white">
  <img src="https://img.shields.io/badge/MLflow-3.0.0-blue?logo=mlflow">
  <img src="https://img.shields.io/badge/Airflow-3.0.2-darkgreen?logo=apacheairflow">
  <img src="https://img.shields.io/badge/Streamlit-App-red?logo=streamlit">
</p>

---

## 📑 Table of Contents

- [🧠 Project Overview](#-project-overview)
- [✨ Features](#-features)
- [🛠️ Setup & Running](#️-setup--running)
  - [📦 Prerequisites](#1--prerequisites)
  - [🚀 Start Everything (Docker)](#2--start-everything-docker)
  - [🧪 Run Notebooks (Optional)](#3--run-notebooks-optional)
- [📁 Project Structure](#-project-structure)
- [🧩 Technologies Used](#-technologies-used)



## 🧠 Project Overview

This project implements a **modular data lake architecture** and pipeline ecosystem as part of the [Lab 3 assignment](./Lab3-Assignment-Statement.pdf) for the course **Big Data Management for Data Science**.

It simulates:
- A local **data lake** with structured zones: **Landing**, **Formatted**, and **Exploitation**.
- A **Spark-based ETL pipeline**.
- **ML model training and tracking** via MLflow.
- **Pipeline orchestration** with Airflow.
- A unified **Streamlit UI** to navigate and interact with everything.

The datasets selected for the project:

- Idealista data (house prices).
- Income data.
- Cultural data.



> ⚠️ **Disclaimer I**: The Streamlit UI was designed specifically for the datasets used during this project. It may require significant adjustments in terms of **validation, scalability, and security** to support production-grade applications.

> ⚠️ **Disclaimer II**: The ML modeling and MLflow tracking components are included to **demonstrate the integration of Big Data tools**, not to optimize model performance. The focus is on **infrastructure and pipeline design**.

> ⚠️ **Disclaimer III**: The datasets used in this project originate from different sources and cover **non-overlapping time periods**. This inconsistency was **intentionally overlooked** to simulate a working end-to-end ML pipeline for predicting housing prices. The primary objective is to demonstrate the use of Big Data tools—not to achieve rigorous modeling accuracy.


## ✨ Features

- 🔄 **Data ingestion pipeline** from raw JSON/CSV into clean Parquet format
- 🧼 **Data cleaning, enrichment**, and formatting with PySpark
- 📊 **Exploratory Data Analysis** and KPIs in Streamlit
- 🧠 **Model training** with MLflow tracking (accuracy, recall, parameters)
- 🪄 **Automatic deployment** of the best model
- ⚙️ **Airflow DAG orchestration**
- 📂 Upload raw datasets via UI and browse zone contents dynamically



## 🛠️ Setup & Running

### 1. 📦 Prerequisites
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- Optional: [uv](https://github.com/astral-sh/uv) for Python environment management

---

### 2. 🚀 Start Everything (Docker)
The following command should be run to build the docker image and start the services:

```bash
docker compose up
````

The services are pointing to the following ports of your local machine, where they all are accessible from the Streamlit App UI:

* Streamlit App: [http://localhost:8501](http://localhost:8501)
* Airflow UI: [http://localhost:8080](http://localhost:8080)
* MLflow UI: [http://localhost:5001](http://localhost:5001)

You can upload data, trigger processing, monitor pipelines, and track ML models — all from the app.

---

### 3. 🧪 Run Notebooks (Optional)

Install [`uv`](https://github.com/astral-sh/uv) and sync the Python environment:

```bash
uv sync
```


## 📁 Project Structure

```bash
bdm3/
├── data_zones/                 # 🗂️ Simulated data lake zones
│   ├── 01_landing/             # Raw uploaded files
│   ├── 02_formatted/           # Cleaned & standardized
│   ├── 03_exploitation/        # Feature-ready data
│
├── notebooks/                  # 📓 Exploratory notebooks
│   ├── a1.ipynb
│   ├── a2.ipynb
│   └── a3.ipynb
│
├── outputs/
│   └── mlruns/                 # 📁 MLflow tracking data
│
├── src/                        # ⚙️ Application logic
│   ├── airflow/                # DAGs & Airflow Dockerfile
│   ├── ml/                     # Model training & selection
│   ├── pipelines/              # Spark-based ETL jobs
│   ├── streamlit_ui/           # Streamlit app (multi-page)
│   └── utils/                  # Shared logic/utilities
│
├── .env.example                # 🔧 Environment config example
├── docker-compose.yml          # 🐳 Service orchestration
├── pyproject.toml              # 📦 Project dependencies (via uv/pip)
├── uv.lock                     # 🔒 Dependency lock file
├── .python-version             # Python version (3.11)
└── README.md                   # 📖 This file
```

---

## 🧩 Technologies Used

* **Apache Spark 4.0 (PySpark)** – Distributed data processing
* **MLflow 2.0.1** – Model management and experiment tracking
* **Apache Airflow 3.0.2** – Workflow scheduling and orchestration
* **Streamlit** – UI for uploads, previews, and dashboards
* **Docker** – Reproducible, containerized development
