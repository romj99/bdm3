import streamlit as st

st.set_page_config(layout="wide", page_title="BDM Lab 3", page_icon=":rocket:")

st.logo("imgs/logo_header.svg")

# Your navigation
pages = {
    "🏠 Landing Zone": [
        st.Page("sections/landing.py", title="Data Upload", default=True),
        st.Page("sections/data_explorer.py", title=" Data Explorer"),
    ],
    "📦 Formatted Zone": [
        st.Page("sections/formatted.py", title="Formatted"),
    ],
    "🏭 Exploitation Zone": [
        st.Page("sections/exploitation.py", title="Exploitation"),
    ],
    "🩺 Data Quality": [
        st.Page("sections/validation/data_sanity.py", title="Data Sanity"),
        st.Page(
            "sections/validation/pipeline_monitoring.py", title="Pipeline Monitoring"
        ),
    ],
    "🏘️ External Tools": [
        st.Page("sections/airflow.py", title="⏱️ Airflow"),
        st.Page("sections/mlflow.py", title="🧪 MLFlow"),
    ],
}

pg = st.navigation(pages)
pg.run()