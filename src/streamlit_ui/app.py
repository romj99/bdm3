import streamlit as st

st.set_page_config(layout="wide", page_title="BDM Lab 3", page_icon=":rocket:")

st.logo("imgs/logo_header.svg")
pages = {
    "🏠 Landing Zone": [
        st.Page("sections/home.py", title="Home", default=True),
    ],
    "📦 Formatted Zone": [
        st.Page("sections/formatted.py", title="Formatted"),
    ],
    "🧪 Exploitation Zone": [
        st.Page("sections/mlflow.py", title="MLFlow"),
    ],
    "⏱️ Scheduling": [
        st.Page("sections/airflow.py", title="Airflow"),
    ],
}

pg = st.navigation(pages)
pg.run()