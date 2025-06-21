import streamlit as st

st.set_page_config(layout="wide", page_title="BDM Lab 3", page_icon=":rocket:")

st.logo("imgs/logo_header.svg")

# Your navigation
pages = {
    "🏠 Landing Zone": [
        st.Page("sections/landing.py", title="Landing Zone", default=True),
        st.Page("sections/data_explorer.py", title="Data Explorer"),
    ],
    "📦 Formatted Zone": [
        st.Page("sections/formatted.py", title="Formatted"),
    ],
    "🧪 Experimentation": [
        st.Page("sections/mlflow.py", title="MLFlow"),
    ],
    "⏱️ Scheduling": [
        st.Page("sections/airflow.py", title="Airflow"),
    ],
}

pg = st.navigation(pages)
pg.run()