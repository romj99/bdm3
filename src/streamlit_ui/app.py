import streamlit as st

st.set_page_config(layout="wide", page_title="BDM Lab 3", page_icon=":rocket:")

st.logo("imgs/logo_header.svg")

# Simple sidebar with external tools
with st.sidebar:
    st.markdown("### 🔗 External Tools")

    # MLflow card
    with st.container(border=True):
        col1, col2 = st.columns([1, 3], vertical_alignment="center")
        with col1:
            st.image("imgs/mlflow.svg", width=56)
        with col2:
            st.markdown("[**MLflow**](http://localhost:5001)")

    # Airflow card
    with st.container(border=True):
        col1, col2 = st.columns([1, 3], vertical_alignment="center")
        with col1:
            st.image("imgs/airflow.svg", width=56)
        with col2:
            st.markdown("[**Airflow**](http://localhost:8080)")

# Your navigation
pages = {
    "🏠 Landing Zone": [
        st.Page("sections/landing.py", title="Landing Zone", default=True),
        st.Page("sections/data_explorer.py", title="Data Explorer"),
    ],
    "📦 Formatted Zone": [
        st.Page("sections/formatted.py", title="Formatted"),
    ],
}

pg = st.navigation(pages)
pg.run()