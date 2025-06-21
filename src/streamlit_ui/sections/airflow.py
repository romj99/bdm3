import streamlit as st
import streamlit.components.v1 as components

components.iframe("http://localhost:8080/dags", height=800, scrolling=True)

st.markdown(
    "[Click here to open Airflow in a new tab](http://localhost:8080/)",
    unsafe_allow_html=True,
)