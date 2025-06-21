import streamlit as st

st.title("MLFlow Integration")

# Option 1: Redirect button
if st.button("Open MLFlow Dashboard", key="mlflow_btn"):
    st.markdown(
        "[Click here to open MLFlow in a new tab](http://localhost:5001/#/models)",
        unsafe_allow_html=True,
    )
