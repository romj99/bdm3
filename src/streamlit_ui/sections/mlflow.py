import streamlit as st
import streamlit.components.v1 as components

components.iframe("http://localhost:5001", height=800, scrolling=True)

st.markdown(
    "[Click here to open MLFlow in a new tab](http://localhost:5001/)",
    unsafe_allow_html=True,
)