
import os
import sys

sys.path.append(os.path.abspath(os.path.join("../..")))
sys.path.append(os.path.abspath(os.path.join("..")))


import streamlit as st
# from config import DATA_PATHS




paths = sys.path

st.title("🛬 Landing Zone")
st.text("Drag and drop the files you want to upload to the Datalake here.")

col1, col2 = st.columns([0.5, 0.5])

with col1:
    uploaded_files = st.file_uploader(
        "Upload CSV or JSON files",
        type=["csv", "json"],
        accept_multiple_files=True,
        key=f"file_uploader_{st.session_state.upload_counter}",
        help="Drag and drop your files here or click to select files from your computer.",
        label_visibility="collapsed",
    )
    dataset_name = st.text_input(
        "Dataset Name", value="Dataset", help="Give your dataset a custom name"
    )

with col2:
    st.write(f"File explorer: \n {paths}")