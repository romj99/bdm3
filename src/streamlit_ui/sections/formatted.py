import os
from pathlib import Path

import polars as pl
import streamlit as st
from deltalake import DeltaTable

FORMATTED_PATH = os.environ.get("FORMATTED_ZONE")

st.title("🔩 Formatted Zone")
st.write("View formatted datasets stored in the Delta Lake.")

if not FORMATTED_PATH or not os.path.exists(FORMATTED_PATH):
    st.error(f"Formatted Zone path not found: {FORMATTED_PATH}")
    st.stop()

# Find Delta table folders
delta_folders = []
for item in Path(FORMATTED_PATH).iterdir():
    if item.is_dir() and (item / "_delta_log").exists():
        delta_folders.append(item.name)

if not delta_folders:
    st.warning("No Delta tables found. - Formatted output already created?")
    st.stop()

# Folder selector
selected_folder = st.selectbox("Select table:", delta_folders)

if selected_folder:
    table_path = os.path.join(FORMATTED_PATH, selected_folder)

    # Load and show table snapshot
    try:
        delta_table = DeltaTable(table_path).to_pyarrow_table()
        df = pl.from_arrow(delta_table).head(100)

        st.subheader(f"📊 {selected_folder}")
        st.write(f"Showing first 100 rows of {df.shape[0]:,} total rows")

        st.dataframe(df, use_container_width=True)

        st.subheader("📊 Statistics")
        st.dataframe(df.describe(), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading table: {e}")