import os
import streamlit as st

FORMATTED_PATH = os.environ.get("FORMATTED_ZONE")

st.title("🔩 Formatted Zone")
st.write("In this section, you can view and manage the formatted datasets stored in the Datalake (Formatted Zone).")


st.subheader("📁 Formatted Zone Explorer")
formated_root = FORMATTED_PATH

for dirpath, dirnames, filenames in os.walk(formated_root):
    level = dirpath.replace(formated_root, "").count(os.sep)
    indent = "— " * level
    relative_dir = os.path.relpath(dirpath, formated_root)

    if relative_dir != ".":
        with st.expander(f"{indent}{relative_dir}/", expanded=False):
            if filenames:
                for file in filenames:
                    st.markdown(f"- `{file}`")
            else:
                st.markdown("*_No files in this folder_*")