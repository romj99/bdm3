import os
import streamlit as st
from config import DATA_PATHS
st.title("🛬 Landing Zone")
st.write("Drag and drop files to upload them to the Datalake (Landing Zone).")

col1, col2 = st.columns([0.5, 0.5])

with col1:
    custom_name = st.text_input(
            "Dataset Name", 
            help="Give your dataset a custom name",
            placeholder="e.g. my_dataset"
        )
    files_uploaded = st.file_uploader(
        "Upload a files",
        accept_multiple_files=True
    )
    #add button to upload
    upload_confirm = st.button("Upload Files")

    if custom_name and files_uploaded and upload_confirm:
        dataset_name = custom_name.strip().replace(" ", "_").lower()
        save_dir = os.path.join(DATA_PATHS["landing"], dataset_name)
        os.makedirs(save_dir, exist_ok=True)

        with st.spinner("Saving files..."):
            for i, uploaded_file in enumerate(files_uploaded, 1):
                file_ext = os.path.splitext(uploaded_file.name)[1]
                new_filename = f"{dataset_name}_{i:03d}{file_ext}"  
                save_path = os.path.join(save_dir, new_filename)
                
                with open(save_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                st.success(f"✅ Saved as: {new_filename}")

with col2:
    st.subheader("📁 Landing Zone Explorer")
    landing_root = DATA_PATHS["landing"]

    for dirpath, dirnames, filenames in os.walk(landing_root):
        level = dirpath.replace(landing_root, "").count(os.sep)
        indent = "— " * level
        relative_dir = os.path.relpath(dirpath, landing_root)

        if relative_dir != ".":
            with st.expander(f"{indent}{relative_dir}/", expanded=False):
                if filenames:
                    for file in filenames:
                        st.markdown(f"- `{file}`")
                else:
                    st.markdown("*_No files in this folder_*")