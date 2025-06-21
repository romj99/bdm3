import tempfile
from pathlib import Path
from typing import Dict, List

import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots


def read_many(path: Path | str) -> pl.DataFrame:
    """
    Read and concatenate multiple CSV or JSON files from a directory.
    """
    f_path = Path(path)

    # Get all CSV and JSON files
    csv_files = list(f_path.glob("*.csv"))
    json_files = list(f_path.glob("*.json"))

    # Validate that folder contains only one file type
    if csv_files and json_files:
        raise ValueError(
            "Folder contains both CSV and JSON files. Only one file type is supported."
        )

    if not csv_files and not json_files:
        raise ValueError("No CSV or JSON files found in the specified folder.")

    # Process based on file type found
    if csv_files:
        # CSV logic
        print(f"Reading {len(csv_files)} CSV file(s)")
        queries = [pl.scan_csv(file) for file in csv_files]
        dataframes = pl.collect_all(queries)
    else:
        # JSON logic with error handling
        print(f"Reading {len(json_files)} JSON file(s)")
        dataframes = []
        for file in json_files:
            try:
                # Try standard JSON first
                df = pl.read_json(file, infer_schema_length=10000)
                dataframes.append(df)
            except pl.ComputeError:
                try:
                    # If that fails, try JSONL/NDJSON format
                    df = pl.read_ndjson(file, infer_schema_length=10000)
                    dataframes.append(df)
                except Exception as e:
                    raise ValueError(f"Failed to read JSON file {file}: {e}")

    return pl.concat(dataframes, how="diagonal_relaxed")


@st.cache_data
def read_uploaded_files(uploaded_files) -> Dict[str, pl.DataFrame]:
    """Process uploaded files using the read_many approach."""
    datasets = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Group files by type
        csv_files = []
        json_files = []

        for uploaded_file in uploaded_files:
            file_ext = uploaded_file.name.split(".")[-1].lower()
            if file_ext == "csv":
                csv_files.append(uploaded_file)
            elif file_ext == "json":
                json_files.append(uploaded_file)

        # Process CSV files as a group if any
        if csv_files:
            csv_dir = temp_path / "csv"
            csv_dir.mkdir()
            for uploaded_file in csv_files:
                file_path = csv_dir / uploaded_file.name
                file_path.write_bytes(uploaded_file.read())

            try:
                csv_df = read_many(csv_dir)
                datasets["CSV_Combined"] = csv_df
            except Exception as e:
                st.error(f"Error reading CSV files: {e}")

        # Process JSON files as a group if any
        if json_files:
            json_dir = temp_path / "json"
            json_dir.mkdir()
            for uploaded_file in json_files:
                file_path = json_dir / uploaded_file.name
                file_path.write_bytes(uploaded_file.read())

            try:
                json_df = read_many(json_dir)
                datasets["JSON_Combined"] = json_df
            except Exception as e:
                st.error(f"Error reading JSON files: {e}")

    return datasets


def get_column_types(df: pl.DataFrame) -> Dict[str, List[str]]:
    """Categorize columns by data type."""
    numeric_cols = [
        col
        for col in df.columns
        if df[col].dtype
        in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]
    ]
    categorical_cols = [
        col for col in df.columns if df[col].dtype in [pl.Utf8, pl.Categorical]
    ]
    date_cols = [col for col in df.columns if df[col].dtype in [pl.Date, pl.Datetime]]
    other_cols = [
        col
        for col in df.columns
        if col not in numeric_cols + categorical_cols + date_cols
    ]

    return {
        "numeric": numeric_cols,
        "categorical": categorical_cols,
        "date": date_cols,
        "other": other_cols,
    }


def calculate_quality_metrics(df: pl.DataFrame) -> Dict:
    """Calculate data quality metrics."""
    n_rows, n_cols = df.shape

    missing_summary = df.null_count()
    total_missing = missing_summary.sum_horizontal().item(0)
    missing_pct = (
        (total_missing / (n_rows * n_cols)) * 100 if n_rows * n_cols > 0 else 0
    )

    duplicate_count = n_rows - df.unique().shape[0]
    duplicate_pct = (duplicate_count / n_rows) * 100 if n_rows > 0 else 0

    missing_by_column = []
    for col in df.columns:
        missing_count = df[col].null_count()
        if missing_count > 0:
            missing_pct_col = (missing_count / n_rows) * 100
            missing_by_column.append((col, missing_count, missing_pct_col))

    return {
        "total_missing": total_missing,
        "missing_percentage": missing_pct,
        "duplicate_count": duplicate_count,
        "duplicate_percentage": duplicate_pct,
        "missing_by_column": sorted(
            missing_by_column, key=lambda x: x[2], reverse=True
        ),
        "total_rows": n_rows,
        "total_columns": n_cols,
        "memory_usage": df.estimated_size("mb"),
    }


def create_overview_dashboard(df: pl.DataFrame, name: str) -> go.Figure:
    """Create overview dashboard."""
    metrics = calculate_quality_metrics(df)
    col_types = get_column_types(df)

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=[
            "Data Types",
            "Quality Score",
            "Missing Values",
            "Dataset Info",
        ],
        specs=[
            [{"type": "pie"}, {"type": "indicator"}],
            [{"type": "bar"}, {"type": "table"}],
        ],
    )

    # Data type pie chart
    type_counts = [len(col_types[t]) for t in col_types if len(col_types[t]) > 0]
    type_labels = [t.title() for t in col_types if len(col_types[t]) > 0]

    if type_counts:
        fig.add_trace(go.Pie(labels=type_labels, values=type_counts), row=1, col=1)

    # Quality score
    quality_score = 100 - (
        metrics["missing_percentage"] + metrics["duplicate_percentage"]
    )
    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=max(0, quality_score),
            title={"text": "Quality"},
            gauge={"axis": {"range": [0, 100]}, "bar": {"color": "darkblue"}},
        ),
        row=1,
        col=2,
    )

    # Missing values
    if metrics["missing_by_column"]:
        missing_data = metrics["missing_by_column"][:10]
        cols, counts, pcts = zip(*missing_data)
        fig.add_trace(go.Bar(x=list(pcts), y=list(cols), orientation="h"), row=2, col=1)

    # Info table
    info_data = [
        ["Rows", f"{metrics['total_rows']:,}"],
        ["Columns", f"{metrics['total_columns']:,}"],
        ["Memory", f"{metrics['memory_usage']:.2f} MB"],
        ["Missing", f"{metrics['missing_percentage']:.1f}%"],
    ]
    fig.add_trace(
        go.Table(
            header=dict(values=["Metric", "Value"]),
            cells=dict(
                values=[[row[0] for row in info_data], [row[1] for row in info_data]]
            ),
        ),
        row=2,
        col=2,
    )

    fig.update_layout(height=600, title_text=f"{name} Overview", showlegend=False)
    return fig


def main():
    st.title("🔍 Data Explorer")

    # Initialize session state
    if "datasets" not in st.session_state:
        st.session_state.datasets = {}
    if "current_dataset" not in st.session_state:
        st.session_state.current_dataset = None

    # File upload
    uploaded_files = st.file_uploader(
        "Upload CSV or JSON files", type=["csv", "json"], accept_multiple_files=True
    )

    if uploaded_files:
        with st.spinner("Processing files..."):
            datasets = read_uploaded_files(uploaded_files)
            st.session_state.datasets.update(datasets)
            st.success(f"Loaded {len(datasets)} dataset(s)")

    # Dataset selection
    if st.session_state.datasets:
        dataset_name = st.selectbox(
            "Select Dataset", list(st.session_state.datasets.keys())
        )
        st.session_state.current_dataset = dataset_name

        if dataset_name:
            df = st.session_state.datasets[dataset_name]

            # Overview
            fig = create_overview_dashboard(df, dataset_name)
            st.plotly_chart(fig, use_container_width=True)

            # Tabs
            tab1, tab2, tab3 = st.tabs(["📊 Data", "📈 Distributions", "🔍 Analysis"])

            with tab1:
                st.dataframe(df.head(100), use_container_width=True)

            with tab2:
                col_types = get_column_types(df)
                if col_types["numeric"]:
                    selected_cols = st.multiselect(
                        "Select numeric columns", col_types["numeric"]
                    )
                    if selected_cols:
                        for col in selected_cols[:3]:
                            values = df[col].drop_nulls().to_list()
                            fig = go.Figure(go.Histogram(x=values, title=col))
                            st.plotly_chart(fig, use_container_width=True)

            with tab3:
                metrics = calculate_quality_metrics(df)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Rows", f"{metrics['total_rows']:,}")
                    st.metric("Missing %", f"{metrics['missing_percentage']:.1f}%")
                with col2:
                    st.metric("Columns", metrics["total_columns"])
                    st.metric("Duplicates %", f"{metrics['duplicate_percentage']:.1f}%")

                if metrics["missing_by_column"]:
                    st.subheader("Missing Values by Column")
                    missing_df = pl.DataFrame(
                        {
                            "Column": [x[0] for x in metrics["missing_by_column"]],
                            "Missing_Count": [
                                x[1] for x in metrics["missing_by_column"]
                            ],
                            "Missing_Percent": [
                                round(x[2], 2) for x in metrics["missing_by_column"]
                            ],
                        }
                    )
                    st.dataframe(missing_df, use_container_width=True)


if __name__ == "__main__":
    main()
