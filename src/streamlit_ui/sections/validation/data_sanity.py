# Import and run the data sanity dashboard
# This file should be saved as sections/data_sanity.py

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


def load_validation_report(
    report_path: str = "outputs/data_validation_report.json",
) -> Optional[Dict[str, Any]]:
    """Load the validation report from JSON file."""
    try:
        report_file = Path(report_path)
        if report_file.exists():
            with open(report_file, "r") as f:
                return json.load(f)
        else:
            return None
    except Exception as e:
        st.error(f"Error loading validation report: {str(e)}")
        return None


def display_executive_summary(report: Dict[str, Any]) -> None:
    """Display executive summary with key metrics."""
    summary = report.get("executive_summary", {})

    st.subheader("🎯 Executive Summary")

    # Status indicator
    status = summary.get("overall_status", "UNKNOWN")
    if status == "PASSED":
        st.success(f"✅ **Pipeline Status: {status}**")
    else:
        st.error(f"❌ **Pipeline Status: {status}**")

    # Key metrics in columns
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        quality_score = summary.get("data_quality_score", 0)
        st.metric(
            label="Data Quality Score",
            value=f"{quality_score:.1f}%",
            delta=None,
            delta_color="normal" if quality_score >= 80 else "inverse",
        )

    with col2:
        load_rate = summary.get("load_success_rate", 0)
        st.metric(
            label="Load Success Rate",
            value=f"{load_rate:.1f}%",
            delta=None,
            delta_color="normal" if load_rate >= 90 else "inverse",
        )

    with col3:
        total_datasets = summary.get("total_datasets_validated", 0)
        st.metric(label="Datasets Validated", value=total_datasets)

    with col4:
        exec_time = summary.get("execution_time", "0s")
        st.metric(label="Execution Time", value=exec_time)


def display_zone_summary(report: Dict[str, Any]) -> None:
    """Display zone-wise summary information."""
    zone_summary = report.get("zone_summary", {})

    st.subheader("📁 Data Zone Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Formatted Zone")
        formatted = zone_summary.get("formatted_zone", {})

        formatted_datasets = formatted.get("total_datasets", 0)
        formatted_success = formatted.get("successful_loads", 0)
        formatted_records = formatted.get("total_records", 0)

        st.write(f"📊 **Total Datasets:** {formatted_datasets}")
        st.write(f"✅ **Successful Loads:** {formatted_success}")
        st.write(f"📝 **Total Records:** {formatted_records:,}")

        # Success rate bar
        if formatted_datasets > 0:
            success_rate = (formatted_success / formatted_datasets) * 100
            st.progress(success_rate / 100, text=f"Success Rate: {success_rate:.1f}%")

    with col2:
        st.markdown("#### Exploitation Zone")
        exploitation = zone_summary.get("exploitation_zone", {})

        exploitation_datasets = exploitation.get("total_datasets", 0)
        exploitation_success = exploitation.get("successful_loads", 0)
        exploitation_records = exploitation.get("total_records", 0)

        st.write(f"📊 **Total Datasets:** {exploitation_datasets}")
        st.write(f"✅ **Successful Loads:** {exploitation_success}")
        st.write(f"📝 **Total Records:** {exploitation_records:,}")

        # Success rate bar
        if exploitation_datasets > 0:
            success_rate = (exploitation_success / exploitation_datasets) * 100
            st.progress(success_rate / 100, text=f"Success Rate: {success_rate:.1f}%")


def create_quality_overview_chart(report: Dict[str, Any]) -> go.Figure:
    """Create quality overview visualization."""
    quality_details = report.get("data_quality_details", {})

    # Prepare data for visualization
    datasets = []
    duplicate_rates = []

    for dataset, details in quality_details.items():
        if isinstance(details, dict) and "duplicate_percentage" in details:
            datasets.append(dataset)
            duplicate_rates.append(details["duplicate_percentage"])

    # Create subplot figure
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=["Duplicate Rates by Dataset", "Data Quality Distribution"],
        specs=[[{"type": "bar"}, {"type": "pie"}]],
    )

    # Bar chart for duplicate rates
    if datasets:
        fig.add_trace(
            go.Bar(
                x=datasets,
                y=duplicate_rates,
                name="Duplicate %",
                marker_color="lightcoral",
            ),
            row=1,
            col=1,
        )

    # Pie chart for overall quality categories
    executive_summary = report.get("executive_summary", {})
    quality_score = executive_summary.get("data_quality_score", 0)

    quality_labels = ["High Quality", "Medium Quality", "Low Quality"]
    quality_values = [
        max(0, quality_score - 80) if quality_score > 80 else 0,
        min(80, max(0, quality_score - 50))
        if quality_score > 50
        else quality_score
        if quality_score > 0
        else 0,
        max(0, 50 - quality_score) if quality_score < 50 else 0,
    ]

    # Only add pie chart if we have values
    if sum(quality_values) > 0:
        fig.add_trace(
            go.Pie(
                labels=quality_labels,
                values=quality_values,
                name="Quality Distribution",
            ),
            row=1,
            col=2,
        )

    fig.update_layout(height=400, showlegend=True, title_text="Data Quality Overview")
    fig.update_xaxes(title_text="Datasets", row=1, col=1)
    fig.update_yaxes(title_text="Duplicate Rate (%)", row=1, col=1)

    return fig


def display_kpi_summary(report: Dict[str, Any]) -> None:
    """Display KPI calculation summary."""
    kpi_summary = report.get("kpi_summary", {})

    st.subheader("📈 KPI Summary")

    if not kpi_summary:
        st.info("No KPI data available in the current report.")
        return

    # Housing Market KPIs
    if "avg_price_per_m2_by_district" in kpi_summary:
        housing_kpis = kpi_summary["avg_price_per_m2_by_district"]

        st.markdown("#### 🏠 Housing Market KPIs")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Max Price per m²", f"€{housing_kpis.get('max_price', 0):,.0f}")

        with col2:
            st.metric("Min Price per m²", f"€{housing_kpis.get('min_price', 0):,.0f}")

        with col3:
            st.metric("Avg Price per m²", f"€{housing_kpis.get('avg_price', 0):,.0f}")

    # Socioeconomic KPIs
    if "income_inequality" in kpi_summary:
        income_kpis = kpi_summary["income_inequality"]

        st.markdown("#### 💰 Socioeconomic KPIs")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Avg Income Inequality", f"{income_kpis.get('avg_inequality', 0):.1f}%"
            )

        with col2:
            st.metric(
                "Max Income Inequality", f"{income_kpis.get('max_inequality', 0):.1f}%"
            )

        with col3:
            st.metric(
                "Min Income Inequality", f"{income_kpis.get('min_inequality', 0):.1f}%"
            )

    # Cultural Accessibility KPIs
    if "cultural_accessibility" in kpi_summary:
        cultural_kpis = kpi_summary["cultural_accessibility"]

        st.markdown("#### 🎭 Cultural Accessibility KPIs")
        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Avg Cultural Density",
                f"{cultural_kpis.get('avg_cultural_density', 0):.2f} per 1000",
            )

        with col2:
            st.metric(
                "Total Cultural Sites",
                f"{cultural_kpis.get('total_cultural_sites', 0):,}",
            )

    # Integration Quality
    if "integration_quality" in kpi_summary:
        integration_kpis = kpi_summary["integration_quality"]

        st.markdown("#### 🔗 Data Integration Quality")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Records", f"{integration_kpis.get('total_records', 0):,}")

        with col2:
            st.metric(
                "Complete Records", f"{integration_kpis.get('complete_records', 0):,}"
            )

        with col3:
            completeness = integration_kpis.get("completeness_percentage", 0)
            st.metric(
                "Completeness Rate",
                f"{completeness:.1f}%",
                delta=None,
                delta_color="normal" if completeness >= 90 else "inverse",
            )


def display_recommendations(report: Dict[str, Any]) -> None:
    """Display actionable recommendations."""
    recommendations = report.get("recommendations", [])

    st.subheader("💡 Recommendations")

    if not recommendations:
        st.success("✅ No issues identified. All validations passed!")
        return

    for i, recommendation in enumerate(recommendations, 1):
        if recommendation.startswith("✅"):
            st.success(recommendation)
        elif recommendation.startswith("⚠️"):
            st.warning(recommendation)
        else:
            st.info(f"{i}. {recommendation}")


def display_performance_metrics(report: Dict[str, Any]) -> None:
    """Display pipeline performance metrics."""
    performance = report.get("performance_metrics", {})

    st.subheader("⚡ Performance Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        exec_time = performance.get("execution_time_seconds", 0)
        st.metric("Execution Time", f"{exec_time:.2f}s")

    with col2:
        spark_version = performance.get("spark_version", "Unknown")
        st.metric("Spark Version", spark_version)

    with col3:
        start_time = performance.get("start_time", "")
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                st.metric("Start Time", start_dt.strftime("%H:%M:%S"))
            except:
                st.metric("Start Time", "Unknown")

    with col4:
        end_time = performance.get("end_time", "")
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                st.metric("End Time", end_dt.strftime("%H:%M:%S"))
            except:
                st.metric("End Time", "Unknown")


# Main execution
st.title("🩺 Data Sanity Dashboard")
st.markdown(
    "Comprehensive data validation and quality monitoring for the Barcelona Data Pipeline"
)

# Load validation report
output_path = os.getenv("OUTPUT_PATH", "/outputs")
report_path = f"{output_path}/data_validation_report.json"

report = load_validation_report(report_path)

if report is None:
    st.warning(
        "⚠️ No validation report found. Please run the data validation pipeline first."
    )
    st.info(f"Expected report location: `{report_path}`")

    # Show sample structure
    with st.expander("Expected Report Structure"):
        st.code(
            """
        {
          "report_metadata": {...},
          "executive_summary": {...},
          "zone_summary": {...},
          "kpi_summary": {...},
          "recommendations": [...]
        }
        """,
            language="json",
        )

    # Instructions
    st.markdown("### 🚀 How to Generate a Report")
    st.markdown("""
    1. **Run the Data Pipeline**: Execute the Airflow DAG `bcn_data_pipeline_with_validation`
    2. **Manual Execution**: Run the A4 validation pipeline directly:
       ```bash
       python src/pipelines/a4.py
       ```
    3. **Check Output Path**: Ensure the output directory is configured correctly
    """)

else:
    # Display report metadata
    metadata = report.get("report_metadata", {})
    generated_at = metadata.get("generated_at", "Unknown")

    if generated_at != "Unknown":
        try:
            gen_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            st.caption(
                f"📅 Report generated: {gen_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
        except:
            st.caption(f"📅 Report generated: {generated_at}")

    # Main dashboard sections
    display_executive_summary(report)
    st.divider()

    display_zone_summary(report)
    st.divider()

    # Quality overview chart
    try:
        fig = create_quality_overview_chart(report)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating quality chart: {str(e)}")

    st.divider()

    display_kpi_summary(report)
    st.divider()

    display_recommendations(report)
    st.divider()

    display_performance_metrics(report)

    # Refresh button
    st.divider()
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🔄 Refresh Report", help="Reload the validation report"):
            st.rerun()
