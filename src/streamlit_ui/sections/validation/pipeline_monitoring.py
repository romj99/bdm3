# sections/pipeline_monitoring.py

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import psutil
import requests
import streamlit as st


@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_real_system_metrics() -> Dict[str, float]:
    """Get real system metrics using psutil."""
    try:
        return {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "memory_used_gb": psutil.virtual_memory().used / (1024**3),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "disk_usage_percent": psutil.disk_usage("/").percent,
            "disk_free_gb": psutil.disk_usage("/").free / (1024**3),
        }
    except Exception as e:
        st.error(f"Error getting system metrics: {e}")
        return {}


@st.cache_data(ttl=60)  # Cache for 1 minute
def get_airflow_dag_status() -> Dict:
    """Try to get real Airflow DAG status via REST API."""
    airflow_url = os.getenv("AIRFLOW_URI", "http://localhost:8080")

    try:
        # Try to get DAG runs
        response = requests.get(
            f"{airflow_url}/api/v1/dags/bcn_data_pipeline_with_validation/dagRuns",
            timeout=5,
            auth=("admin", "admin"),  # Default Airflow auth
        )

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}"}

    except requests.exceptions.RequestException as e:
        return {"error": f"Connection failed: {str(e)}"}


def get_real_validation_reports() -> List[Dict]:
    """Read actual validation reports from outputs directory."""
    output_path = Path(os.getenv("OUTPUT_PATH", "outputs"))
    reports = []

    if not output_path.exists():
        return reports

    # Look for validation reports
    for report_file in output_path.glob("*validation_report*.json"):
        try:
            with open(report_file, "r") as f:
                report_data = json.load(f)
                report_data["file_path"] = str(report_file)
                report_data["file_modified"] = datetime.fromtimestamp(
                    report_file.stat().st_mtime
                )
                reports.append(report_data)
        except Exception as e:
            st.error(f"Error reading report {report_file}: {e}")

    return sorted(reports, key=lambda x: x["file_modified"], reverse=True)


def get_real_pipeline_logs() -> List[Dict]:
    """Get real log files from the system."""
    logs = []

    # Common log locations
    log_locations = [
        ".",  # Current directory
        "outputs",
        "logs",
        "/tmp",
    ]

    log_patterns = [
        "*validation*.log",
        "*formatting*.log",
        "*exploitation*.log",
        "*pipeline*.log",
        "*.log",
    ]

    for location in log_locations:
        log_dir = Path(location)
        if log_dir.exists():
            for pattern in log_patterns:
                for log_file in log_dir.glob(pattern):
                    if log_file.is_file():
                        try:
                            stat = log_file.stat()
                            logs.append(
                                {
                                    "name": log_file.name,
                                    "path": str(log_file),
                                    "size": stat.st_size,
                                    "modified": datetime.fromtimestamp(stat.st_mtime),
                                    "location": location,
                                }
                            )
                        except Exception:
                            continue

    return sorted(logs, key=lambda x: x["modified"], reverse=True)


def get_real_data_zone_info() -> Dict[str, Dict]:
    """Get real information about data zones."""
    zones = {
        "landing": os.getenv("LANDING_ZONE", "data_zones/01_landing"),
        "formatted": os.getenv("FORMATTED_ZONE", "data_zones/02_formatted"),
        "exploitation": os.getenv("EXPLOITATION_ZONE", "data_zones/03_exploitation"),
        "outputs": os.getenv("OUTPUT_PATH", "outputs"),
    }

    zone_info = {}

    for zone_name, zone_path in zones.items():
        path = Path(zone_path)

        if path.exists():
            # Count files and directories
            files = list(path.rglob("*"))
            file_count = len([f for f in files if f.is_file()])
            dir_count = len([f for f in files if f.is_dir()])

            # Calculate total size
            total_size = sum(f.stat().st_size for f in files if f.is_file())

            # Get latest modification time
            try:
                latest_mod = max(f.stat().st_mtime for f in files if f.is_file())
                latest_mod_time = datetime.fromtimestamp(latest_mod)
            except ValueError:
                latest_mod_time = None

            zone_info[zone_name] = {
                "exists": True,
                "path": str(path),
                "file_count": file_count,
                "dir_count": dir_count,
                "total_size_mb": total_size / (1024 * 1024),
                "latest_modification": latest_mod_time,
            }
        else:
            zone_info[zone_name] = {
                "exists": False,
                "path": str(path),
                "file_count": 0,
                "dir_count": 0,
                "total_size_mb": 0,
                "latest_modification": None,
            }

    return zone_info


def get_docker_container_status() -> List[Dict]:
    """Get real Docker container status."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            containers = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    try:
                        container = json.loads(line)
                        containers.append(container)
                    except json.JSONDecodeError:
                        continue
            return containers
        else:
            return []

    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def display_real_system_status():
    """Display real system status and metrics."""
    st.subheader("💻 Real System Status")

    # Get real metrics
    metrics = get_real_system_metrics()

    if metrics:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            cpu = metrics.get("cpu_percent", 0)
            st.metric(
                "CPU Usage",
                f"{cpu:.1f}%",
                delta=None,
                delta_color="inverse" if cpu > 80 else "normal",
            )

        with col2:
            memory = metrics.get("memory_percent", 0)
            memory_used = metrics.get("memory_used_gb", 0)
            st.metric(
                "Memory Usage",
                f"{memory:.1f}% ({memory_used:.1f}GB)",
                delta=None,
                delta_color="inverse" if memory > 80 else "normal",
            )

        with col3:
            disk = metrics.get("disk_usage_percent", 0)
            st.metric(
                "Disk Usage",
                f"{disk:.1f}%",
                delta=None,
                delta_color="inverse" if disk > 90 else "normal",
            )

        with col4:
            disk_free = metrics.get("disk_free_gb", 0)
            st.metric("Free Space", f"{disk_free:.1f} GB")


def display_real_validation_history():
    """Display real validation report history."""
    st.subheader("📊 Real Validation History")

    reports = get_real_validation_reports()

    if not reports:
        st.info(
            "📝 No validation reports found yet. Run the pipeline to generate reports!"
        )
        st.code("python src/pipelines/a4.py")
        return

    st.write(f"Found {len(reports)} validation report(s)")

    # Show reports in expandable sections
    for i, report in enumerate(reports):
        modified_time = report["file_modified"].strftime("%Y-%m-%d %H:%M:%S")

        executive = report.get("executive_summary", {})
        status = executive.get("overall_status", "UNKNOWN")
        quality_score = executive.get("data_quality_score", 0)

        status_emoji = "✅" if status == "PASSED" else "❌"

        with st.expander(
            f"{status_emoji} Report {i + 1}: {modified_time} (Quality: {quality_score:.1f}%)"
        ):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Status", status)
                st.metric("Quality Score", f"{quality_score:.1f}%")

            with col2:
                st.metric(
                    "Load Success", f"{executive.get('load_success_rate', 0):.1f}%"
                )
                st.metric("Datasets", executive.get("total_datasets_validated", 0))

            with col3:
                st.metric("Execution Time", executive.get("execution_time", "Unknown"))
                st.caption(f"File: {Path(report['file_path']).name}")

            # Show recommendations if any
            recommendations = report.get("recommendations", [])
            if recommendations:
                st.markdown("**Recommendations:**")
                for rec in recommendations[:3]:  # Show first 3
                    st.write(f"• {rec}")


def display_real_data_zones():
    """Display real data zone information."""
    st.subheader("📁 Real Data Zone Status")

    zone_info = get_real_data_zone_info()

    for zone_name, info in zone_info.items():
        with st.container(border=True):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                if info["exists"]:
                    st.success(f"✅ {zone_name.title()}")
                else:
                    st.error(f"❌ {zone_name.title()}")
                st.caption(info["path"])

            with col2:
                st.metric("Files", info["file_count"])
                st.metric("Directories", info["dir_count"])

            with col3:
                st.metric("Size", f"{info['total_size_mb']:.1f} MB")

            with col4:
                if info["latest_modification"]:
                    time_ago = datetime.now() - info["latest_modification"]
                    if time_ago.days > 0:
                        st.metric("Last Update", f"{time_ago.days}d ago")
                    elif time_ago.seconds > 3600:
                        st.metric("Last Update", f"{time_ago.seconds // 3600}h ago")
                    else:
                        st.metric("Last Update", f"{time_ago.seconds // 60}m ago")
                else:
                    st.metric("Last Update", "Never")


def display_real_logs():
    """Display real log files."""
    st.subheader("📝 Real Pipeline Logs")

    logs = get_real_pipeline_logs()

    if not logs:
        st.info("📄 No log files found in common locations")
        return

    # Log selector
    log_options = {f"{log['name']} ({log['location']})": log for log in logs[:10]}
    selected_log_name = st.selectbox("Select log file:", list(log_options.keys()))

    if selected_log_name:
        selected_log = log_options[selected_log_name]

        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption(f"Size: {selected_log['size']} bytes")
        with col2:
            st.caption(
                f"Modified: {selected_log['modified'].strftime('%Y-%m-%d %H:%M')}"
            )
        with col3:
            st.caption(f"Location: {selected_log['location']}")

        # Read and display log content
        try:
            with open(selected_log["path"], "r") as f:
                content = f.read()

            # Limit content size for display
            if len(content) > 10000:
                st.warning("⚠️ Large log file, showing last 10,000 characters")
                content = content[-10000:]

            # Search functionality
            search_term = st.text_input(
                "🔍 Search in logs:", placeholder="Enter search term..."
            )

            if search_term:
                lines = content.split("\n")
                matching_lines = [
                    line for line in lines if search_term.lower() in line.lower()
                ]
                content = "\n".join(matching_lines)
                st.info(f"Found {len(matching_lines)} matching lines")

            # Display content
            st.code(content, language="log")

        except Exception as e:
            st.error(f"Error reading log file: {e}")


def display_docker_status():
    """Display real Docker container status."""
    st.subheader("🐳 Docker Container Status")

    containers = get_docker_container_status()

    if not containers:
        st.warning("⚠️ No Docker containers found or Docker not accessible")
        st.info("Make sure Docker is running: `docker ps`")
        return

    for container in containers:
        with st.container(border=True):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                name = container.get("Names", "Unknown")
                st.markdown(f"**{name}**")
                st.caption(container.get("Image", "Unknown"))

            with col2:
                status = container.get("Status", "Unknown")
                if "Up" in status:
                    st.success(f"✅ {status}")
                else:
                    st.error(f"❌ {status}")

            with col3:
                ports = container.get("Ports", "")
                if ports:
                    st.code(ports, language=None)
                else:
                    st.caption("No ports exposed")

            with col4:
                created = container.get("CreatedAt", "Unknown")
                st.caption(f"Created: {created}")


# Main execution
st.title("📊 Real Pipeline Monitoring")
st.markdown("Live monitoring with **actual data** from your Barcelona Data Pipeline")

# Auto-refresh toggle
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("### 🔄 Live Dashboard")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (30s)")
with col3:
    if st.button("🔄 Refresh Now"):
        st.rerun()

st.divider()

# Real system status
display_real_system_status()
st.divider()

# Real validation history
display_real_validation_history()
st.divider()

# Real data zones
display_real_data_zones()
st.divider()

# Docker status
display_docker_status()
st.divider()

# Real logs
display_real_logs()

# Auto-refresh logic
if auto_refresh:
    import time

    time.sleep(30)
    st.rerun()
