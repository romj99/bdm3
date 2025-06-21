# Airflow Integration Guide for BCN Data Pipeline

This guide explains how to deploy and configure our A2 and A3 Spark pipelines with Apache Airflow orchestration.

## 📋 Prerequisites

### Required Dependencies

```bash
uv sync

# This will inter alia install the following needed dependencies:

# Core Airflow with Spark support
# uv add apache-airflow[postgres,spark]
# uv add apache-airflow-providers-apache-spark
# uv add pyspark
# uv add delta-spark==4.0.0

# Additional dependencies for our pipelines
# uv add polars
# uv add deltalake
```

### System Requirements

- Apache Airflow 2.5+
- Apache Spark 3.4+
- Delta Lake 4.0
- Python 3.8+
- PostgreSQL (recommended for Airflow metadata)

## 🏗️ Directory Structure

Organized our project files as follows:

```
/opt/airflow/
├── dags/
│   ├── bcn_data_pipeline.py          # Main DAG file
│   └── pipelines/
│       ├── __init__.py
│       ├── a2.py                     # our A2 DataFormattingPipeline
│       ├── a3.py                     # our A3 ExploitationPipeline
│       └── utils/
│           └── eda_dashboard.py      # our utility functions
├── data/
│   ├── landing_zone/
│   │   ├── idealista/               # JSON files
│   │   ├── income/                  # CSV files
│   │   └── cultural-sites/          # CSV files
│   ├── formatted_zone/              # Delta tables output from A2
│   └── exploitation_zone/           # Analytics datasets from A3
└── logs/                            # Airflow execution logs
```

## ⚙️ Configuration Steps

### 1. Airflow Variables Setup

Set these variables in the Airflow UI (Admin → Variables):

```python
# Core path configuration
bcn_landing_zone_path = "/opt/airflow/data/landing_zone"
bcn_formatted_zone_path = "/opt/airflow/data/formatted_zone"
bcn_exploitation_zone_path = "/opt/airflow/data/exploitation_zone"

# Notification settings
bcn_notification_email = "your-email@my-email.com"

# Spark configuration (optional)
bcn_spark_master = "local[*]"
bcn_spark_driver_memory = "4g"
bcn_spark_executor_memory = "4g"
```

### 2. Spark Connection Setup

Configure Spark connection in Airflow UI (Admin → Connections):

- **Connection Id**: `spark_default`
- **Connection Type**: `Spark`
- **Host**: `local` (for local mode) or our Spark master URL
- **Extra**:

```json
{
  "spark-home": "/path/to/spark",
  "java-home": "/path/to/java"
}
```

### 3. Email Configuration

Configure SMTP settings in `airflow.cfg`:

```ini
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = your-email@my-email.com
smtp_password = your-app-password
smtp_port = 587
smtp_mail_from = your-email@my-email.com
```

## 🚀 Deployment Process

### 1. Copy Files

```bash
# Copy our pipeline files
cp a2.py /opt/airflow/dags/pipelines/
cp a3.py /opt/airflow/dags/pipelines/
cp utils/eda_dashboard.py /opt/airflow/dags/pipelines/utils/

# Copy the DAG file
cp bcn_data_pipeline.py /opt/airflow/dags/

# Ensure proper permissions
chmod -R 755 /opt/airflow/dags/
```

### 2. Create Data Directories

```bash
mkdir -p /opt/airflow/data/{landing_zone,formatted_zone,exploitation_zone}
mkdir -p /opt/airflow/data/landing_zone/{idealista,income,cultural-sites}

# Copy our raw data
cp -r your_data/idealista/* /opt/airflow/data/landing_zone/idealista/
cp -r your_data/income/* /opt/airflow/data/landing_zone/income/
cp -r your_data/cultural-sites/* /opt/airflow/data/landing_zone/cultural-sites/
```

### 3. Start Airflow Services

```bash
# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@company.com

# Start webserver and scheduler
airflow webserver -p 8080 &
airflow scheduler &
```

## 📊 Monitoring and Operations

### DAG Execution

1. Access Airflow UI at `http://localhost:8080`
2. Find the `bcn_data_pipeline` DAG
3. Toggle it ON to enable scheduling
4. Trigger manual run for testing

### Task Monitoring

- **Task Logs**: View detailed logs for each task
- **XCom**: Check inter-task data exchange
- **Gantt Chart**: Visualize task execution timeline
- **Task Duration**: Monitor performance metrics

### Error Handling

The DAG includes:

- **Automatic retries**: 2 retries with 5-minute delays
- **Email notifications**: On failure and success
- **Data validation**: At each pipeline stage
- **Graceful error handling**: With detailed logging

## 🔍 Validation and Testing

### Test Individual Tasks

```python
# Test in Airflow Python environment
from pipelines.a2 import DataFormattingPipeline

# Test A2 pipeline
pipeline = DataFormattingPipeline(
    landing_zone_path="/opt/airflow/data/landing_zone",
    formatted_zone_path="/opt/airflow/data/formatted_zone"
)
results = pipeline.run_pipeline()
print(f"A2 Results: {results}")
```

### Data Quality Checks

The pipeline includes automated validation:

- Landing zone data availability
- Record count thresholds
- Data type validation
- Delta table integrity

### Performance Optimization

- **Spark Configuration**: Tune memory and parallelism
- **Delta Table Optimization**: Use Z-ordering and vacuum
- **Airflow Parallelism**: Configure `max_active_tasks`
- **Resource Monitoring**: Track CPU, memory, and I/O

## 📈 Production Considerations

### Scaling

- **Spark Cluster**: Deploy on YARN/Kubernetes for large datasets
- **Airflow Executor**: Use CeleryExecutor for distributed task execution
- **Data Partitioning**: Optimize Delta table partitioning strategy

### Security

- **Secrets Management**: Use Airflow Secrets Backend
- **Network Security**: Configure VPC and security groups
- **Access Control**: Implement RBAC for Airflow users

### Backup and Recovery

- **Data Backup**: Regular snapshots of Delta tables
- **Metadata Backup**: Airflow database backups
- **Disaster Recovery**: Multi-region deployment strategy

## 🎯 Expected Outcomes

### Daily Execution Results

- **Landing Zone**: 3 validated datasets
- **Formatted Zone**: 3 standardized Delta tables (~21k+ records)
- **Exploitation Zone**: 9 analytics datasets ready for reporting
- **Notifications**: Success/failure alerts via email
- **Logs**: Comprehensive execution history

### Business Value

- **Automated Processing**: Eliminates manual pipeline execution
- **Data Quality Assurance**: Built-in validation at each stage
- **Operational Visibility**: Complete pipeline monitoring
- **Scalable Architecture**: Ready for production workloads

This integration transforms our standalone Spark pipelines into a robust, production-ready data processing system with enterprise-grade orchestration capabilities.
