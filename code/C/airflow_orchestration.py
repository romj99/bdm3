"""
Apache Airflow DAG for Barcelona Data Pipeline
Orchestrates A2 (Data Formatting) and A3 (Data Exploitation) tasks and seperate A4 - but only basic validation

This DAG:
1. Validates landing zone data availability
2. Runs A2 data formatting pipeline (raw -> formatted zone)
3. Validates formatted zone data quality
4. Runs A3 data exploitation pipeline (formatted -> exploitation zone)
5. Validates exploitation zone analytics datasets
6. Sends notifications on success/failure

Dependencies: apache-airflow, pyspark, delta-spark
"""

import logging

# Import your pipeline classes (adjust path as needed)
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.email.operators.email import EmailOperator

sys.path.append("/opt/airflow/dags/pipelines")
from a2 import DataFormattingPipeline
from a3 import ExploitationPipeline

# DAG Configuration
DAG_ID = "bcn_data_pipeline"
DESCRIPTION = "Barcelona Data Processing Pipeline - A2 + A3 Integration"

# Default arguments for all tasks
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-team@company.com"],  # Configure your email
}

# DAG instance
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval="@daily",  # Run daily
    catchup=False,
    max_active_runs=1,
    tags=["barcelona", "real-estate", "spark", "delta-lake"],
)

# Configuration from Airflow Variables (set these in Airflow UI)
LANDING_ZONE_PATH = Variable.get("bcn_landing_zone_path", "/data/landing_zone")
FORMATTED_ZONE_PATH = Variable.get("bcn_formatted_zone_path", "/data/formatted_zone")
EXPLOITATION_ZONE_PATH = Variable.get(
    "bcn_exploitation_zone_path", "/data/exploitation_zone"
)
NOTIFICATION_EMAIL = Variable.get("bcn_notification_email", "admin@company.com")


# Helper Functions
def validate_landing_zone(**context):
    """Validate that required datasets exist in landing zone."""
    logging.info("Validating landing zone data availability...")

    landing_path = Path(LANDING_ZONE_PATH)
    required_datasets = ["idealista", "income", "cultural-sites"]

    validation_results = {}

    for dataset in required_datasets:
        dataset_path = landing_path / dataset
        if dataset_path.exists():
            # Count files in dataset directory
            files = list(dataset_path.iterdir())
            file_count = len([f for f in files if f.is_file()])
            validation_results[dataset] = {
                "exists": True,
                "file_count": file_count,
                "path": str(dataset_path),
            }
            logging.info(f"✅ {dataset}: {file_count} files found")
        else:
            validation_results[dataset] = {
                "exists": False,
                "file_count": 0,
                "path": str(dataset_path),
            }
            logging.error(f"❌ {dataset}: Directory not found")

    # Check if all required datasets are available
    missing_datasets = [
        ds for ds, info in validation_results.items() if not info["exists"]
    ]

    if missing_datasets:
        raise ValueError(f"Missing required datasets: {missing_datasets}")

    # Push results to XCom for downstream tasks
    context["task_instance"].xcom_push(
        key="landing_zone_validation", value=validation_results
    )

    logging.info("Landing zone validation completed successfully")
    return validation_results


def run_data_formatting(**context):
    """Execute A2 data formatting pipeline."""
    logging.info("Starting A2 Data Formatting Pipeline...")

    try:
        # Initialize and run the formatting pipeline
        pipeline = DataFormattingPipeline(
            landing_zone_path=LANDING_ZONE_PATH, formatted_zone_path=FORMATTED_ZONE_PATH
        )

        results = pipeline.run_pipeline()

        # Push results to XCom
        context["task_instance"].xcom_push(key="formatting_results", value=results)

        # Log summary
        total_records = sum(
            stats.get("record_count", 0)
            for stats in results.values()
            if isinstance(stats, dict) and "record_count" in stats
        )

        logging.info(
            f"A2 Pipeline completed: {total_records:,} total records processed"
        )
        return results

    except Exception as e:
        logging.error(f"A2 Pipeline failed: {str(e)}")
        raise


def validate_formatted_zone(**context):
    """Validate formatted zone data quality."""
    logging.info("Validating formatted zone data quality...")

    # Get formatting results from previous task
    formatting_results = context["task_instance"].xcom_pull(
        task_ids="run_data_formatting", key="formatting_results"
    )

    validation_results = {}

    for dataset, stats in formatting_results.items():
        if isinstance(stats, dict) and "record_count" in stats:
            record_count = stats["record_count"]

            # Define validation rules
            min_records = {
                "idealista": 10000,  # Expect at least 10k real estate records
                "income": 500,  # Expect at least 500 income records
                "cultural_sites": 400,  # Expect at least 400 cultural sites
            }

            is_valid = record_count >= min_records.get(dataset, 0)

            validation_results[dataset] = {
                "record_count": record_count,
                "min_required": min_records.get(dataset, 0),
                "is_valid": is_valid,
                "status": "PASS" if is_valid else "FAIL",
            }

            if is_valid:
                logging.info(f"✅ {dataset}: {record_count:,} records (PASS)")
            else:
                logging.warning(
                    f"⚠️ {dataset}: {record_count:,} records (below minimum)"
                )
        else:
            validation_results[dataset] = {
                "status": "ERROR",
                "error": stats.get("error", "Unknown error"),
            }
            logging.error(f"❌ {dataset}: Validation failed")

    # Check if any critical validations failed
    failed_datasets = [
        ds for ds, info in validation_results.items() if info.get("status") == "FAIL"
    ]

    if failed_datasets:
        raise ValueError(f"Data quality validation failed for: {failed_datasets}")

    context["task_instance"].xcom_push(
        key="formatted_zone_validation", value=validation_results
    )

    return validation_results


def run_data_exploitation(**context):
    """Execute A3 data exploitation pipeline."""
    logging.info("Starting A3 Data Exploitation Pipeline...")

    try:
        # Initialize and run the exploitation pipeline
        pipeline = ExploitationPipeline(
            formatted_zone_path=FORMATTED_ZONE_PATH,
            exploitation_zone_path=EXPLOITATION_ZONE_PATH,
        )

        results = pipeline.run_pipeline()

        # Push results to XCom
        context["task_instance"].xcom_push(key="exploitation_results", value=results)

        # Log summary
        total_datasets = len(
            [
                stats
                for stats in results.values()
                if isinstance(stats, dict) and "record_count" in stats
            ]
        )

        total_records = sum(
            stats.get("record_count", 0)
            for stats in results.values()
            if isinstance(stats, dict) and "record_count" in stats
        )

        logging.info(
            f"A3 Pipeline completed: {total_datasets} analytics datasets, {total_records:,} total records"
        )
        return results

    except Exception as e:
        logging.error(f"A3 Pipeline failed: {str(e)}")
        raise


def validate_exploitation_zone(**context):
    """Validate exploitation zone analytics datasets."""
    logging.info("Validating exploitation zone analytics datasets...")

    # Get exploitation results from previous task
    exploitation_results = context["task_instance"].xcom_pull(
        task_ids="run_data_exploitation", key="exploitation_results"
    )

    # Expected analytics datasets
    expected_datasets = [
        "property_analytics",
        "socioeconomic_district_analytics",
        "cultural_district_analytics",
        "integrated_analytics",
    ]

    validation_results = {}

    for dataset in expected_datasets:
        if dataset in exploitation_results:
            stats = exploitation_results[dataset]
            if isinstance(stats, dict) and "record_count" in stats:
                record_count = stats["record_count"]
                is_valid = record_count > 0

                validation_results[dataset] = {
                    "record_count": record_count,
                    "is_valid": is_valid,
                    "status": "PASS" if is_valid else "FAIL",
                }

                logging.info(f"✅ {dataset}: {record_count:,} records")
            else:
                validation_results[dataset] = {
                    "status": "ERROR",
                    "error": stats.get("error", "Unknown error"),
                }
        else:
            validation_results[dataset] = {
                "status": "MISSING",
                "error": "Dataset not found in results",
            }

    # Check for any failures
    failed_datasets = [
        ds
        for ds, info in validation_results.items()
        if info.get("status") in ["FAIL", "ERROR", "MISSING"]
    ]

    if failed_datasets:
        raise ValueError(f"Analytics validation failed for: {failed_datasets}")

    context["task_instance"].xcom_push(
        key="exploitation_validation", value=validation_results
    )

    return validation_results


def generate_pipeline_report(**context):
    """Generate comprehensive pipeline execution report."""
    logging.info("Generating pipeline execution report...")

    # Collect results from all previous tasks
    landing_validation = context["task_instance"].xcom_pull(
        task_ids="validate_landing_zone", key="landing_zone_validation"
    )

    formatting_results = context["task_instance"].xcom_pull(
        task_ids="run_data_formatting", key="formatting_results"
    )

    exploitation_results = context["task_instance"].xcom_pull(
        task_ids="run_data_exploitation", key="exploitation_results"
    )

    # Generate report
    report = {
        "execution_date": context["ds"],
        "dag_run_id": context["dag_run"].run_id,
        "total_duration_minutes": None,  # Calculate if needed
        "landing_zone_datasets": len(landing_validation),
        "formatted_datasets": len(formatting_results),
        "analytics_datasets": len(exploitation_results),
        "total_records_processed": sum(
            stats.get("record_count", 0)
            for stats in formatting_results.values()
            if isinstance(stats, dict) and "record_count" in stats
        ),
        "status": "SUCCESS",
    }

    # Push report to XCom
    context["task_instance"].xcom_push(key="pipeline_report", value=report)

    logging.info(f"Pipeline Report: {report}")
    return report


# Task Definitions

# 1. Landing Zone Validation
validate_landing = PythonOperator(
    task_id="validate_landing_zone",
    python_callable=validate_landing_zone,
    dag=dag,
)

# 2. Data Formatting Pipeline (A2)
format_data = PythonOperator(
    task_id="run_data_formatting",
    python_callable=run_data_formatting,
    dag=dag,
    # Increase timeout for Spark jobs
    execution_timeout=timedelta(hours=2),
)

# 3. Formatted Zone Validation
validate_formatted = PythonOperator(
    task_id="validate_formatted_zone",
    python_callable=validate_formatted_zone,
    dag=dag,
)

# 4. Data Exploitation Pipeline (A3)
exploit_data = PythonOperator(
    task_id="run_data_exploitation",
    python_callable=run_data_exploitation,
    dag=dag,
    # Increase timeout for Spark jobs
    execution_timeout=timedelta(hours=2),
)

# 5. Exploitation Zone Validation
validate_exploitation = PythonOperator(
    task_id="validate_exploitation_zone",
    python_callable=validate_exploitation_zone,
    dag=dag,
)

# 6. Generate Report
generate_report = PythonOperator(
    task_id="generate_pipeline_report",
    python_callable=generate_pipeline_report,
    dag=dag,
)

# 7. Success Notification
success_notification = EmailOperator(
    task_id="send_success_notification",
    to=[NOTIFICATION_EMAIL],
    subject="✅ BCN Data Pipeline - Execution Successful",
    html_content="""
    <h3>Barcelona Data Pipeline Execution Completed Successfully</h3>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>DAG Run ID:</strong> {{ dag_run.run_id }}</p>
    <p>All data processing stages completed without errors.</p>
    <p>Check the exploitation zone for updated analytics datasets.</p>
    """,
    dag=dag,
)

# 8. Start and End markers
start_pipeline = DummyOperator(
    task_id="start_pipeline",
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id="end_pipeline",
    dag=dag,
)

# Task Dependencies - Linear Pipeline
(
    start_pipeline
    >> validate_landing
    >> format_data
    >> validate_formatted
    >> exploit_data
    >> validate_exploitation
    >> generate_report
    >> success_notification
    >> end_pipeline
)

# Optional: Parallel validation tasks (alternative approach)
# Could also group related validations together:
"""
Alternative dependency structure with task groups:

with TaskGroup('data_formatting_group', dag=dag) as formatting_group:
    format_data
    validate_formatted

with TaskGroup('data_exploitation_group', dag=dag) as exploitation_group:
    exploit_data
    validate_exploitation

start_pipeline >> validate_landing >> formatting_group >> exploitation_group >> generate_report >> success_notification >> end_pipeline
"""
