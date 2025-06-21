"""
Apache Airflow 3.0+ Optimized Barcelona Data Pipeline
Orchestrates A2 (Data Formatting) and A3 (Data Exploitation) tasks

This DAG leverages Airflow 3.0 features:
- Task SDK with airflow.sdk imports
- Modern TaskFlow API with decorators
- Asset-based scheduling capabilities
- Enhanced error handling and observability
- Service-oriented architecture support
- DAG versioning compatibility

Dependencies: apache-airflow>=3.0, pyspark, delta-spark
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator

# Airflow 3.0+ imports using the new Task SDK
from airflow.sdk import dag, task
from a2 import DataFormattingPipeline
from a3 import ExploitationPipeline

# DAG Configuration
LANDING_ZONE_PATH = os.getenv("LANDING_ZONE", "/opt/airflow/data/landing_zone")
FORMATTED_ZONE_PATH = os.getenv("FORMATTED_ZONE", "/opt/airflow/data/formatted_zone")
EXPLOITATION_ZONE_PATH = os.getenv(
    "EXPLOITATION_ZONE", "/opt/airflow/data/exploitation_zone"
)
NOTIFICATION_EMAIL = os.getenv("EMAIL", "admin@company.com")


# Define the DAG using Airflow 3.0 @dag decorator
@dag(
    dag_id="bcn_data_pipeline",
    description="Barcelona Data Processing Pipeline - Optimized for Airflow 3.0+",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Use Assets for event-driven scheduling or set schedule as needed
    catchup=False,
    max_active_runs=1,
    tags=["barcelona", "real-estate", "spark", "delta-lake", "airflow-3.0"],
    default_args={
        "owner": "ju-mo",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email": [NOTIFICATION_EMAIL],
    },
)
def bcn_data_pipeline():
    """
    Barcelona Data Processing Pipeline using Airflow 3.0+ features.

    This DAG demonstrates modern Airflow patterns:
    - TaskFlow API with @task decorators
    - Proper error handling and validation
    - Data passing between tasks via return values
    - Comprehensive logging and monitoring
    """

    @task(task_id="validate_landing_zone", retries=0, retry_delay=timedelta(minutes=2))
    def validate_landing_zone() -> Dict[str, Any]:
        """
        Validate that required datasets exist in landing zone.

        Returns:
            Dict containing validation results for each dataset
        """
        logging.info("🔍 Validating landing zone data availability...")

        landing_path = Path(LANDING_ZONE_PATH)
        required_datasets = ["idealista", "income", "cultural-sites"]

        validation_results = {}

        for dataset in required_datasets:
            dataset_path = landing_path / dataset
            if dataset_path.exists():
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
                logging.warning(f"❌ {dataset}: Directory not found")

        # Check if all required datasets are available
        missing_datasets = [
            ds for ds, info in validation_results.items() if not info["exists"]
        ]

        if missing_datasets:
            raise AirflowException(f"Missing required datasets: {missing_datasets}")

        logging.info("✅ Landing zone validation completed successfully")
        return validation_results

    @task(
        task_id="run_data_formatting",
        execution_timeout=timedelta(hours=2),
        retries=0,
        retry_delay=timedelta(minutes=10),
    )
    def run_data_formatting(validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute A2 data formatting pipeline.

        Args:
            validation_results: Results from landing zone validation

        Returns:
            Dict containing formatting pipeline results
        """
        logging.info("🔄 Starting A2 Data Formatting Pipeline...")
        logging.info(f"Validated datasets: {list(validation_results.keys())}")

        try:
            pipeline = DataFormattingPipeline(
                landing_zone_path=LANDING_ZONE_PATH,
                formatted_zone_path=FORMATTED_ZONE_PATH,
            )

            results = pipeline.run_pipeline()

            # Log summary
            total_records = sum(
                stats.get("record_count", 0)
                for stats in results.values()
                if isinstance(stats, dict) and "record_count" in stats
            )

            logging.info(
                f"✅ A2 Pipeline completed: {total_records:,} total records processed"
            )
            return results

        except Exception as e:
            logging.error(f"❌ A2 Pipeline failed: {str(e)}")
            raise AirflowException(f"Data formatting pipeline failed: {str(e)}")

    @task(task_id="validate_formatted_zone", retries=0)
    def validate_formatted_zone(formatting_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate formatted zone data quality.

        Args:
            formatting_results: Results from data formatting pipeline

        Returns:
            Dict containing validation results
        """
        logging.info("🔍 Validating formatted zone data quality...")

        if not formatting_results:
            raise AirflowException("No formatting results found from previous task")

        validation_results = {}

        # Define validation rules
        min_records = {
            "idealista": 10000,  # Expect at least 10k real estate records
            "income": 500,  # Expect at least 500 income records
            "cultural_sites": 400,  # Expect at least 400 cultural sites
        }

        for dataset, stats in formatting_results.items():
            if isinstance(stats, dict) and "record_count" in stats:
                record_count = stats["record_count"]
                min_required = min_records.get(dataset, 0)
                is_valid = record_count >= min_required

                validation_results[dataset] = {
                    "record_count": record_count,
                    "min_required": min_required,
                    "is_valid": is_valid,
                    "status": "PASS" if is_valid else "FAIL",
                }

                if is_valid:
                    logging.info(f"✅ {dataset}: {record_count:,} records (PASS)")
                else:
                    logging.warning(
                        f"⚠️ {dataset}: {record_count:,} records (below minimum {min_required})"
                    )
            else:
                validation_results[dataset] = {
                    "status": "ERROR",
                    "error": stats.get("error", "Unknown error"),
                }
                logging.error(f"❌ {dataset}: Validation failed")

        # Check if any critical validations failed
        failed_datasets = [
            ds
            for ds, info in validation_results.items()
            if info.get("status") == "FAIL"
        ]

        if failed_datasets:
            raise AirflowException(
                f"Data quality validation failed for: {failed_datasets}"
            )

        return validation_results

    @task(
        task_id="run_data_exploitation",
        execution_timeout=timedelta(hours=2),
        retries=0,
        retry_delay=timedelta(minutes=10),
    )
    def run_data_exploitation(validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute A3 data exploitation pipeline.

        Args:
            validation_results: Results from formatted zone validation

        Returns:
            Dict containing exploitation pipeline results
        """
        logging.info("🔄 Starting A3 Data Exploitation Pipeline...")
        logging.info(f"Validated datasets: {list(validation_results.keys())}")

        try:
            pipeline = ExploitationPipeline(
                formatted_zone_path=FORMATTED_ZONE_PATH,
                exploitation_zone_path=EXPLOITATION_ZONE_PATH,
            )

            results = pipeline.run_pipeline()

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
                f"✅ A3 Pipeline completed: {total_datasets} analytics datasets, {total_records:,} total records"
            )
            return results

        except Exception as e:
            logging.error(f"❌ A3 Pipeline failed: {str(e)}")
            raise AirflowException(f"Data exploitation pipeline failed: {str(e)}")

    @task(task_id="validate_exploitation_zone", retries=0)
    def validate_exploitation_zone(
        exploitation_results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate exploitation zone analytics datasets.

        Args:
            exploitation_results: Results from exploitation pipeline

        Returns:
            Dict containing validation results
        """
        logging.info("🔍 Validating exploitation zone analytics datasets...")

        if not exploitation_results:
            raise AirflowException("No exploitation results found from previous task")

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
            raise AirflowException(
                f"Analytics validation failed for: {failed_datasets}"
            )

        return validation_results

    @task(task_id="generate_pipeline_report")
    def generate_pipeline_report(
        landing_validation: Dict[str, Any],
        formatting_results: Dict[str, Any],
        exploitation_results: Dict[str, Any],
        **context,
    ) -> Dict[str, Any]:
        """
        Generate comprehensive pipeline execution report.

        Args:
            landing_validation: Landing zone validation results
            formatting_results: Data formatting results
            exploitation_results: Data exploitation results
            context: Airflow context

        Returns:
            Dict containing comprehensive pipeline report
        """
        logging.info("📊 Generating pipeline execution report...")

        # Generate report
        report = {
            "execution_date": context["ds"],
            "dag_run_id": context["dag_run"].run_id,
            "landing_zone_datasets": len(landing_validation)
            if landing_validation
            else 0,
            "formatted_datasets": len(formatting_results) if formatting_results else 0,
            "analytics_datasets": len(exploitation_results)
            if exploitation_results
            else 0,
            "total_records_processed": sum(
                stats.get("record_count", 0)
                for stats in (formatting_results or {}).values()
                if isinstance(stats, dict) and "record_count" in stats
            ),
            "status": "SUCCESS",
            "timestamp": datetime.now().isoformat(),
        }

        logging.info(f"📈 Pipeline Report: {report}")
        return report

    @task(task_id="send_success_notification")
    def send_success_notification(report: Dict[str, Any]) -> str:
        """
        Send success notification with pipeline summary.

        Args:
            report: Pipeline execution report

        Returns:
            Success message
        """
        logging.info("📧 Sending success notification...")

        message = f"""
        ✅ Barcelona Data Pipeline executed successfully!
        
        📊 Summary:
        - Landing Zone Datasets: {report["landing_zone_datasets"]}
        - Formatted Datasets: {report["formatted_datasets"]} 
        - Analytics Datasets: {report["analytics_datasets"]}
        - Total Records: {report["total_records_processed"]:,}
        - Execution Date: {report["execution_date"]}
        - DAG Run ID: {report["dag_run_id"]}
        
        🎯 All data processing stages completed without errors.
        📁 Check the exploitation zone for updated analytics datasets.
        """

        logging.info(message)
        return "Notification sent successfully"

    # Define task dependencies using TaskFlow API
    # The >> operator works with task objects returned by decorated functions
    landing_validation = validate_landing_zone()
    formatting_results = run_data_formatting(landing_validation)
    formatted_validation = validate_formatted_zone(formatting_results)
    exploitation_results = run_data_exploitation(formatted_validation)
    exploitation_validation = validate_exploitation_zone(exploitation_results)

    # Generate report with all inputs
    report = generate_pipeline_report(
        landing_validation, formatting_results, exploitation_results
    )

    # Send notification
    notification = send_success_notification(report)

    # Final dependency chain
    exploitation_validation >> report >> notification

# Instantiate the DAG
bcn_pipeline_dag = bcn_data_pipeline()

# Optional: Add traditional operators for comparison/integration
with bcn_pipeline_dag:
    # Example of mixing TaskFlow with traditional operators
    cleanup_task = BashOperator(
        task_id="cleanup_temp_files",
        bash_command='echo "Cleaning up temporary files..." && find /tmp -name "bcn_pipeline_*" -delete || true',
        trigger_rule="all_done",  # Run regardless of upstream task success/failure
    )

    # This traditional operator will run after all tasks complete
    cleanup_task

# Log DAG configuration for debugging
logging.info("🏗️ BCN Data Pipeline DAG (Airflow 3.0+) configured:")
logging.info(f"  📂 Landing Zone: {LANDING_ZONE_PATH}")
logging.info(f"  📂 Formatted Zone: {FORMATTED_ZONE_PATH}")
logging.info(f"  📂 Exploitation Zone: {EXPLOITATION_ZONE_PATH}")
logging.info(f"  📧 Notification Email: {NOTIFICATION_EMAIL}")
logging.info("  🆔 DAG ID: bcn_data_pipeline")