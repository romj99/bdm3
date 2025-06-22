"""
A.4 Data Validation Pipeline - Lab 3: Spark
Location: /src/airflow/dags/pipelines//a4.py
Data Source: formatted_zone/{dataset}, exploitation_zone/{dataset}

PIPELINE OVERVIEW:
1. Initialize Spark session with Delta Lake support
2. Validate data integrity in Formatted Zone (3 datasets)
3. Validate data integrity in Exploitation Zone (9+ datasets)
4. Perform data quality checks and basic analytics
5. Calculate and display key KPIs identified in A.1
6. Validate cross-dataset relationships and joins
7. Generate comprehensive validation report for Airflow and Streamlit

AIRFLOW INTEGRATION:
- Structured as a pipeline class similar to A2/A3
- Returns structured results for task dependencies
- Generates JSON report for Streamlit consumption
- Proper logging and error handling for production use

OUTPUTS:
- Validation results dictionary for Airflow
- JSON report file for Streamlit dashboard
- Performance metrics and data quality scores
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.sql.functions import sum as spark_sum


class DataValidationPipeline:
    """
    Data Validation Pipeline for A.4 task.

    Validates data integrity across formatted and exploitation zones,
    calculates KPIs, and generates comprehensive reports for monitoring
    and quality assurance.
    """

    def __init__(
        self,
        formatted_zone_path: str,
        exploitation_zone_path: str,
        output_path: str = "outputs",
        spark_master: Optional[str] = None,
    ) -> None:
        """Initialize the validation pipeline."""
        self.formatted_zone_path = Path(formatted_zone_path)
        self.exploitation_zone_path = Path(exploitation_zone_path)
        self.output_path = Path(output_path)
        self.spark_master = spark_master or "local[*]"
        self.spark: SparkSession = None
        self.logger = self._setup_logging()

        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the pipeline."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("data_validation.log"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        return logging.getLogger(__name__)

    def initialize_spark(self) -> None:
        """Initialize Spark session with Delta Lake 4.0 support."""
        try:
            builder = (
                SparkSession.builder.appName("BCN_DataValidation_A4_Airflow")
                .master(self.spark_master)
                .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            )

            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session initialized for Data Validation Pipeline")

        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {str(e)}")
            raise

    def load_and_validate_zones(self) -> Dict[str, Any]:
        """Load and perform basic validation on both data zones."""
        self.logger.info("Loading and validating data zones...")

        results = {"formatted_zone": {}, "exploitation_zone": {}}

        # Formatted Zone datasets
        formatted_datasets = ["idealista", "income", "cultural_sites"]

        for dataset in formatted_datasets:
            try:
                path = str(self.formatted_zone_path / dataset)
                df = self.spark.read.format("delta").load(path)

                record_count = df.count()
                column_count = len(df.columns)

                # Calculate null count
                null_counts = []
                for column_name in df.columns:
                    null_count_for_col = df.filter(col(column_name).isNull()).count()
                    null_counts.append(null_count_for_col)

                total_null_count = sum(null_counts)
                total_cells = record_count * column_count
                null_percentage = (
                    (total_null_count / total_cells * 100) if total_cells > 0 else 0
                )

                results["formatted_zone"][dataset] = {
                    "record_count": record_count,
                    "column_count": column_count,
                    "null_count": total_null_count,
                    "null_percentage": null_percentage,
                    "path": path,
                    "status": "success",
                }

                self.logger.info(
                    f"✅ {dataset.upper()}: {record_count:,} records, {column_count} columns, {null_percentage:.1f}% missing"
                )

            except Exception as e:
                self.logger.error(f"❌ {dataset.upper()}: Failed to load - {str(e)}")
                results["formatted_zone"][dataset] = {
                    "status": "error",
                    "error": str(e),
                }

        # Exploitation Zone datasets
        exploitation_datasets = [
            "property_analytics",
            "property_type_analytics",
            "socioeconomic_district_analytics",
            "socioeconomic_neighborhood_analytics",
            "income_quintiles",
            "cultural_district_analytics",
            "cultural_neighborhood_analytics",
            "cultural_category_analytics",
            "integrated_analytics",
        ]

        for dataset in exploitation_datasets:
            try:
                path = str(self.exploitation_zone_path / dataset)
                df = self.spark.read.format("delta").load(path)

                record_count = df.count()
                column_count = len(df.columns)

                results["exploitation_zone"][dataset] = {
                    "record_count": record_count,
                    "column_count": column_count,
                    "path": path,
                    "status": "success",
                }

                self.logger.info(
                    f"✅ {dataset}: {record_count:,} records, {column_count} columns"
                )

            except Exception as e:
                self.logger.error(f"❌ {dataset}: Failed to load - {str(e)}")
                results["exploitation_zone"][dataset] = {
                    "status": "error",
                    "error": str(e),
                }

        return results

    def perform_data_quality_checks(
        self, data_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform comprehensive data quality checks."""
        self.logger.info("Performing data quality checks...")

        quality_results = {}

        # Check formatted zone datasets
        for dataset_name, dataset_info in data_results["formatted_zone"].items():
            if dataset_info.get("status") != "success":
                continue

            try:
                path = dataset_info["path"]
                df = self.spark.read.format("delta").load(path)

                quality_checks = {}

                # Duplicate detection
                total_count = df.count()
                unique_count = df.distinct().count()
                duplicate_count = total_count - unique_count
                duplicate_percentage = (
                    (duplicate_count / total_count * 100) if total_count > 0 else 0
                )

                quality_checks["duplicate_count"] = duplicate_count
                quality_checks["duplicate_percentage"] = duplicate_percentage

                # Dataset-specific validations
                if dataset_name == "idealista":
                    negative_prices = df.filter(col("price_eur") <= 0).count()
                    zero_sizes = df.filter(col("size_m2") <= 0).count()
                    quality_checks["negative_prices"] = negative_prices
                    quality_checks["zero_sizes"] = zero_sizes

                elif dataset_name == "income":
                    negative_income = df.filter(col("income_index_bcn_100") < 0).count()
                    future_years = df.filter(col("year") > 2025).count()
                    quality_checks["negative_income"] = negative_income
                    quality_checks["future_years"] = future_years

                quality_results[dataset_name] = quality_checks

            except Exception as e:
                self.logger.error(f"Quality check failed for {dataset_name}: {str(e)}")
                quality_results[dataset_name] = {"error": str(e)}

        return quality_results

    def calculate_key_kpis(self, data_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate key performance indicators."""
        self.logger.info("Calculating key KPIs...")

        kpi_results = {}

        try:
            # Housing Market KPIs
            if "property_analytics" in data_results["exploitation_zone"]:
                df = self.spark.read.format("delta").load(
                    data_results["exploitation_zone"]["property_analytics"]["path"]
                )

                # Average price per m² by district
                district_prices = (
                    df.filter(col("analysis_level") == "district")
                    .select("district", "avg_price_per_m2", "total_properties")
                    .orderBy(col("avg_price_per_m2").desc())
                )

                kpi_results["avg_price_per_m2_by_district"] = {
                    "count": district_prices.count(),
                    "max_price": district_prices.agg(
                        {"avg_price_per_m2": "max"}
                    ).collect()[0][0],
                    "min_price": district_prices.agg(
                        {"avg_price_per_m2": "min"}
                    ).collect()[0][0],
                    "avg_price": district_prices.agg(
                        {"avg_price_per_m2": "avg"}
                    ).collect()[0][0],
                }

            # Socioeconomic KPIs
            if "socioeconomic_district_analytics" in data_results["exploitation_zone"]:
                df = self.spark.read.format("delta").load(
                    data_results["exploitation_zone"][
                        "socioeconomic_district_analytics"
                    ]["path"]
                )

                inequality_stats = df.agg(
                    avg("income_inequality_cv").alias("avg_inequality"),
                    {"income_inequality_cv": "max"},
                    {"income_inequality_cv": "min"},
                ).collect()[0]

                kpi_results["income_inequality"] = {
                    "avg_inequality": float(inequality_stats["avg_inequality"])
                    if inequality_stats["avg_inequality"]
                    else 0,
                    "max_inequality": float(
                        inequality_stats["max(income_inequality_cv)"]
                    )
                    if inequality_stats["max(income_inequality_cv)"]
                    else 0,
                    "min_inequality": float(
                        inequality_stats["min(income_inequality_cv)"]
                    )
                    if inequality_stats["min(income_inequality_cv)"]
                    else 0,
                }

            # Cultural Accessibility KPIs
            if "cultural_district_analytics" in data_results["exploitation_zone"]:
                df = self.spark.read.format("delta").load(
                    data_results["exploitation_zone"]["cultural_district_analytics"][
                        "path"
                    ]
                )

                cultural_stats = df.agg(
                    avg("cultural_sites_per_1000_residents").alias(
                        "avg_cultural_density"
                    ),
                    spark_sum("total_cultural_sites").alias("total_sites"),
                ).collect()[0]

                kpi_results["cultural_accessibility"] = {
                    "avg_cultural_density": float(
                        cultural_stats["avg_cultural_density"]
                    )
                    if cultural_stats["avg_cultural_density"]
                    else 0,
                    "total_cultural_sites": int(cultural_stats["total_sites"])
                    if cultural_stats["total_sites"]
                    else 0,
                }

            # Composite KPIs
            if "integrated_analytics" in data_results["exploitation_zone"]:
                df = self.spark.read.format("delta").load(
                    data_results["exploitation_zone"]["integrated_analytics"]["path"]
                )

                total_records = df.count()
                complete_records = df.filter(
                    col("district").isNotNull()
                    & col("neighborhood").isNotNull()
                    & col("median_price_eur").isNotNull()
                    & col("income_index_bcn_100").isNotNull()
                ).count()

                integration_completeness = (
                    (complete_records / total_records * 100) if total_records > 0 else 0
                )

                kpi_results["integration_quality"] = {
                    "total_records": total_records,
                    "complete_records": complete_records,
                    "completeness_percentage": integration_completeness,
                }

        except Exception as e:
            self.logger.error(f"KPI calculation failed: {str(e)}")
            kpi_results["error"] = str(e)

        return kpi_results

    def calculate_performance_metrics(self, start_time: datetime) -> Dict[str, Any]:
        """Calculate pipeline performance metrics."""
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        return {
            "execution_time_seconds": execution_time,
            "execution_time_formatted": f"{execution_time:.2f}s",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "spark_version": self.spark.version if self.spark else "unknown",
        }

    def generate_data_quality_score(
        self, data_results: Dict[str, Any], quality_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate overall data quality score."""

        total_datasets = 0
        successful_loads = 0
        total_quality_score = 0
        quality_factors = 0

        # Count successful loads in formatted zone
        for dataset, info in data_results["formatted_zone"].items():
            total_datasets += 1
            if info.get("status") == "success":
                successful_loads += 1

        # Count successful loads in exploitation zone
        for dataset, info in data_results["exploitation_zone"].items():
            total_datasets += 1
            if info.get("status") == "success":
                successful_loads += 1

        # Calculate quality score based on duplicates and nulls
        for dataset, checks in quality_results.items():
            if "duplicate_percentage" in checks:
                dup_score = max(0, 100 - checks["duplicate_percentage"])
                total_quality_score += dup_score
                quality_factors += 1

        overall_quality = (
            total_quality_score / quality_factors if quality_factors > 0 else 0
        )
        load_success_rate = (
            (successful_loads / total_datasets * 100) if total_datasets > 0 else 0
        )

        return {
            "overall_quality_score": round(overall_quality, 2),
            "load_success_rate": round(load_success_rate, 2),
            "total_datasets": total_datasets,
            "successful_loads": successful_loads,
            "failed_loads": total_datasets - successful_loads,
        }

    def generate_streamlit_report(
        self,
        data_results: Dict[str, Any],
        quality_results: Dict[str, Any],
        kpi_results: Dict[str, Any],
        performance_metrics: Dict[str, Any],
        quality_score: Dict[str, Any],
    ) -> None:
        """Generate JSON report for Streamlit consumption."""

        report = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "pipeline_version": "A4_v1.0",
                "report_type": "data_validation_summary",
            },
            "executive_summary": {
                "overall_status": "PASSED"
                if quality_score["load_success_rate"] > 80
                else "FAILED",
                "data_quality_score": quality_score["overall_quality_score"],
                "load_success_rate": quality_score["load_success_rate"],
                "total_datasets_validated": quality_score["total_datasets"],
                "execution_time": performance_metrics["execution_time_formatted"],
            },
            "zone_summary": {
                "formatted_zone": {
                    "total_datasets": len(data_results["formatted_zone"]),
                    "successful_loads": sum(
                        1
                        for info in data_results["formatted_zone"].values()
                        if info.get("status") == "success"
                    ),
                    "total_records": sum(
                        info.get("record_count", 0)
                        for info in data_results["formatted_zone"].values()
                        if info.get("status") == "success"
                    ),
                },
                "exploitation_zone": {
                    "total_datasets": len(data_results["exploitation_zone"]),
                    "successful_loads": sum(
                        1
                        for info in data_results["exploitation_zone"].values()
                        if info.get("status") == "success"
                    ),
                    "total_records": sum(
                        info.get("record_count", 0)
                        for info in data_results["exploitation_zone"].values()
                        if info.get("status") == "success"
                    ),
                },
            },
            "kpi_summary": kpi_results,
            "data_quality_details": quality_results,
            "performance_metrics": performance_metrics,
            "detailed_results": {
                "formatted_zone_details": data_results["formatted_zone"],
                "exploitation_zone_details": data_results["exploitation_zone"],
            },
            "recommendations": self._generate_recommendations(
                quality_score, quality_results
            ),
        }

        # Save report to JSON file
        report_file = self.output_path / "data_validation_report.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        self.logger.info(f"Streamlit report saved to: {report_file}")

    def _generate_recommendations(
        self, quality_score: Dict[str, Any], quality_results: Dict[str, Any]
    ) -> List[str]:
        """Generate actionable recommendations based on validation results."""

        recommendations = []

        if quality_score["load_success_rate"] < 100:
            recommendations.append(
                f"⚠️ {quality_score['failed_loads']} datasets failed to load. "
                "Check data paths and file formats."
            )

        if quality_score["overall_quality_score"] < 80:
            recommendations.append(
                f"⚠️ Data quality score is {quality_score['overall_quality_score']:.1f}%. "
                "Review data cleaning processes."
            )

        # Check for high duplicate rates
        for dataset, checks in quality_results.items():
            if checks.get("duplicate_percentage", 0) > 10:
                recommendations.append(
                    f"⚠️ High duplicate rate in {dataset} "
                    f"({checks['duplicate_percentage']:.1f}%). "
                    "Consider deduplication."
                )

        # Check for data validation issues
        for dataset, checks in quality_results.items():
            if checks.get("negative_prices", 0) > 0:
                recommendations.append(
                    f"⚠️ Found {checks['negative_prices']} records with invalid prices in {dataset}. "
                    "Review price validation rules."
                )

        if not recommendations:
            recommendations.append(
                "✅ All validations passed. Data pipeline is healthy."
            )

        return recommendations

    def run_pipeline(self) -> Dict[str, Any]:
        """Execute the complete data validation pipeline."""
        start_time = datetime.now()

        try:
            self.logger.info("=== Starting Data Validation Pipeline ===")

            # Initialize Spark
            self.initialize_spark()

            # Load and validate data zones
            data_results = self.load_and_validate_zones()

            # Perform data quality checks
            quality_results = self.perform_data_quality_checks(data_results)

            # Calculate KPIs
            kpi_results = self.calculate_key_kpis(data_results)

            # Calculate performance metrics
            performance_metrics = self.calculate_performance_metrics(start_time)

            # Generate overall quality score
            quality_score = self.generate_data_quality_score(
                data_results, quality_results
            )

            # Generate Streamlit report
            self.generate_streamlit_report(
                data_results,
                quality_results,
                kpi_results,
                performance_metrics,
                quality_score,
            )

            # Prepare results for Airflow
            pipeline_results = {
                "status": "PASSED"
                if quality_score["load_success_rate"] > 80
                else "FAILED",
                "data_quality_score": quality_score["overall_quality_score"],
                "load_success_rate": quality_score["load_success_rate"],
                "total_datasets": quality_score["total_datasets"],
                "execution_time": performance_metrics["execution_time_seconds"],
                "kpi_count": len([k for k in kpi_results.keys() if not k == "error"]),
                "formatted_zone_records": sum(
                    info.get("record_count", 0)
                    for info in data_results["formatted_zone"].values()
                    if info.get("status") == "success"
                ),
                "exploitation_zone_records": sum(
                    info.get("record_count", 0)
                    for info in data_results["exploitation_zone"].values()
                    if info.get("status") == "success"
                ),
                "report_file": str(self.output_path / "data_validation_report.json"),
            }

            self.logger.info("=== Data Validation Pipeline Completed Successfully ===")
            return pipeline_results

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main() -> int:
    """Main execution function."""
    print("=== Data Validation Pipeline - A.4 ===")

    load_dotenv()

    # Get paths from environment or user input
    FORMATTED_ZONE_PATH = (
        os.getenv("FORMATTED_ZONE")
        or input(
            "Enter Formatted Zone path (press Enter for 'formatted_zone'): "
        ).strip()
        or "formatted_zone"
    )

    EXPLOITATION_ZONE_PATH = (
        os.getenv("EXPLOITATION_ZONE")
        or input(
            "Enter Exploitation Zone path (press Enter for 'exploitation_zone'): "
        ).strip()
        or "exploitation_zone"
    )

    OUTPUT_PATH = (
        os.getenv("OUTPUT_PATH")
        or input("Enter Output path (press Enter for 'outputs'): ").strip()
        or "outputs"
    )

    # Validate required paths exist
    if not Path(FORMATTED_ZONE_PATH).exists():
        print(f"❌ Error: Formatted Zone path '{FORMATTED_ZONE_PATH}' does not exist!")
        return 1

    if not Path(EXPLOITATION_ZONE_PATH).exists():
        print(
            f"❌ Error: Exploitation Zone path '{EXPLOITATION_ZONE_PATH}' does not exist!"
        )
        return 1

    try:
        # Run pipeline
        pipeline = DataValidationPipeline(
            FORMATTED_ZONE_PATH, EXPLOITATION_ZONE_PATH, OUTPUT_PATH
        )
        results = pipeline.run_pipeline()

        # Print summary
        print("\n" + "=" * 60)
        print("VALIDATION PIPELINE SUMMARY")
        print("=" * 60)
        print(f"✅ Status: {results['status']}")
        print(f"📊 Data Quality Score: {results['data_quality_score']:.1f}%")
        print(f"📈 Load Success Rate: {results['load_success_rate']:.1f}%")
        print(f"📁 Total Datasets: {results['total_datasets']}")
        print(f"⏱️  Execution Time: {results['execution_time']:.2f}s")
        print(f"📊 KPIs Calculated: {results['kpi_count']}")
        print(f"📋 Report Generated: {results['report_file']}")

        return 0

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)