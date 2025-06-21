"""
A.2 Data Formatting Pipeline - Lab 3: Spark
Location: /code/A/a2.py
Data Source: landing_zone/{source}

PIPELINE OVERVIEW:
1. Initialize Spark session with Delta Lake support
2. Read raw data from Landing Zone (3 datasets)
3. Apply standardization transformations
4. Write data as Delta tables with appropriate partitioning
5. Validate data integrity

DATASETS PROCESSED:
- Idealista: Real estate data (JSON) - 21,389 rows × 37 columns
- Income: Socioeconomic data (CSV) - 811 rows × 7 columns
- Cultural Sites: Cultural facilities (CSV) - 871 rows × 32 columns
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    from_utc_timestamp,
    lit,
    when,
)
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType


class DataFormattingPipeline:
    """
    Simplified Data Formatting Pipeline for A.2 task.

    Processes the three selected datasets (Idealista, Income, Cultural Sites)
    and converts them to standardized Delta tables in the Formatted Zone.
    """

    def __init__(self, landing_zone_path: str, formatted_zone_path: str) -> None:
        """Initialize the pipeline with source and target paths."""
        self.landing_zone_path = Path(landing_zone_path)
        self.formatted_zone_path = Path(formatted_zone_path)
        self.spark: SparkSession = None
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the pipeline."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("data_formatting.log"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        return logging.getLogger(__name__)

    def initialize_spark(self) -> None:
        """Initialize Spark session with Delta Lake 4.0 support."""
        try:
            builder = (
                SparkSession.builder.appName("BCN_DataFormattingPipeline")
                .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )

            # Create session directly without configure_spark_with_delta_pip for 4.0
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session initialized with Delta Lake 4.0 support")

        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {str(e)}")
            raise

    def create_formatted_zone(self) -> None:
        """Create the Formatted Zone directory structure."""
        self.formatted_zone_path.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Formatted Zone created at: {self.formatted_zone_path}")

    def process_idealista_data(self) -> None:
        """
        Process Idealista dataset (JSON files).
        Standardizes real estate property data.
        """
        self.logger.info("Processing Idealista dataset...")

        # Read all JSON files from idealista directory (without wildcard)
        idealista_path = self.landing_zone_path / "idealista"
        df = self.spark.read.option("multiline", "true").json(str(idealista_path))

        initial_count = df.count()
        self.logger.info(f"Idealista - Read {initial_count} records")

        # Select and standardize key columns
        df_formatted = df.select(
            # Identifiers
            col("propertyCode").cast(StringType()).alias("property_code"),
            col("url").cast(StringType()).alias("property_url"),
            # Location
            col("district").cast(StringType()).alias("district"),
            col("neighborhood").cast(StringType()).alias("neighborhood"),
            col("address").cast(StringType()).alias("address"),
            col("municipality").cast(StringType()).alias("municipality"),
            coalesce(col("latitude"), lit(0.0)).cast(DoubleType()).alias("latitude"),
            coalesce(col("longitude"), lit(0.0)).cast(DoubleType()).alias("longitude"),
            # Property details
            col("propertyType").cast(StringType()).alias("property_type"),
            col("detailedType").cast(StringType()).alias("detailed_type"),
            coalesce(col("size"), lit(0)).cast(IntegerType()).alias("size_m2"),
            coalesce(col("rooms"), lit(0)).cast(IntegerType()).alias("rooms"),
            coalesce(col("bathrooms"), lit(0)).cast(IntegerType()).alias("bathrooms"),
            col("floor").cast(StringType()).alias("floor"),
            col("status").cast(StringType()).alias("status"),
            # Financial
            coalesce(col("price"), lit(0.0)).cast(DoubleType()).alias("price_eur"),
            coalesce(col("priceByArea"), lit(0.0))
            .cast(DoubleType())
            .alias("price_per_m2"),
            # Features (handle complex struct types)
            coalesce(col("exterior"), lit(False))
            .cast(BooleanType())
            .alias("is_exterior"),
            coalesce(col("hasLift"), lit(False)).cast(BooleanType()).alias("has_lift"),
            coalesce(col("parkingSpace.hasParkingSpace"), lit(False))
            .cast(BooleanType())
            .alias("has_parking"),
            # Metadata
            from_utc_timestamp(current_timestamp(), "UTC").alias("load_timestamp"),
            lit("idealista").alias("source_dataset"),
        )

        # Apply data quality filters
        df_clean = df_formatted.filter(
            (col("price_eur") > 0)
            & (col("size_m2") > 0)
            & (col("district").isNotNull())
        )

        # Write as Delta table partitioned by district
        output_path = str(self.formatted_zone_path / "idealista")
        df_clean.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("district").save(output_path)

        final_count = df_clean.count()
        self.logger.info(
            f"Idealista - Processed {final_count} records to {output_path}"
        )

    def process_income_data(self) -> None:
        """
        Process Income dataset (CSV files).
        Standardizes socioeconomic income data.
        """
        self.logger.info("Processing Income dataset...")

        # Read all CSV files from income directory
        income_path = self.landing_zone_path / "income"
        df = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(income_path))
        )

        initial_count = df.count()
        self.logger.info(f"Income - Read {initial_count} records")

        # STEP 1: Clean the data first by replacing "-" with empty strings
        from pyspark.sql.functions import regexp_replace

        df_clean = df
        for col_name in [
            "Any",
            "Codi_Districte",
            "Codi_Barri",
            "Població",
            "Índex RFD Barcelona = 100",
        ]:
            df_clean = df_clean.withColumn(
                col_name, regexp_replace(col(col_name), "^-$", "")
            )

        # STEP 2: Now apply transformations (checking for empty strings instead of dashes)
        df_formatted = df_clean.select(
            # Temporal
            when((col("Any") == "") | col("Any").isNull(), lit(None))
            .otherwise(col("Any"))
            .cast(IntegerType())
            .alias("year"),
            # Geographic identifiers
            when(
                (col("Codi_Districte") == "") | col("Codi_Districte").isNull(),
                lit(None),
            )
            .otherwise(col("Codi_Districte"))
            .cast(IntegerType())
            .alias("district_code"),
            col("Nom_Districte").cast(StringType()).alias("district_name"),
            when((col("Codi_Barri") == "") | col("Codi_Barri").isNull(), lit(None))
            .otherwise(col("Codi_Barri"))
            .cast(IntegerType())
            .alias("neighborhood_code"),
            col("Nom_Barri").cast(StringType()).alias("neighborhood_name"),
            # Economic indicators
            when((col("Població") == "") | col("Població").isNull(), lit(None))
            .otherwise(col("Població"))
            .cast(DoubleType())
            .alias("population"),
            when(
                (col("`Índex RFD Barcelona = 100`") == "")
                | col("`Índex RFD Barcelona = 100`").isNull(),
                lit(None),
            )
            .otherwise(col("`Índex RFD Barcelona = 100`"))
            .cast(DoubleType())
            .alias("income_index_bcn_100"),
            # Metadata
            from_utc_timestamp(current_timestamp(), "UTC").alias("load_timestamp"),
            lit("income").alias("source_dataset"),
        )

        # Write as Delta table partitioned by year
        output_path = str(self.formatted_zone_path / "income")
        df_formatted.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("year").save(output_path)

        final_count = df_formatted.count()
        self.logger.info(f"Income - Processed {final_count} records to {output_path}")

    def process_cultural_sites_data(self) -> None:
        """
        Process Cultural Sites dataset (CSV files).
        Standardizes cultural facility data.
        """
        self.logger.info("Processing Cultural Sites dataset...")

        # Read all CSV files from cultural-sites directory
        cultural_path = self.landing_zone_path / "cultural-sites"
        df = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(cultural_path))
        )

        initial_count = df.count()
        self.logger.info(f"Cultural Sites - Read {initial_count} records")

        # Select and standardize key columns
        df_formatted = df.select(
            # Identifiers
            col("_id").cast(StringType()).alias("site_id"),
            col("name").cast(StringType()).alias("facility_name"),
            col("institution_name").cast(StringType()).alias("institution_name"),
            col("addresses_district_id").cast(IntegerType()).alias("district_code"),
            # Location
            col("addresses_district_name").cast(StringType()).alias("district"),
            col("addresses_neighborhood_name").cast(StringType()).alias("neighborhood"),
            coalesce(col("geo_epgs_4326_y"), lit(0.0))
            .cast(DoubleType())
            .alias("latitude"),
            coalesce(col("geo_epgs_4326_x"), lit(0.0))
            .cast(DoubleType())
            .alias("longitude"),
            # Classification
            col("values_category").cast(StringType()).alias("category"),
            col("values_value").cast(StringType()).alias("facility_type"),
            # Metadata
            from_utc_timestamp(current_timestamp(), "UTC").alias("load_timestamp"),
            lit("cultural_sites").alias("source_dataset"),
        )

        # Filter for valid records
        df_clean = df_formatted.filter(
            (col("site_id").isNotNull())
            & (col("facility_name").isNotNull())
            & (col("district").isNotNull())
        )

        # Write as Delta table partitioned by district
        output_path = str(self.formatted_zone_path / "cultural_sites")
        df_clean.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("district").save(output_path)

        final_count = df_clean.count()
        self.logger.info(
            f"Cultural Sites - Processed {final_count} records to {output_path}"
        )

    def validate_data(self) -> Dict[str, Any]:
        """Validate the processed data and return summary statistics."""
        self.logger.info("Validating processed data...")

        results = {}
        datasets = ["idealista", "income", "cultural_sites"]

        for dataset in datasets:
            try:
                path = str(self.formatted_zone_path / dataset)
                df = self.spark.read.format("delta").load(path)

                record_count = df.count()
                column_count = len(df.columns)

                results[dataset] = {
                    "record_count": record_count,
                    "column_count": column_count,
                    "path": path,
                }

                self.logger.info(
                    f"{dataset.upper()}: {record_count:,} records, {column_count} columns"
                )
                df.show(3, truncate=False)

            except Exception as e:
                self.logger.error(f"Failed to validate {dataset}: {str(e)}")
                results[dataset] = {"error": str(e)}

        return results

    def run_pipeline(self) -> Dict[str, Any]:
        """Execute the complete data formatting pipeline."""
        try:
            self.logger.info("=== Starting Data Formatting Pipeline ===")

            # Initialize Spark
            self.initialize_spark()

            # Create output directory
            self.create_formatted_zone()

            # Process each dataset
            self.process_idealista_data()
            self.process_income_data()
            self.process_cultural_sites_data()

            # Validate results
            results = self.validate_data()

            self.logger.info("=== Pipeline Completed Successfully ===")
            return results

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main() -> int:
    """Main execution function."""
    print("=== Data Formatting Pipeline - A.2 ===")

    # Default paths for the expected structure
    LANDING_ZONE_PATH = (
        input("Enter Landing Zone path (press Enter for 'landing_zone'): ").strip()
        or "landing_zone"
    )

    FORMATTED_ZONE_PATH = (
        input("Enter Formatted Zone path (press Enter for 'formatted_zone'): ").strip()
        or "formatted_zone"
    )

    # Validate landing zone exists
    if not Path(LANDING_ZONE_PATH).exists():
        print(f"❌ Error: Landing Zone path '{LANDING_ZONE_PATH}' does not exist!")
        return 1

    try:
        # Run pipeline
        pipeline = DataFormattingPipeline(LANDING_ZONE_PATH, FORMATTED_ZONE_PATH)
        results = pipeline.run_pipeline()

        # Print summary
        print("\n" + "=" * 50)
        print("PIPELINE SUMMARY")
        print("=" * 50)

        total_records = 0
        for dataset, stats in results.items():
            if "record_count" in stats:
                count = stats["record_count"]
                print(f"✅ {dataset.upper()}: {count:,} records")
                total_records += count
            else:
                print(f"❌ {dataset.upper()}: Processing failed")

        print(f"\n🎯 Total: {total_records:,} records processed")
        print("✅ All datasets converted to Delta format")

        return 0

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)