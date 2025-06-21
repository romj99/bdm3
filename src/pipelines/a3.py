"""
A.3 Data Exploitation Pipeline - Lab 3: Spark
Location: /code/A/a3.py
Data Source: formatted_zone/{dataset}

PIPELINE OVERVIEW:
1. Initialize Spark session with Delta Lake support
2. Read formatted data from Formatted Zone (3 datasets)
3. Perform analytical transformations and feature engineering
4. Create analytical datasets optimized for KPI calculations
5. Join datasets for cross-analysis capabilities
6. Write data to Exploitation Zone in analysis-ready format

ANALYTICAL TRANSFORMATIONS:
- Property metrics aggregation (price/m², availability by area)
- Income inequality and affordability calculations
- Cultural density and accessibility metrics
- Cross-dataset joins for composite analysis
- Data quality enhancements for analytics

DATASETS CREATED:
- property_analytics: Aggregated real estate metrics by district/neighborhood
- socioeconomic_analytics: Income and demographic analytics
- cultural_analytics: Cultural facility density and distribution
- integrated_analytics: Combined dataset for composite KPIs
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    expr,
    first,
    from_utc_timestamp,
    lit,
    percentile_approx,
    stddev,
    when,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import DoubleType, StringType


class ExploitationPipeline:
    """
    Data Exploitation Pipeline for A.3 task.

    Transforms formatted data into analysis-ready datasets optimized for
    the specific KPIs and analytical objectives identified in A.1.
    """

    def __init__(
        self,
        formatted_zone_path: str,
        exploitation_zone_path: str,
        spark_master: Optional[str] = None,
    ) -> None:
        """Initialize the pipeline with source and target paths."""
        self.formatted_zone_path = Path(formatted_zone_path)
        self.exploitation_zone_path = Path(exploitation_zone_path)
        self.spark_master = spark_master or "local[*]"
        self.spark: SparkSession = None
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the pipeline."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("exploitation_pipeline.log"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        return logging.getLogger(__name__)

    def initialize_spark(self) -> None:
        """Initialize Spark session with Delta Lake 4.0 support."""
        try:
            builder = (
                SparkSession.builder.appName("BCN_ExploitationPipeline_Airflow")
                .master(self.spark_master or "local[*]")
                .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
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
            self.logger.info("Spark session initialized for Exploitation Pipeline")

        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {str(e)}")
            raise

    def create_exploitation_zone(self) -> None:
        """Create the Exploitation Zone directory structure."""
        self.exploitation_zone_path.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Exploitation Zone created at: {self.exploitation_zone_path}")

    def load_formatted_data(self) -> Dict[str, Any]:
        """Load all formatted datasets from the Formatted Zone."""
        self.logger.info("Loading formatted datasets...")

        datasets = {}

        try:
            # Load Idealista data
            idealista_path = str(self.formatted_zone_path / "idealista")
            datasets["idealista"] = self.spark.read.format("delta").load(idealista_path)
            self.logger.info(
                f"Loaded Idealista: {datasets['idealista'].count()} records"
            )

            # Load Income data
            income_path = str(self.formatted_zone_path / "income")
            datasets["income"] = self.spark.read.format("delta").load(income_path)
            self.logger.info(f"Loaded Income: {datasets['income'].count()} records")

            # Load Cultural Sites data
            cultural_path = str(self.formatted_zone_path / "cultural_sites")
            datasets["cultural_sites"] = self.spark.read.format("delta").load(
                cultural_path
            )
            self.logger.info(
                f"Loaded Cultural Sites: {datasets['cultural_sites'].count()} records"
            )

        except Exception as e:
            self.logger.error(f"Failed to load formatted data: {str(e)}")
            raise

        return datasets

    def create_property_analytics(self, idealista_df) -> None:
        """
        Create property analytics dataset optimized for housing market KPIs.

        Aggregates property data by district and neighborhood for:
        - Average price per m² analysis
        - Property type distribution analysis
        - Market supply analysis
        """
        self.logger.info("Creating property analytics dataset...")

        # Clean and filter data for analysis
        property_clean = idealista_df.filter(
            (col("price_eur") > 0)
            & (col("size_m2") > 0)
            & (col("price_per_m2") > 0)
            & (col("price_per_m2") < 20000)  # Remove outliers
            & (col("district").isNotNull())
            & (col("neighborhood").isNotNull())
        )

        # District-level aggregations
        district_analytics = (
            property_clean.groupBy("district")
            .agg(
                count("*").alias("total_properties"),
                avg("price_eur").cast(DoubleType()).alias("avg_price_eur"),
                percentile_approx("price_eur", 0.5)
                .cast(DoubleType())
                .alias("median_price_eur"),
                avg("price_per_m2").cast(DoubleType()).alias("avg_price_per_m2"),
                percentile_approx("price_per_m2", 0.5)
                .cast(DoubleType())
                .alias("median_price_per_m2"),
                avg("size_m2").cast(DoubleType()).alias("avg_size_m2"),
                avg("rooms").cast(DoubleType()).alias("avg_rooms"),
                stddev("price_per_m2").cast(DoubleType()).alias("price_per_m2_stddev"),
                spark_min("price_per_m2").cast(DoubleType()).alias("min_price_per_m2"),
                spark_max("price_per_m2").cast(DoubleType()).alias("max_price_per_m2"),
            )
            .withColumn("analysis_level", lit("district"))
        )

        # Neighborhood-level aggregations
        neighborhood_analytics = (
            property_clean.groupBy("district", "neighborhood")
            .agg(
                count("*").alias("total_properties"),
                avg("price_eur").cast(DoubleType()).alias("avg_price_eur"),
                percentile_approx("price_eur", 0.5)
                .cast(DoubleType())
                .alias("median_price_eur"),
                avg("price_per_m2").cast(DoubleType()).alias("avg_price_per_m2"),
                percentile_approx("price_per_m2", 0.5)
                .cast(DoubleType())
                .alias("median_price_per_m2"),
                avg("size_m2").cast(DoubleType()).alias("avg_size_m2"),
                avg("rooms").cast(DoubleType()).alias("avg_rooms"),
                stddev("price_per_m2").cast(DoubleType()).alias("price_per_m2_stddev"),
                spark_min("price_per_m2").cast(DoubleType()).alias("min_price_per_m2"),
                spark_max("price_per_m2").cast(DoubleType()).alias("max_price_per_m2"),
            )
            .withColumn("analysis_level", lit("neighborhood"))
        )

        # Property type analysis
        property_type_analytics = property_clean.groupBy(
            "district", "property_type"
        ).agg(
            count("*").alias("type_count"),
            avg("price_per_m2").cast(DoubleType()).alias("avg_price_per_m2_by_type"),
            percentile_approx("price_per_m2", 0.5)
            .cast(DoubleType())
            .alias("median_price_per_m2_by_type"),
        )

        # Add metadata and round numerical values
        district_final = district_analytics.select(
            col("district"),
            lit(None).cast(StringType()).alias("neighborhood"),
            col("analysis_level"),
            col("total_properties"),
            spark_round("avg_price_eur", 2).alias("avg_price_eur"),
            spark_round("median_price_eur", 2).alias("median_price_eur"),
            spark_round("avg_price_per_m2", 2).alias("avg_price_per_m2"),
            spark_round("median_price_per_m2", 2).alias("median_price_per_m2"),
            spark_round("avg_size_m2", 1).alias("avg_size_m2"),
            spark_round("avg_rooms", 1).alias("avg_rooms"),
            spark_round("price_per_m2_stddev", 2).alias("price_per_m2_stddev"),
            col("min_price_per_m2"),
            col("max_price_per_m2"),
            from_utc_timestamp(current_timestamp(), "UTC").alias("created_at"),
        )

        neighborhood_final = neighborhood_analytics.select(
            col("district"),
            col("neighborhood"),
            col("analysis_level"),
            col("total_properties"),
            spark_round("avg_price_eur", 2).alias("avg_price_eur"),
            spark_round("median_price_eur", 2).alias("median_price_eur"),
            spark_round("avg_price_per_m2", 2).alias("avg_price_per_m2"),
            spark_round("median_price_per_m2", 2).alias("median_price_per_m2"),
            spark_round("avg_size_m2", 1).alias("avg_size_m2"),
            spark_round("avg_rooms", 1).alias("avg_rooms"),
            spark_round("price_per_m2_stddev", 2).alias("price_per_m2_stddev"),
            col("min_price_per_m2"),
            col("max_price_per_m2"),
            from_utc_timestamp(current_timestamp(), "UTC").alias("created_at"),
        )

        # Combine district and neighborhood analytics
        property_analytics_combined = district_final.union(neighborhood_final)

        # Write property analytics
        output_path = str(self.exploitation_zone_path / "property_analytics")
        property_analytics_combined.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("analysis_level").save(output_path)

        # Write property type analytics separately
        property_type_path = str(
            self.exploitation_zone_path / "property_type_analytics"
        )
        property_type_analytics.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(property_type_path)

        record_count = property_analytics_combined.count()
        type_count = property_type_analytics.count()
        self.logger.info(f"Property Analytics: {record_count} records created")
        self.logger.info(f"Property Type Analytics: {type_count} records created")

    def create_socioeconomic_analytics(self, income_df) -> None:
        """
        Create socioeconomic analytics dataset for income and demographic KPIs.

        Processes income data for:
        - Income inequality analysis
        - Economic accessibility scoring
        - Demographic trend analysis
        """
        self.logger.info("Creating socioeconomic analytics dataset...")

        # Filter valid income data
        income_clean = income_df.filter(
            (col("income_index_bcn_100").isNotNull())
            & (col("income_index_bcn_100") > 0)
            & (col("district_name").isNotNull())
            & (col("neighborhood_name").isNotNull())
        )

        # Latest year analysis (most recent data per district/neighborhood)
        latest_year_df = income_clean.groupBy("district_name", "neighborhood_name").agg(
            spark_max("year").alias("latest_year")
        )

        # Join to get latest income data - using proper aliases
        latest_income = (
            income_clean.alias("inc")
            .join(
                latest_year_df.alias("latest"),
                (col("inc.district_name") == col("latest.district_name"))
                & (col("inc.neighborhood_name") == col("latest.neighborhood_name"))
                & (col("inc.year") == col("latest.latest_year")),
            )
            .select(col("inc.*"), col("latest.latest_year"))
        )

        # District-level socioeconomic analytics
        district_socio = (
            latest_income.groupBy("district_name")
            .agg(
                first("district_code").alias("district_code"),
                count("*").alias("neighborhood_count"),
                avg("income_index_bcn_100")
                .cast(DoubleType())
                .alias("avg_income_index"),
                percentile_approx("income_index_bcn_100", 0.5)
                .cast(DoubleType())
                .alias("median_income_index"),
                stddev("income_index_bcn_100")
                .cast(DoubleType())
                .alias("income_inequality_stddev"),
                spark_min("income_index_bcn_100")
                .cast(DoubleType())
                .alias("min_income_index"),
                spark_max("income_index_bcn_100")
                .cast(DoubleType())
                .alias("max_income_index"),
                spark_sum("population").cast(DoubleType()).alias("total_population"),
                avg("population")
                .cast(DoubleType())
                .alias("avg_neighborhood_population"),
            )
            .withColumn(
                # Calculate coefficient of variation as inequality measure
                "income_inequality_cv",
                spark_round(
                    (col("income_inequality_stddev") / col("avg_income_index")) * 100, 2
                ),
            )
            .withColumn("analysis_level", lit("district"))
        )

        # Neighborhood-level socioeconomic analytics
        neighborhood_socio = latest_income.select(
            col("district_name"),
            col("neighborhood_name"),
            col("district_code"),
            col("neighborhood_code"),
            col("income_index_bcn_100"),
            col("population"),
            col("latest_year").alias("data_year"),
        ).withColumn("analysis_level", lit("neighborhood"))

        # Income quintile classification
        income_quintiles = latest_income.select(
            col("district_name"), col("neighborhood_name"), col("income_index_bcn_100")
        ).withColumn(
            "income_quintile",
            when(col("income_index_bcn_100") <= 70, "Q1_Lowest")
            .when(col("income_index_bcn_100") <= 85, "Q2_Low")
            .when(col("income_index_bcn_100") <= 100, "Q3_Medium")
            .when(col("income_index_bcn_100") <= 120, "Q4_High")
            .otherwise("Q5_Highest"),
        )

        # Write socioeconomic analytics
        district_output_path = str(
            self.exploitation_zone_path / "socioeconomic_district_analytics"
        )
        district_socio.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(district_output_path)

        neighborhood_output_path = str(
            self.exploitation_zone_path / "socioeconomic_neighborhood_analytics"
        )
        neighborhood_socio.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(neighborhood_output_path)

        quintiles_output_path = str(self.exploitation_zone_path / "income_quintiles")
        income_quintiles.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(quintiles_output_path)

        district_count = district_socio.count()
        neighborhood_count = neighborhood_socio.count()
        self.logger.info(f"Socioeconomic District Analytics: {district_count} records")
        self.logger.info(
            f"Socioeconomic Neighborhood Analytics: {neighborhood_count} records"
        )

    def create_cultural_analytics(self, cultural_df, income_df) -> None:
        """
        Create cultural analytics dataset for cultural accessibility KPIs.

        Processes cultural sites data for:
        - Cultural density by district/neighborhood
        - Cultural facility type distribution
        - Cultural-economic correlation analysis
        """
        self.logger.info("Creating cultural analytics dataset...")

        # Clean cultural sites data
        cultural_clean = cultural_df.filter(
            (col("site_id").isNotNull())
            & (col("district").isNotNull())
            & (col("facility_name").isNotNull())
        )

        # Get latest population data for density calculations - with proper aliases
        latest_year_pop = (
            income_df.filter(col("income_index_bcn_100").isNotNull())
            .groupBy("district_name", "neighborhood_name")
            .agg(spark_max("year").alias("latest_year"))
        )

        latest_population = (
            latest_year_pop.alias("pop")
            .join(
                income_df.alias("inc"),
                (col("inc.district_name") == col("pop.district_name"))
                & (col("inc.neighborhood_name") == col("pop.neighborhood_name"))
                & (col("inc.year") == col("pop.latest_year")),
            )
            .select(
                col("inc.district_name").alias("district"),
                col("inc.neighborhood_name").alias("neighborhood"),
                col("inc.population"),
            )
        )

        # District-level cultural analytics
        district_cultural = cultural_clean.groupBy("district").agg(
            count("*").alias("total_cultural_sites"),
            expr("size(collect_set(category))").alias("unique_categories"),
            expr("size(collect_set(facility_type))").alias("unique_facility_types"),
        )

        # Add population data for density calculation
        district_population = latest_population.groupBy("district").agg(
            spark_sum("population").alias("total_population")
        )

        district_cultural_final = (
            district_cultural.alias("cult")
            .join(
                district_population.alias("pop"),
                col("cult.district") == col("pop.district"),
                "left",
            )
            .withColumn(
                "cultural_sites_per_1000_residents",
                spark_round(
                    (col("cult.total_cultural_sites") / col("pop.total_population"))
                    * 1000,
                    2,
                ),
            )
            .withColumn("analysis_level", lit("district"))
            .select(
                col("cult.district"),
                col("analysis_level"),
                col("cult.total_cultural_sites"),
                col("cult.unique_categories"),
                col("cult.unique_facility_types"),
                col("pop.total_population"),
                col("cultural_sites_per_1000_residents"),
            )
        )

        # Neighborhood-level cultural analytics
        neighborhood_cultural = cultural_clean.groupBy("district", "neighborhood").agg(
            count("*").alias("total_cultural_sites"),
            expr("size(collect_set(category))").alias("unique_categories"),
            expr("size(collect_set(facility_type))").alias("unique_facility_types"),
        )

        neighborhood_cultural_final = (
            neighborhood_cultural.alias("cult")
            .join(
                latest_population.alias("pop"),
                (col("cult.district") == col("pop.district"))
                & (col("cult.neighborhood") == col("pop.neighborhood")),
                "left",
            )
            .withColumn(
                "cultural_sites_per_1000_residents",
                when(
                    col("pop.population") > 0,
                    spark_round(
                        (col("cult.total_cultural_sites") / col("pop.population"))
                        * 1000,
                        2,
                    ),
                ).otherwise(0.0),
            )
            .withColumn("analysis_level", lit("neighborhood"))
            .select(
                col("cult.district"),
                col("cult.neighborhood"),
                col("analysis_level"),
                col("cult.total_cultural_sites"),
                col("cult.unique_categories"),
                col("cult.unique_facility_types"),
                col("pop.population"),
                col("cultural_sites_per_1000_residents"),
            )
        )

        # Category analysis
        category_analytics = (
            cultural_clean.filter(col("category").isNotNull())
            .groupBy("district", "category")
            .agg(count("*").alias("category_count"))
        )

        # Write cultural analytics
        district_output_path = str(
            self.exploitation_zone_path / "cultural_district_analytics"
        )
        district_cultural_final.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(district_output_path)

        neighborhood_output_path = str(
            self.exploitation_zone_path / "cultural_neighborhood_analytics"
        )
        neighborhood_cultural_final.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(neighborhood_output_path)

        category_output_path = str(
            self.exploitation_zone_path / "cultural_category_analytics"
        )
        category_analytics.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(category_output_path)

        district_count = district_cultural_final.count()
        neighborhood_count = neighborhood_cultural_final.count()
        self.logger.info(f"Cultural District Analytics: {district_count} records")
        self.logger.info(
            f"Cultural Neighborhood Analytics: {neighborhood_count} records"
        )

    def create_integrated_analytics(self, datasets: Dict[str, Any]) -> None:
        """
        Create integrated analytics dataset for composite KPIs.

        Joins all datasets for:
        - Neighborhood attractiveness scoring
        - Housing affordability analysis
        - Spatial equity assessment
        """
        self.logger.info("Creating integrated analytics dataset...")

        # Load the analytics datasets we just created
        property_analytics = (
            self.spark.read.format("delta")
            .load(str(self.exploitation_zone_path / "property_analytics"))
            .filter(col("analysis_level") == "neighborhood")
        )

        socioeconomic_analytics = self.spark.read.format("delta").load(
            str(self.exploitation_zone_path / "socioeconomic_neighborhood_analytics")
        )

        cultural_analytics = self.spark.read.format("delta").load(
            str(self.exploitation_zone_path / "cultural_neighborhood_analytics")
        )

        income_quintiles = self.spark.read.format("delta").load(
            str(self.exploitation_zone_path / "income_quintiles")
        )

        # Start with property analytics as base
        integrated = property_analytics.alias("prop").select(
            col("prop.district"),
            col("prop.neighborhood"),
            col("prop.total_properties"),
            col("prop.avg_price_eur"),
            col("prop.median_price_eur"),
            col("prop.avg_price_per_m2"),
            col("prop.median_price_per_m2"),
            col("prop.avg_size_m2"),
        )

        # Join socioeconomic data
        integrated = (
            integrated.alias("base")
            .join(
                socioeconomic_analytics.alias("socio").select(
                    col("socio.district_name").alias("socio_district"),
                    col("socio.neighborhood_name").alias("socio_neighborhood"),
                    col("socio.income_index_bcn_100"),
                    col("socio.population"),
                ),
                (col("base.district") == col("socio_district"))
                & (col("base.neighborhood") == col("socio_neighborhood")),
                "left",
            )
            .select(
                col("base.district"),
                col("base.neighborhood"),
                col("base.total_properties"),
                col("base.avg_price_eur"),
                col("base.median_price_eur"),
                col("base.avg_price_per_m2"),
                col("base.median_price_per_m2"),
                col("base.avg_size_m2"),
                col("income_index_bcn_100"),
                col("population"),
            )
        )

        # Join cultural data
        integrated = (
            integrated.alias("base2")
            .join(
                cultural_analytics.alias("cult").select(
                    col("cult.district"),
                    col("cult.neighborhood"),
                    col("cult.total_cultural_sites"),
                    col("cult.cultural_sites_per_1000_residents"),
                ),
                (col("base2.district") == col("cult.district"))
                & (col("base2.neighborhood") == col("cult.neighborhood")),
                "left",
            )
            .select(
                col("base2.*"),
                col("cult.total_cultural_sites"),
                col("cult.cultural_sites_per_1000_residents"),
            )
        )

        # Join income quintiles
        integrated = (
            integrated.alias("base3")
            .join(
                income_quintiles.alias("quint").select(
                    col("quint.district_name").alias("quint_district"),
                    col("quint.neighborhood_name").alias("quint_neighborhood"),
                    col("quint.income_quintile"),
                ),
                (col("base3.district") == col("quint_district"))
                & (col("base3.neighborhood") == col("quint_neighborhood")),
                "left",
            )
            .select(
                col("base3.*"),
                col("income_quintile"),
            )
        )

        # Calculate composite metrics
        integrated_final = (
            integrated.withColumn(
                # Housing affordability ratio (lower is more affordable)
                "affordability_ratio",
                spark_round(
                    col("median_price_eur") / (col("income_index_bcn_100") * 1000), 2
                ),
            )
            .withColumn(
                # Neighborhood attractiveness score (normalized composite)
                "attractiveness_score",
                spark_round(
                    (
                        col("income_index_bcn_100") * 0.3
                        + col("cultural_sites_per_1000_residents") * 10 * 0.3
                        + (1000 / col("avg_price_per_m2")) * 100 * 0.4
                    ),
                    2,
                ),
            )
            .withColumn(
                # Market accessibility (properties available)
                "market_accessibility",
                when(col("total_properties") > 100, "High")
                .when(col("total_properties") > 50, "Medium")
                .otherwise("Low"),
            )
            .withColumn("created_at", from_utc_timestamp(current_timestamp(), "UTC"))
        )

        # Filter out records with insufficient data
        integrated_clean = integrated_final.filter(
            col("district").isNotNull()
            & col("neighborhood").isNotNull()
            & col("median_price_eur").isNotNull()
            & col("income_index_bcn_100").isNotNull()
        )

        # Write integrated analytics
        output_path = str(self.exploitation_zone_path / "integrated_analytics")
        integrated_clean.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("income_quintile").save(output_path)

        record_count = integrated_clean.count()
        self.logger.info(f"Integrated Analytics: {record_count} records created")

    def validate_exploitation_data(self) -> Dict[str, Any]:
        """Validate the exploitation datasets and return summary statistics."""
        self.logger.info("Validating exploitation datasets...")

        results = {}
        expected_datasets = [
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

        for dataset in expected_datasets:
            try:
                path = str(self.exploitation_zone_path / dataset)
                df = self.spark.read.format("delta").load(path)

                record_count = df.count()
                column_count = len(df.columns)

                results[dataset] = {
                    "record_count": record_count,
                    "column_count": column_count,
                    "path": path,
                }

                self.logger.info(
                    f"{dataset}: {record_count:,} records, {column_count} columns"
                )

                # Show sample for key datasets
                if dataset in ["integrated_analytics", "property_analytics"]:
                    df.show(3, truncate=False)

            except Exception as e:
                self.logger.error(f"Failed to validate {dataset}: {str(e)}")
                results[dataset] = {"error": str(e)}

        return results

    def run_pipeline(self) -> Dict[str, Any]:
        """Execute the complete exploitation pipeline."""
        try:
            self.logger.info("=== Starting Data Exploitation Pipeline ===")

            # Initialize Spark
            self.initialize_spark()

            # Create output directory
            self.create_exploitation_zone()

            # Load formatted data
            datasets = self.load_formatted_data()

            # Create analytical datasets
            self.create_property_analytics(datasets["idealista"])
            self.create_socioeconomic_analytics(datasets["income"])
            self.create_cultural_analytics(
                datasets["cultural_sites"], datasets["income"]
            )

            # Create integrated dataset
            self.create_integrated_analytics(datasets)

            # Validate results
            results = self.validate_exploitation_data()

            self.logger.info("=== Exploitation Pipeline Completed Successfully ===")
            return results

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main() -> int:
    """Main execution function."""
    print("=== Data Exploitation Pipeline - A.3 ===")

    load_dotenv()

    # Get paths from user
    FORMATTED_ZONE_PATH = (
        os.getenv("FORMATTED_PATH")
        or input(
            "Enter Formatted Zone path (press Enter for 'formatted_zone'): "
        ).strip()
        or "formatted_zone"
    )

    EXPLOITATION_ZONE_PATH = (
        os.getenv("EXPLOITATION_PATH")
        or input(
            "Enter Exploitation Zone path (press Enter for 'exploitation_zone'): "
        ).strip()
        or "exploitation_zone"
    )

    # Validate formatted zone exists
    if not Path(FORMATTED_ZONE_PATH).exists():
        print(f"❌ Error: Formatted Zone path '{FORMATTED_ZONE_PATH}' does not exist!")
        print("Please run A.2 (Data Formatting Pipeline) first.")
        return 1

    try:
        # Run pipeline
        pipeline = ExploitationPipeline(FORMATTED_ZONE_PATH, EXPLOITATION_ZONE_PATH)
        results = pipeline.run_pipeline()

        # Print summary
        print("\n" + "=" * 60)
        print("EXPLOITATION PIPELINE SUMMARY")
        print("=" * 60)

        total_records = 0
        success_count = 0

        for dataset, stats in results.items():
            if "record_count" in stats:
                count = stats["record_count"]
                print(f"✅ {dataset}: {count:,} records")
                total_records += count
                success_count += 1
            else:
                print(f"❌ {dataset}: Processing failed")

        print(f"\n🎯 Total: {total_records:,} records across {success_count} datasets")
        print("✅ Analytics datasets ready for KPI calculations")
        print("\n📊 Available datasets for analysis:")
        print("   • property_analytics - Housing market metrics")
        print("   • socioeconomic_*_analytics - Income and demographic data")
        print("   • cultural_*_analytics - Cultural facility distribution")
        print("   • integrated_analytics - Combined dataset for composite KPIs")

        return 0

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
