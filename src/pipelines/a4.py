"""
A.4 Data Validation and Analytics - Lab 3: Spark
Location: A4_Data_Validation.ipynb

VALIDATION OVERVIEW:
1. Initialize Spark session with Delta Lake support
2. Validate data integrity in Formatted Zone (3 datasets)
3. Validate data integrity in Exploitation Zone (9+ datasets)
4. Perform data quality checks and basic analytics
5. Calculate and display key KPIs identified in A.1
6. Validate cross-dataset relationships and joins
7. Generate comprehensive validation report

DATASETS VALIDATED:
Formatted Zone:
- idealista: Real estate property data
- income: Socioeconomic income data
- cultural_sites: Cultural facility data

Exploitation Zone:
- property_analytics: Aggregated real estate metrics
- socioeconomic_*_analytics: Income and demographic analytics
- cultural_*_analytics: Cultural facility distribution
- integrated_analytics: Combined dataset for composite KPIs
- Additional analytical tables

KPIs CALCULATED:
- Housing market metrics (price/m², availability, distribution)
- Socioeconomic indicators (inequality, affordability)
- Cultural accessibility metrics (density, correlation)
- Composite scores (attractiveness, equity)
"""

import warnings

warnings.filterwarnings("ignore")

from typing import Any, Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# PySpark imports - importing functions separately to avoid conflicts
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)

# ============================================================================
# 1. SPARK SESSION INITIALIZATION
# ============================================================================


def initialize_spark_session() -> SparkSession:
    """Initialize Spark session with Delta Lake 4.0 support."""
    print("🚀 Initializing Spark session with Delta Lake support...")

    try:
        spark = (
            SparkSession.builder.appName("BCN_DataValidation_A4")
            .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark session initialized successfully")
        return spark

    except Exception as e:
        print(f"❌ Failed to initialize Spark: {str(e)}")
        raise


# ============================================================================
# 2. DATA LOADING AND BASIC VALIDATION
# ============================================================================


def load_and_validate_zones(
    spark: SparkSession,
    formatted_zone_path: str = "formatted_zone",
    exploitation_zone_path: str = "exploitation_zone",
) -> Dict[str, Any]:
    """Load and perform basic validation on both data zones."""

    print("\n" + "=" * 60)
    print("📂 LOADING AND VALIDATING DATA ZONES")
    print("=" * 60)

    results = {"formatted_zone": {}, "exploitation_zone": {}, "validation_summary": {}}

    # Formatted Zone datasets
    formatted_datasets = ["idealista", "income", "cultural_sites"]

    print("\n🔍 FORMATTED ZONE VALIDATION:")
    print("-" * 40)

    for dataset in formatted_datasets:
        try:
            path = f"{formatted_zone_path}/{dataset}"
            df = spark.read.format("delta").load(path)

            record_count = df.count()
            column_count = len(df.columns)

            # Calculate null count using PySpark aggregation
            null_counts = []
            for column_name in df.columns:
                null_count_for_col = df.filter(col(column_name).isNull()).count()
                null_counts.append(null_count_for_col)

            total_null_count = sum(null_counts)  # Python sum on Python list
            total_cells = record_count * column_count
            null_percentage = (
                (total_null_count / total_cells * 100) if total_cells > 0 else 0
            )

            results["formatted_zone"][dataset] = {
                "dataframe": df,
                "record_count": record_count,
                "column_count": column_count,
                "null_count": total_null_count,
                "null_percentage": null_percentage,
                "path": path,
            }

            print(
                f"✅ {dataset.upper()}: {record_count:,} records, {column_count} columns, {null_percentage:.1f}% missing"
            )

        except Exception as e:
            print(f"❌ {dataset.upper()}: Failed to load - {str(e)}")
            results["formatted_zone"][dataset] = {"error": str(e)}

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

    print("\n🔍 EXPLOITATION ZONE VALIDATION:")
    print("-" * 40)

    for dataset in exploitation_datasets:
        try:
            path = f"{exploitation_zone_path}/{dataset}"
            df = spark.read.format("delta").load(path)

            record_count = df.count()
            column_count = len(df.columns)

            results["exploitation_zone"][dataset] = {
                "dataframe": df,
                "record_count": record_count,
                "column_count": column_count,
                "path": path,
            }

            print(f"✅ {dataset}: {record_count:,} records, {column_count} columns")

        except Exception as e:
            print(f"❌ {dataset}: Failed to load - {str(e)}")
            results["exploitation_zone"][dataset] = {"error": str(e)}

    return results


# ============================================================================
# 3. DATA QUALITY VALIDATION
# ============================================================================


def perform_data_quality_checks(data_results: Dict[str, Any]) -> Dict[str, Any]:
    """Perform comprehensive data quality checks."""

    print("\n" + "=" * 60)
    print("🔍 DATA QUALITY VALIDATION CHECKS")
    print("=" * 60)

    quality_results = {}

    # Formatted Zone Quality Checks
    print("\n📊 FORMATTED ZONE QUALITY CHECKS:")
    print("-" * 40)

    for dataset_name, dataset_info in data_results["formatted_zone"].items():
        if "error" in dataset_info:
            continue

        df = dataset_info["dataframe"]
        quality_checks = {}

        print(f"\n🔎 Analyzing {dataset_name.upper()}:")

        # 1. Schema validation
        expected_columns = {
            "idealista": [
                "property_code",
                "district",
                "neighborhood",
                "price_eur",
                "size_m2",
                "price_per_m2",
            ],
            "income": [
                "year",
                "district_name",
                "neighborhood_name",
                "income_index_bcn_100",
            ],
            "cultural_sites": ["site_id", "facility_name", "district", "neighborhood"],
        }

        if dataset_name in expected_columns:
            missing_cols = set(expected_columns[dataset_name]) - set(df.columns)
            quality_checks["missing_critical_columns"] = list(missing_cols)
            print(
                f"   • Critical columns: {'✅ All present' if not missing_cols else f'❌ Missing: {missing_cols}'}"
            )

        # 2. Duplicate detection using PySpark
        total_count = df.count()
        unique_count = df.distinct().count()
        duplicate_count = total_count - unique_count
        duplicate_percentage = (
            (duplicate_count / total_count * 100) if total_count > 0 else 0
        )

        quality_checks["duplicate_count"] = duplicate_count
        quality_checks["duplicate_percentage"] = duplicate_percentage
        print(
            f"   • Duplicates: {duplicate_count:,} records ({duplicate_percentage:.1f}%)"
        )

        # 3. Dataset-specific validations using PySpark functions
        if dataset_name == "idealista":
            # Price validation using PySpark filter and count
            negative_prices = df.filter(col("price_eur") <= 0).count()
            zero_sizes = df.filter(col("size_m2") <= 0).count()
            quality_checks["negative_prices"] = negative_prices
            quality_checks["zero_sizes"] = zero_sizes
            print(f"   • Invalid prices: {negative_prices:,} records")
            print(f"   • Invalid sizes: {zero_sizes:,} records")

        elif dataset_name == "income":
            # Income validation using PySpark
            negative_income = df.filter(col("income_index_bcn_100") < 0).count()
            future_years = df.filter(col("year") > 2025).count()
            quality_checks["negative_income"] = negative_income
            quality_checks["future_years"] = future_years
            print(f"   • Negative income indices: {negative_income:,} records")
            print(f"   • Future years: {future_years:,} records")

        quality_results[dataset_name] = quality_checks

    # Exploitation Zone Quality Checks
    print("\n📊 EXPLOITATION ZONE QUALITY CHECKS:")
    print("-" * 40)

    key_datasets = ["property_analytics", "integrated_analytics"]

    for dataset_name in key_datasets:
        if (
            dataset_name in data_results["exploitation_zone"]
            and "error" not in data_results["exploitation_zone"][dataset_name]
        ):
            df = data_results["exploitation_zone"][dataset_name]["dataframe"]

            print(f"\n🔎 Analyzing {dataset_name.upper()}:")

            # Check for aggregation consistency using PySpark
            if dataset_name == "property_analytics":
                # Verify no negative aggregated values
                negative_prices = df.filter(col("avg_price_eur") < 0).count()
                negative_sizes = df.filter(col("avg_size_m2") < 0).count()
                print(f"   • Negative average prices: {negative_prices:,} records")
                print(f"   • Negative average sizes: {negative_sizes:,} records")

            elif dataset_name == "integrated_analytics":
                # Check join completeness using PySpark
                null_districts = df.filter(col("district").isNull()).count()
                null_prices = df.filter(col("median_price_eur").isNull()).count()
                null_income = df.filter(col("income_index_bcn_100").isNull()).count()

                print(f"   • Missing districts: {null_districts:,} records")
                print(f"   • Missing prices: {null_prices:,} records")
                print(f"   • Missing income data: {null_income:,} records")

    return quality_results


# ============================================================================
# 4. KPI CALCULATIONS AND ANALYTICS
# ============================================================================


def calculate_housing_market_kpis(data_results: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate housing market KPIs."""

    print("\n" + "=" * 60)
    print("🏠 HOUSING MARKET KPI CALCULATIONS")
    print("=" * 60)

    kpi_results = {}

    if "property_analytics" in data_results["exploitation_zone"]:
        df = data_results["exploitation_zone"]["property_analytics"]["dataframe"]

        # KPI 1: Average Price per m² by District
        print("\n📈 KPI 1: Average Price per m² by District")
        district_prices = (
            df.filter(col("analysis_level") == "district")
            .select("district", "avg_price_per_m2", "total_properties")
            .orderBy(col("avg_price_per_m2").desc())
        )

        print("Top 5 Most Expensive Districts:")
        district_prices.show(5, truncate=False)

        # Convert to pandas for storage and visualization
        kpi_results["avg_price_per_m2_by_district"] = district_prices.toPandas()

        # KPI 2: Property Availability by District
        print("\n📈 KPI 2: Property Availability by District")
        availability = (
            df.filter(col("analysis_level") == "district")
            .select("district", "total_properties")
            .orderBy(col("total_properties").desc())
        )

        print("Districts by Property Availability:")
        availability.show(10, truncate=False)

        kpi_results["property_availability"] = availability.toPandas()

    # Property Type Analysis
    if "property_type_analytics" in data_results["exploitation_zone"]:
        df_types = data_results["exploitation_zone"]["property_type_analytics"][
            "dataframe"
        ]

        print("\n📈 KPI 3: Price Distribution by Property Type")
        type_analysis = (
            df_types.groupBy("property_type")
            .agg(
                avg("avg_price_per_m2_by_type").alias("overall_avg_price_per_m2"),
                spark_sum("type_count").alias("total_properties"),
            )
            .orderBy(col("overall_avg_price_per_m2").desc())
        )

        print("Property Types by Average Price per m²:")
        type_analysis.show(truncate=False)

        kpi_results["price_by_property_type"] = type_analysis.toPandas()

    return kpi_results


def calculate_socioeconomic_kpis(data_results: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate socioeconomic KPIs."""

    print("\n" + "=" * 60)
    print("💰 SOCIOECONOMIC KPI CALCULATIONS")
    print("=" * 60)

    kpi_results = {}

    if "socioeconomic_district_analytics" in data_results["exploitation_zone"]:
        df = data_results["exploitation_zone"]["socioeconomic_district_analytics"][
            "dataframe"
        ]

        # KPI 4: Income Inequality Index
        print("\n📈 KPI 4: Income Inequality Index by District")
        inequality = df.select(
            "district_name", "income_inequality_cv", "avg_income_index"
        ).orderBy(col("income_inequality_cv").desc())

        print("Districts by Income Inequality (Coefficient of Variation):")
        inequality.show(10, truncate=False)

        kpi_results["income_inequality"] = inequality.toPandas()

    # Income Quintile Distribution
    if "income_quintiles" in data_results["exploitation_zone"]:
        df_quintiles = data_results["exploitation_zone"]["income_quintiles"][
            "dataframe"
        ]

        print("\n📈 KPI 5: Income Quintile Distribution")
        quintile_dist = (
            df_quintiles.groupBy("income_quintile")
            .agg(
                count("*").alias("neighborhood_count"),
                avg("income_index_bcn_100").alias("avg_income_index"),
            )
            .orderBy("income_quintile")
        )

        print("Income Distribution Across Quintiles:")
        quintile_dist.show(truncate=False)

        kpi_results["income_quintiles"] = quintile_dist.toPandas()

    return kpi_results


def calculate_cultural_accessibility_kpis(
    data_results: Dict[str, Any],
) -> Dict[str, Any]:
    """Calculate cultural accessibility KPIs."""

    print("\n" + "=" * 60)
    print("🎭 CULTURAL ACCESSIBILITY KPI CALCULATIONS")
    print("=" * 60)

    kpi_results = {}

    if "cultural_district_analytics" in data_results["exploitation_zone"]:
        df = data_results["exploitation_zone"]["cultural_district_analytics"][
            "dataframe"
        ]

        # KPI 7: Cultural Density by District
        print("\n📈 KPI 7: Cultural Density by District")
        cultural_density = df.select(
            "district",
            "total_cultural_sites",
            "cultural_sites_per_1000_residents",
            "total_population",
        ).orderBy(col("cultural_sites_per_1000_residents").desc())

        print("Districts by Cultural Sites per 1000 Residents:")
        cultural_density.show(10, truncate=False)

        kpi_results["cultural_density"] = cultural_density.toPandas()

    # Cultural Category Distribution
    if "cultural_category_analytics" in data_results["exploitation_zone"]:
        df_categories = data_results["exploitation_zone"][
            "cultural_category_analytics"
        ]["dataframe"]

        print("\n📈 Cultural Facility Categories")
        category_dist = (
            df_categories.groupBy("category")
            .agg(spark_sum("category_count").alias("total_facilities"))
            .orderBy(col("total_facilities").desc())
        )

        print("Cultural Facilities by Category:")
        category_dist.show(truncate=False)

        kpi_results["cultural_categories"] = category_dist.toPandas()

    return kpi_results


def calculate_composite_kpis(data_results: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate composite KPIs from integrated analytics."""

    print("\n" + "=" * 60)
    print("📊 COMPOSITE KPI CALCULATIONS")
    print("=" * 60)

    kpi_results = {}

    if "integrated_analytics" in data_results["exploitation_zone"]:
        df = data_results["exploitation_zone"]["integrated_analytics"]["dataframe"]

        # KPI 8: Housing Affordability Analysis
        print("\n📈 KPI 8: Housing Affordability Ratio")
        affordability = (
            df.filter(col("affordability_ratio").isNotNull())
            .select(
                "district",
                "neighborhood",
                "affordability_ratio",
                "median_price_eur",
                "income_index_bcn_100",
            )
            .orderBy(col("affordability_ratio").asc())
        )

        print("Most Affordable Neighborhoods (Lower ratio = more affordable):")
        affordability.show(10, truncate=False)

        print("Least Affordable Neighborhoods:")
        affordability.orderBy(col("affordability_ratio").desc()).show(
            10, truncate=False
        )

        kpi_results["affordability_analysis"] = affordability.toPandas()

        # KPI 9: Neighborhood Attractiveness Score
        print("\n📈 KPI 9: Neighborhood Attractiveness Score")
        attractiveness = (
            df.filter(col("attractiveness_score").isNotNull())
            .select(
                "district",
                "neighborhood",
                "attractiveness_score",
                "income_index_bcn_100",
                "cultural_sites_per_1000_residents",
                "avg_price_per_m2",
            )
            .orderBy(col("attractiveness_score").desc())
        )

        print("Most Attractive Neighborhoods:")
        attractiveness.show(10, truncate=False)

        kpi_results["neighborhood_attractiveness"] = attractiveness.toPandas()

        # KPI 10: Market Accessibility Analysis
        print("\n📈 KPI 10: Market Accessibility")
        market_access = (
            df.groupBy("market_accessibility")
            .agg(
                count("*").alias("neighborhood_count"),
                avg("total_properties").alias("avg_properties"),
                avg("median_price_eur").alias("avg_median_price"),
            )
            .orderBy("market_accessibility")
        )

        print("Market Accessibility Distribution:")
        market_access.show(truncate=False)

        kpi_results["market_accessibility"] = market_access.toPandas()

    return kpi_results


# ============================================================================
# 5. CROSS-DATASET RELATIONSHIP VALIDATION
# ============================================================================


def validate_cross_dataset_relationships(
    data_results: Dict[str, Any],
) -> Dict[str, Any]:
    """Validate relationships and joins between datasets."""

    print("\n" + "=" * 60)
    print("🔗 CROSS-DATASET RELATIONSHIP VALIDATION")
    print("=" * 60)

    validation_results = {}

    # Check district/neighborhood consistency across datasets
    if (
        "idealista" in data_results["formatted_zone"]
        and "income" in data_results["formatted_zone"]
    ):
        idealista_df = data_results["formatted_zone"]["idealista"]["dataframe"]
        income_df = data_results["formatted_zone"]["income"]["dataframe"]

        print("\n🔍 DISTRICT CONSISTENCY CHECK:")
        print("-" * 40)

        # Get unique districts using PySpark operations
        idealista_districts_df = (
            idealista_df.select("district")
            .filter(col("district").isNotNull())
            .distinct()
        )
        income_districts_df = (
            income_df.select("district_name")
            .filter(col("district_name").isNotNull())
            .distinct()
        )

        # Convert to Python sets for comparison
        idealista_districts = set(
            [row.district for row in idealista_districts_df.collect()]
        )
        income_districts = set(
            [row.district_name for row in income_districts_df.collect()]
        )

        common_districts = idealista_districts.intersection(income_districts)
        idealista_only = idealista_districts - income_districts
        income_only = income_districts - idealista_districts

        print(f"✅ Common districts: {len(common_districts)}")
        print(
            f"⚠️  Idealista only: {len(idealista_only)} - {list(idealista_only)[:5]}..."
        )
        print(f"⚠️  Income only: {len(income_only)} - {list(income_only)[:5]}...")

        validation_results["district_consistency"] = {
            "common_count": len(common_districts),
            "idealista_only_count": len(idealista_only),
            "income_only_count": len(income_only),
        }

    # Validate integrated analytics completeness
    if "integrated_analytics" in data_results["exploitation_zone"]:
        df = data_results["exploitation_zone"]["integrated_analytics"]["dataframe"]

        print("\n🔍 INTEGRATED ANALYTICS COMPLETENESS:")
        print("-" * 40)

        total_records = df.count()
        complete_records = df.filter(
            col("district").isNotNull()
            & col("neighborhood").isNotNull()
            & col("median_price_eur").isNotNull()
            & col("income_index_bcn_100").isNotNull()
        ).count()

        completeness_rate = (
            (complete_records / total_records * 100) if total_records > 0 else 0
        )

        print(f"Total integrated records: {total_records:,}")
        print(f"Complete records: {complete_records:,}")
        print(f"Completeness rate: {completeness_rate:.1f}%")

        validation_results["integration_completeness"] = {
            "total_records": total_records,
            "complete_records": complete_records,
            "completeness_rate": completeness_rate,
        }

    return validation_results


# ============================================================================
# 6. VISUALIZATION AND SUMMARY REPORTING
# ============================================================================


def create_summary_visualizations(kpi_results: Dict[str, Any]) -> None:
    """Create summary visualizations for key KPIs."""

    print("\n" + "=" * 60)
    print("📊 GENERATING SUMMARY VISUALIZATIONS")
    print("=" * 60)

    # Set up the plotting style
    plt.style.use("default")
    sns.set_palette("husl")

    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(
        "Barcelona Urban Analytics - Key KPIs Summary", fontsize=16, fontweight="bold"
    )

    # Plot 1: Average Price per m² by District
    if "avg_price_per_m2_by_district" in kpi_results:
        df = kpi_results["avg_price_per_m2_by_district"]
        if not df.empty:
            top_districts = df.head(8)
            axes[0, 0].barh(
                top_districts["district"], top_districts["avg_price_per_m2"]
            )
            axes[0, 0].set_title("Average Price per m² by District")
            axes[0, 0].set_xlabel("Price per m² (€)")
            axes[0, 0].tick_params(axis="y", labelsize=8)

    # Plot 2: Income Inequality
    if "income_inequality" in kpi_results:
        df = kpi_results["income_inequality"]
        if not df.empty:
            top_inequality = df.head(8)
            axes[0, 1].bar(
                range(len(top_inequality)), top_inequality["income_inequality_cv"]
            )
            axes[0, 1].set_title("Income Inequality by District (CV)")
            axes[0, 1].set_ylabel("Coefficient of Variation")
            axes[0, 1].set_xticks(range(len(top_inequality)))
            axes[0, 1].set_xticklabels(
                top_inequality["district_name"], rotation=45, ha="right", fontsize=8
            )

    # Plot 3: Cultural Density
    if "cultural_density" in kpi_results:
        df = kpi_results["cultural_density"]
        if not df.empty:
            df_clean = df.dropna(subset=["cultural_sites_per_1000_residents"])
            if not df_clean.empty:
                axes[1, 0].scatter(
                    df_clean["total_population"],
                    df_clean["cultural_sites_per_1000_residents"],
                )
                axes[1, 0].set_title("Cultural Density vs Population")
                axes[1, 0].set_xlabel("Total Population")
                axes[1, 0].set_ylabel("Cultural Sites per 1000 Residents")

    # Plot 4: Affordability Distribution
    if "affordability_analysis" in kpi_results:
        df = kpi_results["affordability_analysis"]
        if not df.empty:
            df_clean = df.dropna(subset=["affordability_ratio"])
            if not df_clean.empty and len(df_clean) > 0:
                affordability_values = df_clean["affordability_ratio"]
                # Remove extreme outliers for better visualization
                q75, q25 = np.percentile(affordability_values, [75, 25])
                iqr = q75 - q25
                lower_bound = q25 - (1.5 * iqr)
                upper_bound = q75 + (1.5 * iqr)
                filtered_values = affordability_values[
                    (affordability_values >= lower_bound)
                    & (affordability_values <= upper_bound)
                ]

                axes[1, 1].hist(filtered_values, bins=20, alpha=0.7)
                axes[1, 1].set_title("Housing Affordability Distribution")
                axes[1, 1].set_xlabel("Affordability Ratio")
                axes[1, 1].set_ylabel("Number of Neighborhoods")

    plt.tight_layout()
    plt.show()

    print("✅ Visualizations generated successfully")


def generate_validation_report(
    data_results: Dict[str, Any],
    quality_results: Dict[str, Any],
    kpi_results: Dict[str, Any],
    relationship_results: Dict[str, Any],
) -> None:
    """Generate comprehensive validation report."""

    print("\n" + "=" * 80)
    print("📋 COMPREHENSIVE VALIDATION REPORT")
    print("=" * 80)

    # Summary Statistics
    print("\n📊 DATASET SUMMARY:")
    print("-" * 40)

    formatted_total = sum(
        [
            info.get("record_count", 0)
            for info in data_results["formatted_zone"].values()
            if "record_count" in info
        ]
    )
    exploitation_total = sum(
        [
            info.get("record_count", 0)
            for info in data_results["exploitation_zone"].values()
            if "record_count" in info
        ]
    )

    print(
        f"Formatted Zone: {len(data_results['formatted_zone'])} datasets, {formatted_total:,} total records"
    )
    print(
        f"Exploitation Zone: {len(data_results['exploitation_zone'])} datasets, {exploitation_total:,} total records"
    )

    # Data Quality Assessment
    print("\n🔍 DATA QUALITY ASSESSMENT:")
    print("-" * 40)

    quality_score = 0
    quality_factors = 0

    for dataset, checks in quality_results.items():
        if "duplicate_percentage" in checks:
            dup_score = max(0, 100 - checks["duplicate_percentage"])
            quality_score += dup_score
            quality_factors += 1
            print(f"{dataset}: {dup_score:.1f}% quality (duplicates)")

    if quality_factors > 0:
        overall_quality = quality_score / quality_factors
        print(f"\nOverall Data Quality Score: {overall_quality:.1f}/100")

    # KPI Summary
    print("\n📈 KPI VALIDATION SUMMARY:")
    print("-" * 40)

    kpi_count = len(
        [
            k
            for k in kpi_results.keys()
            if isinstance(kpi_results[k], pd.DataFrame) and not kpi_results[k].empty
        ]
    )
    print(f"✅ Successfully calculated {kpi_count} KPIs")

    if (
        "avg_price_per_m2_by_district" in kpi_results
        and not kpi_results["avg_price_per_m2_by_district"].empty
    ):
        price_df = kpi_results["avg_price_per_m2_by_district"]
        max_price = price_df["avg_price_per_m2"].max()
        min_price = price_df["avg_price_per_m2"].min()
        print(f"Price range: {min_price:.0f}€ - {max_price:.0f}€ per m²")

    if (
        "income_inequality" in kpi_results
        and not kpi_results["income_inequality"].empty
    ):
        ineq_df = kpi_results["income_inequality"]
        max_ineq = ineq_df["income_inequality_cv"].max()
        print(f"Max income inequality: {max_ineq:.1f}% CV")

    # Integration Assessment
    print("\n🔗 DATA INTEGRATION ASSESSMENT:")
    print("-" * 40)

    if "integration_completeness" in relationship_results:
        completeness = relationship_results["integration_completeness"][
            "completeness_rate"
        ]
        print(f"Integration completeness: {completeness:.1f}%")

        if completeness >= 90:
            print("✅ Excellent integration quality")
        elif completeness >= 75:
            print("⚠️  Good integration quality")
        else:
            print("❌ Poor integration quality - review joins")

    # Final Assessment
    print("\n🎯 FINAL ASSESSMENT:")
    print("-" * 40)

    issues = []

    if formatted_total == 0:
        issues.append("No data in formatted zone")
    if exploitation_total == 0:
        issues.append("No data in exploitation zone")
    if quality_factors > 0 and overall_quality < 70:
        issues.append(f"Low data quality ({overall_quality:.1f}%)")
    if kpi_count < 5:
        issues.append(f"Few KPIs calculated ({kpi_count})")

    if not issues:
        print("✅ ALL VALIDATIONS PASSED")
        print("🎉 Data pipeline is ready for production analysis!")
        print("\n📋 Recommended next steps:")
        print("   • Proceed with advanced analytics and modeling")
        print("   • Create production dashboards")
        print("   • Implement automated monitoring")
    else:
        print("⚠️  ISSUES IDENTIFIED:")
        for issue in issues:
            print(f"   • {issue}")
        print("\n📋 Recommended actions:")
        print("   • Review and fix identified issues")
        print("   • Re-run data pipelines if necessary")
        print("   • Validate data sources")


# ============================================================================
# 7. MAIN EXECUTION FUNCTION
# ============================================================================


def run_comprehensive_validation(
    formatted_zone_path: str = "formatted_zone",
    exploitation_zone_path: str = "exploitation_zone",
) -> Dict[str, Any]:
    """
    Run comprehensive validation of the data pipeline.

    Args:
        formatted_zone_path: Path to the formatted zone directory
        exploitation_zone_path: Path to the exploitation zone directory

    Returns:
        Dictionary containing all validation results
    """

    print("🚀 STARTING COMPREHENSIVE DATA VALIDATION")
    print("=" * 80)

    # Initialize Spark
    spark = initialize_spark_session()

    try:
        # 1. Load and validate zones
        data_results = load_and_validate_zones(
            spark, formatted_zone_path, exploitation_zone_path
        )

        # 2. Perform data quality checks
        quality_results = perform_data_quality_checks(data_results)

        # 3. Calculate KPIs
        print("\n🔄 Calculating KPIs...")
        housing_kpis = calculate_housing_market_kpis(data_results)
        socioeconomic_kpis = calculate_socioeconomic_kpis(data_results)
        cultural_kpis = calculate_cultural_accessibility_kpis(data_results)
        composite_kpis = calculate_composite_kpis(data_results)

        # Combine all KPI results
        all_kpi_results = {
            **housing_kpis,
            **socioeconomic_kpis,
            **cultural_kpis,
            **composite_kpis,
        }

        # 4. Validate cross-dataset relationships
        relationship_results = validate_cross_dataset_relationships(data_results)

        # 5. Create visualizations
        create_summary_visualizations(all_kpi_results)

        # 6. Generate final report
        generate_validation_report(
            data_results, quality_results, all_kpi_results, relationship_results
        )

        # Return comprehensive results
        return {
            "data_results": data_results,
            "quality_results": quality_results,
            "kpi_results": all_kpi_results,
            "relationship_results": relationship_results,
            "validation_status": "PASSED",
        }

    except Exception as e:
        print(f"\n❌ VALIDATION FAILED: {str(e)}")
        return {"validation_status": "FAILED", "error": str(e)}

    finally:
        # Clean up Spark session
        if spark:
            spark.stop()
            print("\n🔚 Spark session closed")


# ============================================================================
# 8. QUICK EXECUTION CELLS
# ============================================================================


def quick_validation_check():
    """Quick validation check - run this first to test setup."""
    print("🔍 QUICK VALIDATION CHECK")
    print("=" * 40)

    spark = initialize_spark_session()

    try:
        # Test if zones exist and are accessible
        formatted_path = "formatted_zone"
        exploitation_path = "exploitation_zone"

        # Check if paths exist
        from pathlib import Path

        if not Path(formatted_path).exists():
            print(f"❌ Formatted zone not found at: {formatted_path}")
            return False

        if not Path(exploitation_path).exists():
            print(f"❌ Exploitation zone not found at: {exploitation_path}")
            return False

        # Try to load one dataset from each zone
        try:
            df_idealista = spark.read.format("delta").load(
                f"{formatted_path}/idealista"
            )
            count_idealista = df_idealista.count()
            print(
                f"✅ Formatted zone accessible: {count_idealista:,} Idealista records"
            )
        except Exception as e:
            print(f"❌ Cannot access formatted zone: {e}")
            return False

        try:
            df_integrated = spark.read.format("delta").load(
                f"{exploitation_path}/integrated_analytics"
            )
            count_integrated = df_integrated.count()
            print(
                f"✅ Exploitation zone accessible: {count_integrated:,} integrated records"
            )
        except Exception as e:
            print(f"❌ Cannot access exploitation zone: {e}")
            return False

        print("✅ Quick validation passed - ready for full validation!")
        return True

    finally:
        spark.stop()


# Example usage in Jupyter:
"""
# Cell 1: Quick check
quick_validation_check()

# Cell 2: Full validation  
results = run_comprehensive_validation()

# Cell 3: Access specific results
if results["validation_status"] == "PASSED":
    print("✅ Validation successful!")
    # Access KPI results
    kpis = results["kpi_results"]
    print(f"Calculated {len(kpis)} KPIs")
else:
    print("❌ Validation failed:", results.get("error", "Unknown error"))
"""
