import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# --- Config ---

ZONE = os.getenv("EXPLOITATION_ZONE", "/opt/airflow/data/03_exploitation")
DATA_PATH = ZONE + "/house_prices/house_prices.parquet"  # exact parquet file path

MLFLOW_URI = os.environ.get("MLFLOW_URI")
TARGET_COLUMN = "price"
print(f"Using MLFLOW_URI: {MLFLOW_URI}")

MLFLOW_EXPERIMENT_NAME = "HousePriceRegression"
MODEL_NAME = "house_price_model"

def main():
    spark = SparkSession.builder.appName("HousePriceRegressionTraining").getOrCreate()
    df = spark.read.parquet(DATA_PATH)

    # Drop 'id' since it's unique identifier, not a feature
    df = df.drop("id")

    # Drop rows with nulls in features or target
    feature_cols = [col for col in df.columns if col != TARGET_COLUMN]
    df = df.na.drop(subset=feature_cols + [TARGET_COLUMN])

    # Handle categorical column(s)
    categorical_cols = ["neighbourhood"]
    indexers = [StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="skip") for col in categorical_cols]
    # Replace original categorical columns with indexed versions for features
    feature_cols = [col for col in feature_cols if col not in categorical_cols] + [col + "_idx" for col in categorical_cols]

    # VectorAssembler to combine features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Prepare pipeline stages before model: indexers + assembler
    stages = indexers + [assembler]

    # --- Train/Validation Split ---
    # We'll apply stages before split to avoid data leakage during indexing,
    # so fit indexers and assembler on entire data first.
    pipeline_prep = Pipeline(stages=stages)
    df_prepped = pipeline_prep.fit(df).transform(df)

    # Select final dataset with features and label
    dataset = df_prepped.select("features", TARGET_COLUMN).withColumnRenamed(TARGET_COLUMN, "label")

    train_df, val_df = dataset.randomSplit([0.8, 0.2], seed=42)
    print("ACAAAA2...")
    # --- Set Up MLflow ---
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    models = [
        ("LinearRegression", LinearRegression(maxIter=50)),
        ("RandomForestRegressor", RandomForestRegressor(numTrees=100))
    ]
    print("ACAAAA3...")
    evaluator = RegressionEvaluator(metricName="rmse")
    best_rmse = float("inf")
    best_run_id = None

    print("Starting model training...")

    for name, model in models:
        with mlflow.start_run(run_name=name) as run:
            # Only the regressor stage is added here because feature preprocessing already applied
            pipeline = Pipeline(stages=[model])
            pipeline_model = pipeline.fit(train_df)

            predictions = pipeline_model.transform(val_df)
            rmse = evaluator.evaluate(predictions)

            mlflow.log_param("model_type", name)
            mlflow.log_metric("rmse", rmse)
            mlflow.spark.log_model(pipeline_model, artifact_path="model")

            print(f"{name} RMSE: {rmse:.2f}")

            if rmse < best_rmse:
                best_rmse = rmse
                best_run_id = run.info.run_id
    return best_run_id


def auto_register_and_deploy_model(best_model_id: str = None):
        mlflow.set_tracking_uri(MLFLOW_URI)
        client = mlflow.tracking.MlflowClient()

        # Get the best run (e.g., lowest RMSE)
        experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["metrics.rmse ASC"],
            max_results=1,
        )

        best_run = runs[0]
        print(f"Best run ID: {best_run.info.run_id}, RMSE: {best_run.data.metrics['rmse']}")

        # Register model
        model_uri = f"runs:/{best_run.info.run_id}/model"
        result = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)

        # Transition to Production
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=result.version,
            stage="Production",
            archive_existing_versions=True
        )

        print(f"Model {MODEL_NAME} version {result.version} deployed to Production.")
    

if __name__ == "__main__":
    main()
