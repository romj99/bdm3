import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from mlflow.models.signature import infer_signature
from delta import configure_spark_with_delta_pip


ZONE = os.getenv("FORMATTED_ZONE")
DATA_PATH = ZONE + "/idealista"

MLFLOW_URI = os.environ.get("MLFLOW_URI")
TARGET_COLUMN = "price"
print(f"Using MLFLOW_URI: {MLFLOW_URI}")

MLFLOW_EXPERIMENT_NAME = "HousePriceRegression"
MODEL_NAME = "house_price_model"

def train():
    """ Train a regression model to predict house prices using Spark MLlib and log the model with MLflow."""
    # Spark Session
    builder = SparkSession.builder.appName("HousePriceRegressionTraining") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load Data
    df = spark.read.format("delta").load(DATA_PATH)
    df = df.select(
        'neighborhood', 'latitude', 'longitude', 'property_type', 'size_m2',
        'rooms', 'bathrooms', 'floor', 'status', 'price_eur', 'is_exterior',
        'has_lift', 'has_parking'
    )
    df = df.fillna({'neighborhood': 'unknown', "status": 'good', 'floor': 0})
    df = df.withColumnRenamed("price_eur", "price")
    df = df.filter(col("floor").rlike("^\d+$")).withColumn("floor", col("floor").cast(IntegerType()))

    categorical_cols = ["neighborhood", "property_type", "status"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="skip") for c in categorical_cols]
    numerical_cols = [c for c in df.columns if c not in categorical_cols + [TARGET_COLUMN]]

    feature_cols = numerical_cols + [f"{c}_idx" for c in categorical_cols]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    pre_pipeline = Pipeline(stages=indexers + [assembler])
    df_transformed = pre_pipeline.fit(df).transform(df)

    dataset = df_transformed.select("features", TARGET_COLUMN).withColumnRenamed(TARGET_COLUMN, "label")
    train_df, val_df = dataset.randomSplit([0.8, 0.2], seed=42)

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    models = [
        ("LinearRegression", LinearRegression(maxIter=100)),
        ("RandomForestRegressor", RandomForestRegressor(numTrees=100, maxBins=128))
    ]
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

            input_example_df = train_df.select("features").limit(5).toPandas()


            # Convert any DenseVectors in 'features' column to list
            if "features" in input_example_df.columns:
                input_example_df["features"] = input_example_df["features"].apply(lambda v: v.toArray().tolist())
            
            #print columns of input_example_df
            print("Input example columns:", input_example_df.columns)

            # Generate output example (optional for better signature)
            predictions_df = pipeline_model.transform(train_df.limit(5))
            output_example_df = predictions_df.select("prediction").toPandas()


            # Log the model
            mlflow.spark.log_model(
                spark_model=pipeline_model,
                artifact_path="model",
                input_example=input_example_df,
                signature=infer_signature(input_example_df, output_example_df)
            )

            print(f"{name} RMSE: {rmse:.2f}")

            if rmse < best_rmse:
                best_rmse = rmse
                best_run_id = run.info.run_id
    return best_run_id



def auto_register_and_deploy_model(best_model_id: str = None):
        """ Automatically register and deploy the best model from MLflow runs. """
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
    train()
