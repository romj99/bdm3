from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from ml_experiments.house_price_prediction import main as train_main
from ml_experiments.house_price_prediction import auto_register_and_deploy_model

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
}

@dag(
    dag_id="train_and_deploy_model",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ml", "mlflow"],
)
def train_deploy_pipeline():
    @task
    def run_training():
        run_id = train_main()
        return run_id

    @task
    def deploy_best_model(best_model_id):
        auto_register_and_deploy_model(best_model_id)

    # Task: Serve model with BashOperator
    serve_model = BashOperator(
        task_id="serve_model",
        bash_command="""
            pkill -f "mlflow models serve -m models:/house_price_model/Production" || true
            nohup mlflow models serve -m "models:/house_price_model/Production" -p 1234 --no-conda > /tmp/mlflow_serve.log 2>&1 &
        """
    )

    best_model_id = run_training()
    deploy = deploy_best_model(best_model_id)

    deploy >> serve_model  # Serve only after deploy finishes

dag = train_deploy_pipeline()  # **Make sure to call the DAG function!**
