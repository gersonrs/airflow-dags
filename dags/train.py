from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sdk import Asset
from dags.utils.constants import default_args
from mlflow_provider.hooks.client import MLflowClientHook
from sklearn.linear_model import LogisticRegression

FILE_PATH = "features.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

# MLFlow parameters
MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "POC"
REGISTERED_MODEL_NAME = "modelIris"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000

# Data parameters
TARGET_COLUMN = "target"


@dag(
    dag_id="train_model",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=[Asset("s3://" + DATA_BUCKET_NAME + "/temp/" + FILE_PATH)],
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Train"],
)
def train() -> None:  # noqa: C901
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Asset("model_trained")])

    @task
    def fetch_feature_df(**context: Any):
        "Fetch the feature dataframe from the feature engineering DAG."
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering",
            task_ids="feature_eng",
            include_prior_dates=True,
        )
        return feature_df

    @task
    def fetch_experiment_id(experiment_name: str, max_results: int = 1000) -> Any:
        "Get the ID of the specified MLFlow experiment."

        mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
        experiments_information = mlflow_hook.run(
            endpoint="api/2.0/mlflow/experiments/search",
            request_params={"max_results": max_results},
        ).json()

        for experiment in experiments_information["experiments"]:
            if experiment["name"] == experiment_name:
                return experiment["experiment_id"]

        raise ValueError(f"{experiment_name} not found in MLFlow experiments.")

    # Train a model
    # @task(executor_config=etl_config)
    @task(task_id="train_model")
    def train_model(feature_df: dict, experiment_id: str, run_name: str) -> str:
        "Train a model and log it to MLFlow."
        import mlflow
        import pandas as pd  # Importe o pandas se precisar tipar internamente

        mlflow.sklearn.autolog()

        # Certifique-se de que o xcom_pull anterior entregou um dicionário puro do Python
        X_train = feature_df["X_train"]
        y_train = feature_df["y_train"]

        model = LogisticRegression()

        with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
            model.fit(X_train, y_train)
            mlflow.sklearn.log_model(model, "model")

        run_id = run.info.run_id
        return str(run_id)

    fetched_feature_df = fetch_feature_df()
    fetched_experiment_id = fetch_experiment_id(experiment_name=EXPERIMENT_NAME)

    model_trained = train_model(
        feature_df=fetched_feature_df,
        experiment_id=fetched_experiment_id,
        run_name="ModelLR-{{ ts_nodash }}",
    )

    (start >> [fetched_feature_df, fetched_experiment_id] >> model_trained >> end)


train()
