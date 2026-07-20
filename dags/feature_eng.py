from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sdk import Asset
from astro import sql as aql
from astro.files import File
from mlflow_provider.hooks.client import MLflowClientHook
from utils.constants import default_args

log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

FEATURE_FILE_PATH = "features.parquet"
DATA_FILE_PATH = "data.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

# MLFlow parameters
MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "POC"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000


@dag(
    dag_id="feaure_engineering",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=[
        Asset(
            "s3://data/data.parquet", name="temp_ingestion_parquet", connection_id="conn_minio_s3"
        )
    ],
    # schedule="@once",
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "feature engineering"],
)
def feature_eng() -> None:  # noqa: C901
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        outlets=[Asset("s3://" + DATA_BUCKET_NAME + "/temp/" + FEATURE_FILE_PATH)],
    )

    @task_group
    def prepare_mlflow_experiment() -> None:
        @task
        def list_existing_experiments(max_results: int = 1000) -> Any:
            "Get information about existing MLFlow experiments."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            existing_experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": max_results},
            ).json()

            return existing_experiments_information

        @task.branch
        def check_if_experiment_exists(
            experiment_name: str,
            existing_experiments_information: dict[str, list[dict[str, str]]],
        ) -> Any:
            "Check if the specified experiment already exists."

            if existing_experiments_information:
                existing_experiment_names = [
                    experiment["name"]
                    for experiment in existing_experiments_information["experiments"]
                ]
                if experiment_name in existing_experiment_names:
                    return "prepare_mlflow_experiment.experiment_exists"
                else:
                    return "prepare_mlflow_experiment.create_experiment"
            else:
                return "prepare_mlflow_experiment.create_experiment"

        @task
        def create_experiment(experiment_name: str, artifact_bucket: str) -> Any:
            """Create a new MLFlow experiment with a specified name.
            Save artifacts to the specified S3 bucket."""

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": 1000},
            ).json()
            num = -1000
            for experiment in experiments_information["experiments"]:
                if num < int(experiment["experiment_id"]):
                    num = int(experiment["experiment_id"])

            new_experiment_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/create",
                request_params={
                    "name": experiment_name,
                    "artifact_location": f"s3://{artifact_bucket}/{num+1}",
                },
            ).json()

            return new_experiment_information

        experiment_already_exists = EmptyOperator(task_id="experiment_exists")

        @task(trigger_rule="none_failed")
        def get_current_experiment_id(experiment_name: str, max_results: int = 1000) -> Any:
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

        experiment_id = get_current_experiment_id(
            experiment_name=EXPERIMENT_NAME,
            max_results=MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS,
        )

        (
            check_if_experiment_exists(
                experiment_name=EXPERIMENT_NAME,
                existing_experiments_information=list_existing_experiments(
                    max_results=MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS
                ),
            )
            >> [
                experiment_already_exists,
                create_experiment(
                    experiment_name=EXPERIMENT_NAME,
                    artifact_bucket=MLFLOW_ARTIFACT_BUCKET,
                ),
            ]
            >> experiment_id
        )

    iris_data = aql.load_file(
        task_id="load_iris_data",
        input_file=File(
            path=os.path.join("s3://", DATA_BUCKET_NAME, DATA_FILE_PATH), conn_id=AWS_CONN_ID
        ),
    )

    @aql.dataframe(multiple_outputs=True)
    def feature_eng(df: pd.DataFrame, experiment_id: str, name: str) -> Any:
        import mlflow
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        mlflow.sklearn.autolog()

        print(df.head())

        y = df["target"]
        X = df.drop(columns=["target"])[
            ["sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm"]
        ]

        scaler = StandardScaler()
        X = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)

        with mlflow.start_run(experiment_id=experiment_id, run_name=name):
            mlflow.sklearn.log_model(scaler, artifact_path="scaler")
            mlflow.log_metrics(pd.DataFrame(scaler.mean_, index=X.columns)[0].to_dict())

        log.info(pd.concat([X, y], axis=1).head())

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        X_train_df = pd.DataFrame(X_train, columns=X.columns)
        X_train_df.index = X_train.index

        X_test_df = pd.DataFrame(X_test, columns=X.columns)
        X_test_df.index = X_test.index

        y_train_df = pd.DataFrame({"target": y_train})
        y_train_df.index = X_train.index

        y_test_df = pd.DataFrame({"target": y_test})
        y_test_df.index = X_test.index

        return {
            "X_train": X_train_df,
            "X_test": X_test_df,
            "y_train": y_train_df,
            "y_test": y_test_df,
        }

    (
        start
        >> prepare_mlflow_experiment()
        >> feature_eng(
            df=iris_data,
            experiment_id="{{ ti.xcom_pull(task_ids='prepare_mlflow_experiment.get_current_experiment_id') }}",  # noqa: E501
            name="Scaler_{{ ts_nodash }}",
        )
        >> end
    )


feature_eng()
