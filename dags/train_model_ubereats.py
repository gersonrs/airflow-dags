from __future__ import annotations

import os
from typing import Any

from airflow import Dataset
from airflow.decorators import dag
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.dataframes.pandas import DataFrame
from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.registry import CreateModelVersionOperator
from mlflow_provider.operators.registry import CreateRegisteredModelOperator
from mlflow_provider.operators.registry import TransitionModelVersionStageOperator
from src.feature_engineer_common import create_model
from utils.constants import default_args

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

# Parâmetros
FEATURE_FILE_PATH = "features.parquet"
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "ubereats_feature_eng"
REGISTERED_MODEL_NAME = "UberEatsDeliveryPredictor"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000
TARGET_COLUMN = "Time_taken"


@dag(
    dag_id="train_model_ubereats",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule=[Dataset(f"s3://{DATA_BUCKET_NAME}/uber/{FEATURE_FILE_PATH}")],
    tags=["ubereats", "ml", "regression", "mlflow", "training"],
    default_view="graph",
)
def train_model_ubereats() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("ubereats_model_trained")])

    @task
    def fetch_feature_df(**context: Any) -> DataFrame:
        """Busca o DataFrame de features da DAG de feature engineering."""
        return context["ti"].xcom_pull(
            dag_id="feature_engineering_ubereats",
            task_ids="build_features",
            include_prior_dates=True,
        )

    @task
    def fetch_experiment_id(experiment_name: str, max_results: int = 1000) -> Any:
        mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
        experiments_information = mlflow_hook.run(
            endpoint="api/2.0/mlflow/experiments/search",
            request_params={"max_results": max_results},
        ).json()

        for experiment in experiments_information["experiments"]:
            if experiment["name"] == experiment_name:
                return experiment["experiment_id"]
        raise ValueError(f"{experiment_name} not found in MLFlow experiments.")

    @aql.dataframe(multiple_outputs=True)
    def split_data(data: DataFrame, target_column: str) -> dict[str, DataFrame]:
        from sklearn.model_selection import train_test_split

        X = data.drop(target_column, axis=1)
        y = data[target_column]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        return {
            "X_train": X_train,
            "X_test": X_test,
            "y_train": y_train.to_frame(name=target_column),
            "y_test": y_test.to_frame(name=target_column),
        }

    @aql.dataframe()
    def train_model_task(data: dict[str, DataFrame], experiment_id: str, run_name: str) -> str:
        import mlflow
        from tensorflow.keras.callbacks import EarlyStopping
        from sklearn.preprocessing import MinMaxScaler

        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

        mlflow.sklearn.autolog()

        early_stop = EarlyStopping(monitor="val_loss", mode="min", verbose=1, patience=3)

        scaler = MinMaxScaler()

        model = create_model()

        with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
            X_train = scaler.fit_transform(data["X_train"])
            X_test = scaler.transform(data["X_test"])
            mlflow.sklearn.log_model(scaler, "minmaxscaler")
            model.fit(
                x=X_train,
                y=data["y_train"],
                validation_data=(X_test, data["y_test"]),
                batch_size=1,
                epochs=100,
                callbacks=[early_stop],
            )
            input_example = X_train[:3, :]
            mlflow.tensorflow.log_model(model, "model", input_example=input_example)

        return run.info.run_id

    @task_group
    def register_model() -> None:
        @task.branch
        def check_if_model_registered(reg_model_name: str) -> str:
            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID, method="GET")
            response = mlflow_hook.run(
                endpoint="api/2.0/mlflow/registered-models/get",
                request_params={"name": reg_model_name},
            ).json()
            if "error_code" in response and response["error_code"] == "RESOURCE_DOES_NOT_EXIST":
                return "register_model.create_registered_model"
            return "register_model.model_already_registered"

        create_registered_model = CreateRegisteredModelOperator(
            task_id="create_registered_model",
            name=REGISTERED_MODEL_NAME,
            mlflow_conn_id=MLFLOW_CONN_ID,
            tags=[
                {"key": "model_type", "value": "regression"},
                {"key": "domain", "value": "ubereats"},
            ],
        )

        model_already_registered = EmptyOperator(task_id="model_already_registered")

        create_model_version = CreateModelVersionOperator(
            task_id="create_model_version",
            mlflow_conn_id=MLFLOW_CONN_ID,
            name=REGISTERED_MODEL_NAME,
            source=f"s3://{MLFLOW_ARTIFACT_BUCKET}/"
            + "{{ ti.xcom_pull(task_ids='train_model_task') }}",
            run_id="{{ ti.xcom_pull(task_ids='train_model_task') }}",
            trigger_rule="none_failed",
        )

        transition_model = TransitionModelVersionStageOperator(
            task_id="transition_model",
            mlflow_conn_id=MLFLOW_CONN_ID,
            name=REGISTERED_MODEL_NAME,
            version="{{ ti.xcom_pull(task_ids='register_model.create_model_version')['model_version']['version'] }}",  # noqa: E501
            stage="Staging",
            archive_existing_versions=True,
        )

        (
            check_if_model_registered(reg_model_name=REGISTERED_MODEL_NAME)
            >> [model_already_registered, create_registered_model]
            >> create_model_version
            >> transition_model
        )

    # Encadeamento da DAG
    fetched_feature_df = fetch_feature_df()
    fetched_experiment_id = fetch_experiment_id(EXPERIMENT_NAME)

    run_id = train_model_task(
        data=split_data(fetched_feature_df, TARGET_COLUMN),
        experiment_id=fetched_experiment_id,
        run_name="UberEats-LinearRegression-{{ ts_nodash }}",
    )

    (start >> [fetched_feature_df, fetched_experiment_id] >> run_id >> register_model() >> end)


train_model_ubereats()
