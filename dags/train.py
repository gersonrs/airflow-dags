from __future__ import annotations

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
from sklearn.linear_model import LogisticRegression

from utils.constants import default_args

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
    start_date=days_ago(1),
    catchup=False,
    schedule=[Dataset("s3://" + DATA_BUCKET_NAME + "/temp/" + FILE_PATH)],
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Train"],
)
def train() -> None:  # noqa: C901
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("model_trained")])

    @task
    def fetch_feature_df(**context: Any) -> DataFrame:
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
    @aql.dataframe()
    def train_model(feature_df: dict[str, DataFrame], experiment_id: str, run_name: str) -> Any:
        "Train a model and log it to MLFlow."

        import mlflow

        mlflow.sklearn.autolog()

        X_train = feature_df["X_train"]
        y_train = feature_df["y_train"]

        model = LogisticRegression()

        with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
            model.fit(X_train, y_train)
            # Registro do modelo
            mlflow.sklearn.log_model(model, "model")

        run_id = run.info.run_id

        return run_id

    fetched_feature_df = fetch_feature_df()
    fetched_experiment_id = fetch_experiment_id(experiment_name=EXPERIMENT_NAME)

    model_trained = train_model(
        feature_df=fetched_feature_df,
        experiment_id=fetched_experiment_id,
        run_name="ModelLR-{{ ts_nodash }}",
    )

    @task_group
    def register_model() -> None:
        @task.branch
        def check_if_model_already_registered(reg_model_name: str) -> Any:
            "Get information about existing registered MLFlow models."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID, method="GET")
            get_reg_model_response = mlflow_hook.run(
                endpoint="api/2.0/mlflow/registered-models/get",
                request_params={"name": reg_model_name},
            ).json()

            if "error_code" in get_reg_model_response:
                if get_reg_model_response["error_code"] == "RESOURCE_DOES_NOT_EXIST":
                    reg_model_exists = False
                else:
                    raise ValueError(
                        f"Error when checking if model is registered: {get_reg_model_response['error_code']}"  # noqa: E501
                    )
            else:
                reg_model_exists = True

            if reg_model_exists:
                return "register_model.model_already_registered"
            else:
                return "register_model.create_registered_model"

        model_already_registered = EmptyOperator(task_id="model_already_registered")

        create_registered_model = CreateRegisteredModelOperator(
            mlflow_conn_id=MLFLOW_CONN_ID,
            task_id="create_registered_model",
            name=REGISTERED_MODEL_NAME,
            tags=[
                {"key": "model_type", "value": "regression"},
                {"key": "data", "value": "iris"},
            ],
        )

        create_model_version = CreateModelVersionOperator(
            mlflow_conn_id=MLFLOW_CONN_ID,
            task_id="create_model_version",
            name=REGISTERED_MODEL_NAME,
            source="s3://"
            + MLFLOW_ARTIFACT_BUCKET
            + "/"
            + "{{ ti.xcom_pull(task_ids='train_model') }}",
            run_id="{{ ti.xcom_pull(task_ids='train_model') }}",
            trigger_rule="none_failed",
        )

        transition_model = TransitionModelVersionStageOperator(
            mlflow_conn_id=MLFLOW_CONN_ID,
            task_id="transition_model",
            name=REGISTERED_MODEL_NAME,
            version="{{ ti.xcom_pull(task_ids='register_model.create_model_version')['model_version']['version'] }}",  # noqa: E501
            stage="Staging",
            archive_existing_versions=True,
        )

        (
            check_if_model_already_registered(reg_model_name=REGISTERED_MODEL_NAME)
            >> [model_already_registered, create_registered_model]
            >> create_model_version
            >> transition_model
        )

    (
        start
        >> [fetched_feature_df, fetched_experiment_id]
        >> model_trained
        >> register_model()
        >> end
    )


train()
