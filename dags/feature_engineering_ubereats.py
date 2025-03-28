from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np
import pandas as pd
from airflow import Dataset
from airflow.decorators import dag
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.dataframes.pandas import DataFrame
from astro.files import File
from deltalake import DeltaTable
from mlflow_provider.hooks.client import MLflowClientHook
from src.feature_engineer_common import haversine_vector

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

# Arquivos e buckets
FEATURE_FILE_PATH = "features.parquet"
DATA_FILE_PATH = "gold/delivery_dataset/"

AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

# MLFlow
MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "ubereats_feature_eng"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000


@dag(
    schedule=[Dataset(f"s3://{DATA_FILE_PATH}")],
    start_date=days_ago(1),
    catchup=False,
    default_view="graph",
    tags=["ubereats", "mlflow", "feature_engineering"],
    dag_id="feature_engineering_ubereats",
)
def feature_engineering_ubereats() -> None:  # noqa: C901
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end", outlets=[Dataset(f"s3://{DATA_BUCKET_NAME}/uber/{FEATURE_FILE_PATH}")]
    )

    create_buckets = S3CreateBucketOperator.partial(
        task_id="create_buckets", aws_conn_id=AWS_CONN_ID
    ).expand(bucket_name=[DATA_BUCKET_NAME, MLFLOW_ARTIFACT_BUCKET])

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

    @aql.dataframe()
    def extract_data() -> pd.DataFrame:
        # Recupera as credenciais da conexão do Airflow
        conn = BaseHook.get_connection("conn_minio_s3")
        extra = conn.extra_dejson

        # Configurações do S3/MinIO
        s3_options = {
            "AWS_ACCESS_KEY_ID": extra.get("aws_access_key_id"),
            "AWS_SECRET_ACCESS_KEY": extra.get("aws_secret_access_key"),
            "AWS_ENDPOINT_URL": extra.get(
                "endpoint_url"
            ),  # exemplo: http://host.docker.internal:9000
        }

        # Caminho da tabela Delta no MinIO
        delta_path = "s3://gold/delivery_dataset/"

        # Lê a tabela Delta
        dt = DeltaTable(delta_path, storage_options=s3_options)

        # Converte para Pandas
        df = dt.to_pandas()

        return df

    extracted_df = extract_data()
    # extracted_df = aql.load_file(
    #     task_id="load_ubereats_data",
    #     input_file=File(
    #         path="s3://gold/delivery_dataset/", filetype=FileType.PARQUET, conn_id="conn_minio_s3"
    #     ),
    # )

    @aql.dataframe()
    def build_features(data: DataFrame, experiment_id: str) -> DataFrame:
        import mlflow
        from sklearn.preprocessing import OneHotEncoder
        from sklearn.compose import ColumnTransformer

        for column in data.columns:
            data[column] = data[column].apply(lambda value: np.nan if value == "NaN " else value)

        data["Time_Orderd"] = pd.to_datetime(data["Time_Orderd"])
        data["Time_Order_picked"] = pd.to_datetime(data["Time_Order_picked"])

        data["Time_To_Pick"] = (
            data["Time_Order_picked"] - data["Time_Orderd"]
        ).dt.total_seconds() / 60

        monhts = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December",
        }

        days = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }

        data["Order_Year"] = data["Time_Orderd"].apply(lambda value: value.year)
        data["Order_Month"] = data["Time_Orderd"].apply(lambda value: value.month).map(monhts)
        data["Order_Day"] = data["Time_Orderd"].apply(lambda value: value.dayofweek).map(days)

        data["Delivery_person_Age"] = data["Delivery_person_Age"].astype(float)
        data["Delivery_person_Ratings"] = data["Delivery_person_Ratings"].astype(float)
        data["multiple_deliveries"] = data["multiple_deliveries"].astype(float)
        data["Vehicle_condition"] = data["Vehicle_condition"].astype(float)
        data["Weatherconditions"] = data["Weatherconditions"].apply(
            lambda value: value.split(" ")[-1]
        )

        for feature in data.select_dtypes(include="O").columns:
            data[feature] = data[feature].apply(
                lambda value: np.nan if pd.isnull(value) else value.replace(" ", "")
            )
        data.loc[data["City"].isna(), "City"] = "Metropolitian"
        data.loc[data["multiple_deliveries"].isna(), "multiple_deliveries"] = 1.0

        data = data[(data["Delivery_person_Age"] >= 18) & (data["Delivery_person_Age"] <= 80)]
        data = data[data["Time_taken"] > 0]
        data["Delivery_person_Ratings"] = data["Delivery_person_Ratings"].apply(
            lambda x: round(x, 1)
        )
        data["Time_To_Pick"] = data["Time_To_Pick"].apply(lambda x: round(x, 1))
        data = data.drop("ID", axis=1)

        # Aplica o cálculo da distância
        data["DeliveryDistance"] = data.apply(haversine_vector, axis=1)

        q99 = data["DeliveryDistance"].quantile(0.99)
        data.loc[data["DeliveryDistance"] > q99, "DeliveryDistance"] = q99

        data["OrderTime"] = pd.to_datetime(data["Time_Order_picked"]).apply(
            lambda value: value.hour
        )

        data["TypeOfMeal"] = data["OrderTime"].apply(
            lambda value: (
                "Breakfast"
                if value in [8, 9, 10, 11]
                else "Launch"
                if value in [12, 13, 14, 15, 16]
                else "Dinner"
            )
        )

        data = data.drop(
            [
                "Delivery_person_ID",
                "Restaurant_latitude",
                "Restaurant_longitude",
                "Delivery_location_latitude",
                "Delivery_location_longitude",
                "Order_Date",
                "Time_Orderd",
                "Time_Order_picked",
                "Time_To_Pick",
                "Order_Year",
                "Order_Month",
                "Festival",
            ],
            axis=1,
        )

        objList = list(data.select_dtypes(include="object").columns)

        ct = ColumnTransformer([("encode", OneHotEncoder(), objList)], remainder="passthrough")

        with mlflow.start_run(experiment_id=experiment_id, run_name="ubereats_encoding"):
            data = pd.DataFrame(
                ct.fit_transform(data),
                columns=list(
                    map(
                        lambda x: x.replace("encode__", "").replace("remainder__", ""),
                        ct.get_feature_names_out(),
                    )
                ),
            )
            mlflow.sklearn.log_model(ct, "onehot_encoder")

        return data

    features = build_features(
        data=extracted_df,
        experiment_id="{{ ti.xcom_pull(task_ids='prepare_mlflow_experiment.get_current_experiment_id') }}",  # noqa: E501
    )

    (
        aql.export_file(
            task_id="export_features_to_s3",
            input_data=features,
            output_file=File(
                path=f"s3://{DATA_BUCKET_NAME}/uber/{FEATURE_FILE_PATH}",
                conn_id=AWS_CONN_ID,
            ),
            if_exists="replace",
        )
        >> end
    )

    start >> create_buckets >> prepare_mlflow_experiment() >> extracted_df >> features >> end


feature_engineering_ubereats()
