"""
### Generate True Values with MLflow

Artificially generates feedback on the predictions made by the model in the predict DAG.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from astro import sql as aql
from astro.files import File
from utils.constants import default_args

from airflow import Dataset

log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

DATA_FILE_PATH = "data.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

XCOM_BUCKET = "localxcom"


@dag(
    dag_id="temp_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@once",
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Generate values"],
)
def generate_values() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        outlets=[Dataset("astro+s3://conn_minio_s3@data/data.parquet")],
    )

    create_buckets_if_not_exists = S3CreateBucketOperator.partial(
        task_id="create_buckets_if_not_exists",
        aws_conn_id=AWS_CONN_ID,
    ).expand(bucket_name=[DATA_BUCKET_NAME, MLFLOW_ARTIFACT_BUCKET, XCOM_BUCKET])

    @aql.dataframe()
    def generate_df_values() -> pd.DataFrame:
        from sklearn import datasets

        # load iris dataset
        iris = datasets.load_iris()
        # Since this is a bunch, create a dataframe
        df = pd.DataFrame(iris.data)
        df.columns = [
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
        ]

        df["target"] = iris.target

        df.dropna(how="all", inplace=True)  # remove any empty lines

        return df

    true_values = generate_df_values()

    save_data_to_other_s3 = aql.export_file(
        task_id="save_data_to_other_s3",
        input_data=true_values,
        output_file=File(
            path=os.path.join("s3://", DATA_BUCKET_NAME, DATA_FILE_PATH), conn_id=AWS_CONN_ID
        ),
        if_exists="replace",
    )

    (start >> [true_values, create_buckets_if_not_exists] >> save_data_to_other_s3 >> end)


generate_true_values = generate_values()
