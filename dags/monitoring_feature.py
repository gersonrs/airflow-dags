from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from astro import sql as aql
from astro.files import File
from utils.constants import default_args

FEATURE_FILE_PATH = "features.parquet"
DATA_FILE_PATH = "data.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"


@dag(
    dag_id="monitoring_feature",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # schedule=[Dataset("prediction_data")],
    schedule_interval="@daily",
    default_view="graph",
    render_template_as_native_obj=True,
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Monitoring"],
)
def feature_monitoring() -> None:
    ref_data = aql.load_file(
        task_id="get_ref_data",
        input_file=File(
            path=os.path.join("s3://", DATA_BUCKET_NAME, DATA_FILE_PATH), conn_id=AWS_CONN_ID
        ),
    )

    curr_data = aql.load_file(
        task_id="get_curr_data",
        input_file=File(
            path=os.path.join("s3://" + DATA_BUCKET_NAME, DATA_FILE_PATH),
            conn_id=AWS_CONN_ID,
        ),
    )

    # feature_data = (Dataset("s3://" + DATA_BUCKET_NAME + "/temp/" + FEATURE_FILE_PATH),)

    @aql.dataframe(columns_names_capitalization="lower")
    def generate_reports(ref_data: pd.DataFrame, curr_data: pd.DataFrame) -> dict[str, Any]:
        from evidently.test_preset import DataDriftTestPreset
        from evidently.test_suite import TestSuite

        suite = TestSuite(tests=[DataDriftTestPreset()])
        suite.run(reference_data=ref_data, current_data=curr_data)

        return suite.as_dict()

    reports = generate_reports(ref_data=ref_data, curr_data=curr_data)

    send_report = EmptyOperator(task_id="send_alert")
    # send_report = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_alert",
    #     text="""
    #     *Evidently Test Suite results:*
    #     ```{report}```
    #     """.format(
    #         report="{{ ti.xcom_pull(task_ids='generate_reports') }}"
    #     ),
    #     channel="#integrations",
    # )

    @task.short_circuit
    def check_drift(metrics: dict[str, list[dict[str, str]]]) -> bool:
        status = metrics["tests"][0]["status"]
        logging.info(status)
        if status == "FAIL":
            return True
        return False

    send_retrain_alert = EmptyOperator(task_id="send_retrain_alert")
    # send_retrain_alert = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_retrain_alert",
    #     text="""
    #     *Warning:* Retrain was triggered because of data drift conditions.
    #     {description}
    #     """.format(
    #         description="{{ti.xcom_pull(task_ids='generate_reports')['tests'][0]['description']}}"
    #     ),
    #     channel="#integrations",
    # )

    trigger_retrain = TriggerDagRunOperator(task_id="trigger_retrain", trigger_dag_id="train_model")

    cleanup = aql.cleanup()

    chain(
        reports,
        check_drift(metrics="{{ ti.xcom_pull(task_ids='generate_reports') }}"),
        trigger_retrain,
        send_retrain_alert,
    )

    reports >> [send_report, cleanup]


feature_monitoring()
