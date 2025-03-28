from __future__ import annotations

import os
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from airflow import Dataset
from airflow.decorators import dag
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.files import File
from src.feature_engineer_common import GetMetrics
from utils.constants import default_args

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"
MLFLOW_CONN_ID = "conn_mlflow"
FILE_TO_SAVE_PREDICTIONS = "ubereats_predictions.parquet"


@dag(
    dag_id="predict_model_ubereats",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule=[Dataset("ubereats_model_trained")],
    default_view="graph",
    tags=["ubereats", "ml", "prediction", "regression", "mlflow"],
)
def predict_model_ubereats() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def fetch_feature_df_test(**context: Any) -> pd.DataFrame:
        feature_df = context["ti"].xcom_pull(
            dag_id="train_model_ubereats",
            task_ids="split_data",
            include_prior_dates=True,
        )
        return feature_df["X_test"]

    @task
    def fetch_target_test(**context: Any) -> pd.DataFrame:
        feature_df = context["ti"].xcom_pull(
            dag_id="train_model_ubereats",
            task_ids="split_data",
            include_prior_dates=True,
        )
        return feature_df["y_test"]

    @task
    def fetch_model_run_id(**context: Any) -> str:
        return context["ti"].xcom_pull(
            dag_id="train_model_ubereats", task_ids="train_model_task", include_prior_dates=True
        )

    fetched_feature_df = fetch_feature_df_test()
    fetched_model_run_id = fetch_model_run_id()
    target_data = fetch_target_test()

    @aql.dataframe()
    def prediction(data: pd.DataFrame, run_id: str) -> pd.DataFrame:
        import mlflow

        logged_model = f"runs:/{run_id}/model"
        model = mlflow.pyfunc.load_model(logged_model)
        preds = model.predict(data)
        return pd.DataFrame(preds, columns=["Predictions"], index=data.index)

    run_prediction = prediction(fetched_feature_df, fetched_model_run_id)

    @aql.dataframe()
    def metrics(y_test: pd.DataFrame, y_pred: pd.DataFrame, run_id: str) -> None:
        import mlflow
        from sklearn.metrics import explained_variance_score, max_error

        y_true = y_test["target"] if "target" in y_test.columns else y_test.squeeze()
        y_pred = y_pred["Predictions"]

        metrics = GetMetrics(y_true, y_pred)

        with mlflow.start_run(run_id=run_id):
            mlflow.log_metric("MAE", metrics["MAE"])
            mlflow.log_metric("MSE", metrics["MSE"])
            mlflow.log_metric("RMSE", metrics["RMSE"])
            mlflow.log_metric("R2", metrics["R2"])
            mlflow.log_metric("Max Error", max_error(y_test, y_pred))
            mlflow.log_metric("Explained Variance", explained_variance_score(y_test, y_pred))

    @task
    def plot_predictions(y_test: pd.DataFrame, y_pred: pd.DataFrame, run_id: str) -> None:
        import mlflow

        os.makedirs("plots", exist_ok=True)

        y_true = y_test["target"] if "target" in y_test.columns else y_test.squeeze()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(y_true.values, label="True", color="green")
        ax.plot(y_pred["Predictions"].values, label="Predicted", color="blue")
        ax.set_title("Predicted vs True Delivery Time")
        ax.set_xlabel("Index")
        ax.set_ylabel("Delivery Time (min)")
        ax.legend()

        predictions_plot = "plots/predictions_plot.png"
        fig.savefig(predictions_plot)
        plt.close()

        plt.figure(figsize=(12, 4))
        plt.scatter(y_test, y_pred)
        plt.plot(y_test, y_test, "r")
        plt.ylabel("Real Value")
        plt.xlabel("Prediction")
        plt.title("Model Predictions vs. Perfect Predictions (Line)")
        predictions_line = "plots/predictions_line.png"
        plt.savefig(predictions_line)
        plt.close()

        plt.figure(figsize=(12, 4))
        errors = y_test.values.reshape(len(y_pred), 1) - y_pred
        sns.distplot(errors)
        plt.title("Residuals")
        residuals = "plots/residuals.png"
        plt.savefig(residuals)
        plt.close()

        with mlflow.start_run(run_id=run_id):
            mlflow.log_artifact(predictions_plot, artifact_path="prediction-plots")
            mlflow.log_artifact(predictions_line, artifact_path="prediction-line")
            mlflow.log_artifact(residuals, artifact_path="Residuals")

    pred_file = aql.export_file(
        task_id="save_predictions",
        input_data=run_prediction,
        output_file=File(f"s3://{DATA_BUCKET_NAME}/{FILE_TO_SAVE_PREDICTIONS}", AWS_CONN_ID),
        if_exists="replace",
    )

    (
        start
        >> [
            metrics(target_data, run_prediction, fetched_model_run_id),
            plot_predictions(
                y_test=target_data, y_pred=run_prediction, run_id=fetched_model_run_id
            ),
        ]
        >> pred_file
        >> end
    )
    start >> fetched_feature_df

    start >> fetched_model_run_id


predict_model_ubereats()
