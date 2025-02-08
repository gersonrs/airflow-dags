from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from typing import Any

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 10),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "github_repositories_dag",
    default_args=default_args,
    description="Extrai dados da API do GitHub e processa",
    schedule_interval=timedelta(days=1),
)


def check_api_available() -> None:
    url = "https://api.github.com"
    response = requests.get(url)
    response.raise_for_status()  # Lança uma exceção se o status não for 200


check_api_task = PythonOperator(
    task_id="check_api_available",
    python_callable=check_api_available,
    dag=dag,
)


def extract_github_data() -> None:
    url = "https://api.github.com/search/repositories?q=language:python&sort=stars&order=desc"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


extract_data_task = PythonOperator(
    task_id="extract_github_data",
    python_callable=extract_github_data,
    dag=dag,
)


def process_github_data(**context: dict[str, Any]) -> None:
    data = context["ti"].xcom_pull(task_ids="extract_github_data")  # type: ignore
    repositories = data["items"]
    languages = [repo["language"] for repo in repositories]
    df = pd.DataFrame(languages, columns=["language"])
    language_counts = df["language"].value_counts().reset_index()
    language_counts.columns = ["language", "count"]
    language_counts.to_csv("language_counts.csv", index=False)


process_data_task = PythonOperator(
    task_id="process_github_data",
    python_callable=process_github_data,
    dag=dag,
)

check_api_task >> extract_data_task >> process_data_task
