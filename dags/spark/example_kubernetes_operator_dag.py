from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="example_kubernetes_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    # [START howto_operator_k8s_write_xcom]
    write_xcom = KubernetesPodOperator(
        namespace="default",
        image="hello-world:linux",
        name="write-xcom",
        do_xcom_push=True,
        on_finish_action="delete_pod",
        in_cluster=True,
        task_id="write-xcom",
        get_logs=True,
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom') }}\"",
        task_id="pod_task_xcom_result",
    )

    write_xcom >> pod_task_xcom_result
    # [END howto_operator_k8s_write_xcom]
