#
# Author: GersonRS
# Email: gersonrodriguessantos8@gmail.com
#
"""
Este é um exemplo de DAG que usa SparkKubernetesOperator e SparkKubernetesSensor.
Neste exemplo, crio duas tarefas que são executadas sequencialmente.
A primeira tarefa é enviar sparkApplication no cluster Kubernetes.
E a segunda tarefa é verificar o estado final do sparkApplication que enviou
no primeiro estado.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import Asset

# [START import_module]
# O objeto DAG; precisaremos disso para instanciar um DAG
# Operadores, precisamos que isso funcione!

# [END import_module]


# [INICIO import_module]
# O decorator dag; precisaremos disso para instanciar um DAG
# Operadores; precisamos disso para funcionar!

# [FIM import_module]
# Documentação baseada em Markdown que serão renderizados nas páginas Grid , Graph e Calendar.
doc_md_DAG = """
# DAG Entrega dos dados que estão na camada silver para a camada gold

Este é um exemplo de DAG que usa SparkKubernetesOperator e SparkKubernetesSensor.
Neste exemplo, crio duas tarefas que são executadas sequencialmente.
A primeira tarefa é enviar sparkApplication no cluster Kubernetes.
E a segunda tarefa é verificar o estado final do sparkApplication que enviou no primeiro estado.

## Objetivo desta DAG

* Processar todos os dados da silver zone referentes aos dados de subscribers e voters, passando
para uma tabela na camada gold no minio

Execute para testar.
"""

# [INICIO default_args]
# Esses argumentos serão basicamete repassados para cada operador
# Você pode substituí-los pelos valores que quiser durante a inicialização do operador
default_args = {
    "owner": "GersonRS",
    "depends_on_past": False,
    "email": ["gerson.santos@owshq.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(1),
}
# [FIM default_args]


# [INICIO dag]
@dag(
    dag_id="show-case-uber-eats",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@daily",
    max_active_runs=1,
    tags=["spark", "kubernetes", "delta", "minio", "s3", "silver", "gold"],
    doc_md=doc_md_DAG,
)
def show_case_uber_eats_dag() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        outlets=[Asset("s3://gold/delivery_dataset/")],
    )

    """
    `show_case_uber_eats_dag()` é uma função que define um DAG
    (Directed Gráfico acíclico) no Apache Airflow. Este DAG é responsável por ingerir
    dados da silver, processar e colocar em uma tabela delta na camada gold.
    Consiste em uma tarefa:
    """

    # [INICIO set_tasks]

    # A variável(task) `submit` está criando uma instância da classe
    # `SparkKubernetesOperator`. Esse operador é responsável por enviar um
    # `SparkApplication` para execução em um cluster Kubernetes. Atravez da definição
    # de yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
    # dados e criando um `SparkApplication` em contêiner.
    show_case_uber_eats_bronze_ingestion = SparkKubernetesOperator(
        task_id="bronze_ingestion",
        namespace="processing",
        application_file="yamls/show_case_uber_eats.yaml",
        kubernetes_conn_id="conn_kubernetes",
        # O parâmetro `params` no `SparkKubernetesOperator` é usado para passar parâmetros
        # adicionais para o `SparkApplication` que será executado no cluster Kubernetes.
        # Esses parâmetros podem ser acessados no código do aplicativo Spark.
        params={
            "spark_driver_cores": 2,
            "spark_driver_memory": "2G",
            "spark_executor_cores": 2,
            "spark_executor_instances": 1,
            "spark_executor_memory": "4G",
            "spark_job_name": "show-case-uber-eats-bronze-ingestion",
            "spark_file": "show_case_uber_eats_bronze_ingestion.py",
        },
        doc_md="""
        ### Proposta desta tarefa

        * Ser responsável por enviar um `SparkApplication` para execução em um cluster Kubernetes.

        * Definir um yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
        dados e criando um `SparkApplication` em contêiner.
        """,
    )

    show_case_uber_eats_silver_transformation = SparkKubernetesOperator(
        task_id="silver_transformation",
        namespace="processing",
        application_file="yamls/show_case_uber_eats.yaml",
        kubernetes_conn_id="conn_kubernetes",
        # O parâmetro `params` no `SparkKubernetesOperator` é usado para passar parâmetros
        # adicionais para o `SparkApplication` que será executado no cluster Kubernetes.
        # Esses parâmetros podem ser acessados no código do aplicativo Spark.
        params={
            "spark_driver_cores": 2,
            "spark_driver_memory": "2G",
            "spark_executor_cores": 2,
            "spark_executor_instances": 1,
            "spark_executor_memory": "4G",
            "spark_job_name": "show-case-uber-eats-silver-transformation",
            "spark_file": "show_case_uber_eats_silver_transformation.py",
        },
        doc_md="""
        ### Proposta desta tarefa

        * Ser responsável por enviar um `SparkApplication` para execução em um cluster Kubernetes.

        * Definir um yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
        dados e criando um `SparkApplication` em contêiner.
        """,
    )

    show_case_uber_eats_gold_dataset = SparkKubernetesOperator(
        task_id="gold_dataset",
        namespace="processing",
        application_file="yamls/show_case_uber_eats.yaml",
        kubernetes_conn_id="conn_kubernetes",
        # O parâmetro `params` no `SparkKubernetesOperator` é usado para passar parâmetros
        # adicionais para o `SparkApplication` que será executado no cluster Kubernetes.
        # Esses parâmetros podem ser acessados no código do aplicativo Spark.
        params={
            "spark_driver_cores": 2,
            "spark_driver_memory": "2G",
            "spark_executor_cores": 2,
            "spark_executor_instances": 1,
            "spark_executor_memory": "4G",
            "spark_job_name": "show-case-uber-eats-gold-dataset",
            "spark_file": "show_case_uber_eats_gold_dataset.py",
        },
        doc_md="""
        ### Proposta desta tarefa

        * Ser responsável por enviar um `SparkApplication` para execução em um cluster Kubernetes.

        * Definir um yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
        dados e criando um `SparkApplication` em contêiner.
        """,
    )

    # [FIM set_tasks]

    # [INICIO task_sequence]
    (
        start
        >> show_case_uber_eats_bronze_ingestion
        >> show_case_uber_eats_silver_transformation
        >> show_case_uber_eats_gold_dataset
        >> end
    )
    # [FIM task_sequence]


# [FIM dag]

# [INICIO start_dag]
# `show_case_uber_eats_dag()` está criando uma instância da DAG
# `show-case-uber-eats-bronze-ingestion`. Esta função(instância) pode ser usada para
# iniciar a execução da DAG no Apache Airflow.
show_case_uber_eats_dag()
# [FIM start_dag]
