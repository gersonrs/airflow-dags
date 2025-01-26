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

from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

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
# DAG Entrega dos dados que vem da landing para uma tabela bronze

Este é um exemplo de DAG que usa SparkKubernetesOperator e SparkKubernetesSensor.
Neste exemplo, crio duas tarefas que são executadas sequencialmente.
A primeira tarefa é enviar sparkApplication no cluster Kubernetes.
E a segunda tarefa é verificar o estado final do sparkApplication que enviou no primeiro estado.

## Objetivo desta DAG

* Processar todos os dados da landing zone referentes aos dados de user,subscription,movies
e credit_card, passando para uma tabela na camada bronze no minio

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
    dag_id="ingestion-from-landing-data-file-to-bronze-tables",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["spark", "kubernetes", "sensor", "iceberg", "minio", "s3", "raw", "bronze"],
    doc_md=doc_md_DAG,
)
def ingestion_from_landing_data_file_to_bronze_tables_dag() -> None:
    """
    `ingestion_from_landing_data_file_to_bronze_tables_dag()` é uma função que define um DAG
    (Directed Gráfico acíclico) no Apache Airflow. Este DAG é responsável por ingerir
    dados dda landing, processar e colocar em uma tabela delta na camada bronze.
    Consiste em duas tarefas:
    """

    # [INICIO set_tasks]

    # A variável(task) `submit` está criando uma instância da classe
    # `SparkKubernetesOperator`. Esse operador é responsável por enviar um
    # `SparkApplication` para execução em um cluster Kubernetes. Atravez da definição
    # de yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
    # dados e criando um `SparkApplication` em contêiner.
    submit = SparkKubernetesOperator(
        task_id="ingestion_from_landing_data_file_to_bronze_tables_submit",
        namespace="processing",
        application_file="yamls/ingestion_from_landing_data_file_to_bronze_tables.yaml",
        kubernetes_conn_id="conn_kubernetes",
        # O parâmetro `params` no `SparkKubernetesOperator` é usado para passar parâmetros
        # adicionais para o `SparkApplication` que será executado no cluster Kubernetes.
        # Esses parâmetros podem ser acessados no código do aplicativo Spark.
        params={
            "spark_driver_cores": 2,
            "spark_driver_memory": "2G",
            "spark_executor_cores": 2,
            "spark_executor_instances": 2,
            "spark_executor_memory": "2G",
            "spark_job_name": "ingestion-from-landing-data-file-to-bronze-tables",
            "spark_file": "ingestion_from_landing_data_file_to_bronze_tables.py",
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
    submit
    # [FIM task_sequence]


# [FIM dag]

# [INICIO start_dag]
# `ingestion_from_landing_data_file_to_bronze_tables_dag()` está criando uma instância da DAG
# `ingestion-from-landing-data-file-to-bronze-tables`. Esta função(instância) pode ser usada para
# iniciar a execução da DAG no Apache Airflow.
ingestion_from_landing_data_file_to_bronze_tables_dag()
# [FIM start_dag]
