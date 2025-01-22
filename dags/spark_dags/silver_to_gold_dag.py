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

# [START import_module]
# O objeto DAG; precisaremos disso para instanciar um DAG
from airflow.decorators import dag

# Operadores, precisamos que isso funcione!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

# [END import_module]


# [INICIO import_module]
# O decorator dag; precisaremos disso para instanciar um DAG
# Operadores; precisamos disso para funcionar!

# [FIM import_module]
# Documentação baseada em Markdown que serão renderizados nas páginas Grid , Graph e Calendar.
doc_md_DAG = """
# DAG Entrega dos dados que vem de um csv para uma tabela iceberg no minio

Este é um exemplo de DAG que usa SparkKubernetesOperator e SparkKubernetesSensor.
Neste exemplo, crio duas tarefas que são executadas sequencialmente.
A primeira tarefa é enviar sparkApplication no cluster Kubernetes.
E a segunda tarefa é verificar o estado final do sparkApplication que enviou no primeiro estado.

## Objetivo desta DAG

* Processar todos os dados da raw zone referentes aos dados de torre e passar para uma tabela
iceberg no minio

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
    "pool": "slow_pool",
}
# [FIM default_args]


# [INICIO dag]
@dag(
    dag_id="silver-to-gold",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["spark", "kubernetes", "sensor", "iceberg", "minio", "s3", "gold", "silver"],
    doc_md=doc_md_DAG,
)
def silver_to_gold_dag() -> None:
    """
    `silver_to_gold_dag()` é uma função que define um DAG
    (Directed Gráfico acíclico) no Apache Airflow. Este DAG é responsável por ingerir
    dados de uma tabela bronze para uma tabela silver. Consiste em duas tarefas:
    """

    # [INICIO set_tasks]

    # A variável(task) `submit` está criando uma instância da classe
    # `SparkKubernetesOperator`. Esse operador é responsável por enviar um
    # `SparkApplication` para execução em um cluster Kubernetes. Atravez da definição
    # de yaml para acionar o processo, usando o spark-on-k8s para operar com base nos
    # dados e criando um `SparkApplication` em contêiner.
    submit = SparkKubernetesOperator(
        task_id="silver_to_gold_submit",
        namespace="processing",
        application_file="silver_to_gold.yaml",
        do_xcom_push=True,
        # O parâmetro `params` no `SparkKubernetesOperator` é usado para passar parâmetros
        # adicionais para o `SparkApplication` que será executado no cluster Kubernetes.
        # Esses parâmetros podem ser acessados no código do aplicativo Spark.
        queue="kubernetes",
        params={
            "spark_driver_cores": 2,
            "spark_driver_memory": "2G",
            "spark_executor_cores": 2,
            "spark_executor_instances": 1,
            "spark_executor_memory": "2G",
            "spark_job_name": "silver-to-gold",
            "spark_file": "silver_to_gold.py",
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
# `silver_to_gold_dag()` está criando uma instância da DAG
# `delivery-data-from-sap-hana-to-kafka`. Esta função(instância) pode ser usada para
# iniciar a execução da DAG no Apache Airflow.
silver_to_gold_dag()
# [FIM start_dag]
