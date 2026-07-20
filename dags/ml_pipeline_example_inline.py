"""
DAG de exemplo de ML Pipeline — demonstra o uso de:
  - magalu_operators.maga_ml_task  (KubernetesPodOperator customizado)
  - gcs_xcom_backend.GCSXComBackend (XCom com DataFrames no GCS)

Como usar:
  1. Configure a variável de ambiente XCOM_GCS_BUCKET no Composer.
  2. Coloque os scripts Python referenciados em script_path no bucket de DAGs.
  3. Ative o backend XCom customizado no airflow.cfg ou via variável:
       AIRFLOW__CORE__XCOM_BACKEND = gcs_xcom_backend.GCSXComBackend
"""

from datetime import datetime, timedelta

from airflow.decorators import task

from airflow import DAG

# ──────────────────────────────────────────────
# Configurações padrão da DAG
# ──────────────────────────────────────────────
TEAM_POOL = "ml_team"  # Pool definido no Airflow para sua equipe
DEFAULT_IMAGE = "gersonrs/airflow:latest"  # Imagem Docker com dependências de ML

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_pipeline_example_inline",
    description="Pipeline de ML de exemplo usando o decorator do kubernetes",
    default_args=default_args,
    schedule="0 6 * * 1",  # Toda segunda às 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "exemplo"],
) as dag:

    @task(task_id="extrai_dados", pool=TEAM_POOL)
    def extrai_dados() -> dict:
        import pandas as pd

        df = pd.DataFrame({"feature_a": [1, 2], "target": [0, 1]})
        return {"shape": df.shape}

    # ──────────────────────────────────────────────
    # Feature engineering rodando em um Pod Isolado via Função Inline
    # ──────────────────────────────────────────────
    @task.kubernetes(
        task_id="feature_engineering",
        namespace="default",
        image=DEFAULT_IMAGE,
        in_cluster=True,  # Usa as credenciais do cluster onde o Airflow está
        is_delete_operator_pod=True,
        get_logs=True,
        deferrable=True,
        env_vars={"AMBIENTE": "producao"},
    )
    def feature_engineering_inline(dados_extraidos: dict):
        """
        ATENÇÃO: Tudo aqui dentro roda no Pod isolado!
        Você deve fazer os imports DENTRO da função.
        """
        import os

        import pandas as pd

        print(f"Executando dentro do K8s com a imagem {DEFAULT_IMAGE}")
        shape = dados_extraidos.get("shape")
        print(f"Shape recebido via XCom: {shape}")

        # A lógica real iria aqui
        resultado = "features_geradas_com_sucesso"

        return resultado

    # Dependências (A TaskFlow API resolve tudo sozinha)
    dados = extrai_dados()
    features = feature_engineering_inline(dados)
