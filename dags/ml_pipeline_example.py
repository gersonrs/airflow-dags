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
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Importa o operador customizado
from magalu_operators import maga_ml_task

from airflow import DAG

# ──────────────────────────────────────────────
# Configurações padrão da DAG
# ──────────────────────────────────────────────
TEAM_POOL = "ml_team"  # Pool definido no Airflow para sua equipe
DEFAULT_IMAGE = "gcr.io/maga-bigdata/ml-base:latest"

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_pipeline_example",
    description="Pipeline de ML de exemplo usando maga_ml_task e GCSXComBackend",
    default_args=default_args,
    schedule="0 6 * * 1",  # Toda segunda às 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "exemplo"],
) as dag:

    # ──────────────────────────────────────────────
    # 1. Extração de dados  (task Python tradicional)
    # ──────────────────────────────────────────────
    @task(
        task_id="extrai_dados",
        pool=TEAM_POOL,
    )
    def extrai_dados() -> dict:
        """
        Busca dados de uma fonte externa (ex.: BigQuery) e retorna
        metadados sobre o dataset. DataFrames grandes são transferidos
        via XCom GCS automaticamente se o backend estiver ativo.
        """
        import pandas as pd

        # Simula leitura de dados
        df = pd.DataFrame(
            {
                "feature_a": [1.2, 3.4, 5.6],
                "feature_b": [0.1, 0.2, 0.3],
                "target": [0, 1, 0],
            }
        )

        return {"dataframe": df, "shape": df.shape, "columns": list(df.columns)}

    # ──────────────────────────────────────────────
    # 2. Feature engineering  (KubernetesPodOperator)
    # ──────────────────────────────────────────────
    feat_eng = maga_ml_task(
        task_id="feature_engineering",
        team_pool=TEAM_POOL,
        script_path="equipe_fraude/scripts/feature_engineering.py",
        image=DEFAULT_IMAGE,
        env_vars={
            "INPUT_SHAPE": "{{ ti.xcom_pull(task_ids='extrai_dados', key='shape') }}",
        },
    )

    # ──────────────────────────────────────────────
    # 3. Treinamento  (KubernetesPodOperator)
    # ──────────────────────────────────────────────
    treino = maga_ml_task(
        task_id="treina_modelo",
        team_pool=TEAM_POOL,
        script_path="equipe_fraude/scripts/train.py",
        image=DEFAULT_IMAGE,
        env_vars={
            "MLFLOW_TRACKING_URI": Variable.get("mlflow_tracking_uri", default_var=""),
            "EXPERIMENT_NAME": "fraude_exemplo",
        },
    )

    # ──────────────────────────────────────────────
    # 4. Avaliação  (KubernetesPodOperator)
    # ──────────────────────────────────────────────
    # Adicionado do_xcom_push=True para garantir que avaliacao.output funcione
    avaliacao = maga_ml_task(
        task_id="avalia_modelo",
        team_pool=TEAM_POOL,
        script_path="equipe_fraude/scripts/evaluate.py",
        image=DEFAULT_IMAGE,
        do_xcom_push=True,
    )

    # ──────────────────────────────────────────────
    # 5. Promoção do modelo  (task Python)
    # ──────────────────────────────────────────────
    @task(
        task_id="promove_modelo",
        pool=TEAM_POOL,
    )
    def promove_modelo(avaliacao_result: str) -> str:
        """
        Se a avaliação foi bem-sucedida, registra o modelo no
        MLflow ou copia o artefato para o bucket de produção.
        """
        from airflow.models import Variable

        print(f"Resultado da avaliação: {avaliacao_result}")
        modelo_aprovado = True  # Avaliação hipotética

        if modelo_aprovado:
            version = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"Modelo promovido para produção — versão {version}")
            return f"promoted:{version}"

        print("Modelo NÃO aprovado — nenhuma promoção.")
        return "rejected"

    # ──────────────────────────────────────────────
    # 6. (Opcional) Carrega resultado para BigQuery
    # ──────────────────────────────────────────────
    @task(
        task_id="registra_metadata",
        pool=TEAM_POOL,
    )
    def registra_metadata(promocao: str) -> None:
        """Registra metadados da execução (ex.: no BigQuery ou GCS)."""
        from datetime import datetime, timezone

        # Usa os macros passados diretamente via contexto, em vez de Jinja puro dentro de @task
        run_id = "{{ run_id }}"
        execution_date = "{{ ds }}"

        print(f"Pipeline {run_id} executado em {execution_date}")
        print(f"Status da promoção: {promocao}")

        hook = GCSHook()
        log_content = (
            f"run_id: {run_id}\n"
            f"execution_date: {execution_date}\n"
            f"promocao: {promocao}\n"
            f"timestamp: {datetime.now(timezone.utc).isoformat()}\n"
        )
        hook.upload(
            bucket_name="{{ var.value.get('metadata_bucket', 'maga-bigdata-composer-bucket') }}",
            object_name=f"logs/ml_pipeline_example/{run_id}/metadata.txt",
            data=log_content.encode(),
        )

    # ──────────────────────────────────────────────
    # Dependências
    # ──────────────────────────────────────────────
    dados = extrai_dados()

    # 1. Encadeamento dos Operadores Clássicos (KPO)
    dados >> feat_eng >> treino >> avaliacao

    # 2. Encadeamento da TaskFlow API
    # Passamos avaliacao.output como argumento para promove_modelo.
    # O Airflow cria a dependência automaticamente: avaliacao >> promove_modelo
    resultado_promocao = promove_modelo(avaliacao_result=avaliacao.output)

    # Passamos o retorno de promove_modelo como argumento para registra_metadata.
    # O Airflow cria a dependência automaticamente: promove_modelo >> registra_metadata
    registra_metadata(promocao=resultado_promocao)
