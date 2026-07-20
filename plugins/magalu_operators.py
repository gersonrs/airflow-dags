import os

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def maga_ml_task(
    task_id: str,
    team_pool: str,
    script_path: str,  # Ex: "equipe_fraude/scripts/train.py"
    env_vars: dict = None,
    image: str = "gcr.io/maga-bigdata/ml-base:latest",
    **kwargs,
) -> KubernetesPodOperator:

    # 1. Pega o nome do bucket dinamicamente (variável de ambiente que o Terraform criou)
    # Obs: O Composer injeta a variável GCS_BUCKET automaticamente com o bucket de DAGs
    bucket = os.environ.get("GCS_BUCKET", "maga-bigdata-composer-bucket")

    # 2. Monta o comando inteligente:
    # Ele copia o arquivo do GCS para dentro do Pod (/tmp/) e depois executa
    comando_magico = (
        f"gcloud storage cp gs://{bucket}/dags/{script_path} /tmp/script.py "
        f"&& python /tmp/script.py"
    )

    return KubernetesPodOperator(
        task_id=task_id,
        name=f"ml-pod-{task_id.replace('_', '-')}",
        namespace="default",
        image=image,
        pool=team_pool,
        # O PULO DO GATO: Usamos o bash para rodar nosso comando duplo
        cmds=["bash", "-c"],
        arguments=[comando_magico],
        deferrable=True,
        is_delete_operator_pod=True,
        env_vars=env_vars or {},
        **kwargs,
    )
