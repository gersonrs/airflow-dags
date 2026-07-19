import os
import pickle
import uuid
from typing import Any

import pandas as pd
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class GCSXComBackend(BaseXCom):

    @staticmethod
    def get_xcom_bucket():
        # Lê a variável injetada pelo Terraform
        bucket = os.environ.get("XCOM_GCS_BUCKET")
        if not bucket:
            raise ValueError("A variável de ambiente XCOM_GCS_BUCKET não está configurada.")
        return bucket

    @staticmethod
    def serialize_value(value: Any, **kwargs):
        if isinstance(value, pd.DataFrame):
            hook = GCSHook()
            key = f"xcom_data/{uuid.uuid4()}.pickle"
            data = pickle.dumps(value)

            # Pega o nome do bucket dinamicamente
            bucket_name = GCSXComBackend.get_xcom_bucket()

            hook.upload(bucket_name=bucket_name, object_name=key, data=data)

            reference_dict = {"gcs_xcom_reference": f"gs://{bucket_name}/{key}"}
            return BaseXCom.serialize_value(reference_dict, **kwargs)

        return BaseXCom.serialize_value(value, **kwargs)

    @staticmethod
    def deserialize_value(result) -> Any:
        # Puxa o dado do banco de metadados
        result_deserialized = BaseXCom.deserialize_value(result)

        # Verifica se é uma referência nossa do GCS
        if isinstance(result_deserialized, dict) and "gcs_xcom_reference" in result_deserialized:
            path = result_deserialized["gcs_xcom_reference"]

            # Quebra a string "gs://bucket/caminho"
            path_parts = path.split("/")
            bucket = path_parts[2]
            key = "/".join(path_parts[3:])

            # Baixa do GCS e reconstrói o DataFrame
            hook = GCSHook()
            data = hook.download(bucket_name=bucket, object_name=key)
            return pickle.loads(data)

        # Se não for nossa referência, devolve o dado normal
        return result_deserialized
