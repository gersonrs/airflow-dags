from __future__ import annotations

from airflow.models import DagBag


def test_dag_integrity() -> None:
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert dag_bag.import_errors == {}, "DAGs com erro de importação"
