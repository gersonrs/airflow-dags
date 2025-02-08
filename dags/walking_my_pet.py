"""
### Simple DAG that runs one task with params

This DAG uses one string type param and uses it in a python decorated task.
"""
from __future__ import annotations

import random
from typing import Any

from airflow.decorators import dag
from airflow.decorators import task
from airflow.models.param import Param
from pendulum import datetime


@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={"pet_name": Param("Undefined!", type="string")},
)
def walking_my_pet() -> None:
    @task
    def walking_your_pet(**context: dict[str, Any]) -> None:
        pet_name = context["params"]["pet_name"]
        minutes = random.randint(2, 10)
        print(f"{pet_name} has been on a {minutes} minute walk!")

    walking_your_pet()


walking_my_pet()
