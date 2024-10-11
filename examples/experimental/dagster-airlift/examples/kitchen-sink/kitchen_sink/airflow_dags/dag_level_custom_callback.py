from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from dagster_airlift.in_airflow import proxying_to_dagster, DefaultProxyDAGToDagsterOperator 
from dagster_airlift.in_airflow.dag_proxy_operator import BaseProxyDAGToDagsterOperator
from dagster_airlift.in_airflow.proxied_state import load_proxied_state_from_yaml


def print_hello() -> None:
    print("Hello")  # noqa: T201


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


with DAG(
    "overridden_dag_custom_callback",
    default_args=default_args,
    schedule_interval=None,
    is_paused_upon_creation=False,
) as dag:
    PythonOperator(task_id="print_task", python_callable=print_hello) << PythonOperator(
        task_id="downstream_print_task", python_callable=print_hello
    )  # type: ignore

def custom_callback(dag: DAG) -> BaseProxyDAGToDagsterOperator:
    return DefaultProxyDAGToDagsterOperator(dag=dag, task_id="OVERRIDDEN")


proxying_to_dagster(
    proxied_state=load_proxied_state_from_yaml(Path(__file__).parent / "proxied_state"),
    global_vars=globals(),
    full_dag_operator_construction_callback=custom_callback,
)
