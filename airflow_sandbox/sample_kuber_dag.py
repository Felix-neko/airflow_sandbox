from typing import Union
from datetime import date, datetime

from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from airflow.executors.debug_executor import DebugExecutor
from airflow.executors.kubernetes_executor import KubernetesExecutor


dag = DAG(
    dag_id='xcom_fuss',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dummy'],
    render_template_as_native_obj=True
)


def run_first(*args, **kwargs):
    ti = kwargs["task_instance"]
    ti.xcom_push(key="hell", value=["yeah!"])


first_oper = PythonOperator(
    dag=dag, task_id="first_oper",
    python_callable=run_first, provide_context=True,
    )


if __name__ == "__main__":
    logical_date = pendulum.datetime(2021, 5, 1)

    dag.clear()
    # executor = DebugExecutor()
    executor = KubernetesExecutor()
    dag.run(start_date=logical_date, end_date=logical_date, executor=executor, run_at_least_once=True)
