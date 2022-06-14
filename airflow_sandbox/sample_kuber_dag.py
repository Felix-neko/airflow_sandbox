from typing import Union
from datetime import date, datetime

from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from airflow.executors.debug_executor import DebugExecutor
from airflow.executors.kubernetes_executor import KubernetesExecutor


def run_dag_once(dag: DAG, calc_date: Union[date, datetime], use_kuber: bool = False):
    """
    Однократный запуск DAGа airflow

    Args:
        dag: какой DAG запускаем
        calc_date: логическая дата запуска (дата или datetime для запуска на конкретное время)
    """
    if not isinstance(calc_date, date):
        raise TypeError("calc_date should be a datetime.date or datetime.datetime object!")

    if isinstance(calc_date, datetime):
        logical_date = pendulum.datetime(
            calc_date.year,
            calc_date.month,
            calc_date.day,
            hour=calc_date.hour,
            minute=calc_date.minute,
            second=calc_date.second,
            microsecond=calc_date.microsecond
        )
    else:
        logical_date = pendulum.datetime(calc_date.year, calc_date.month, calc_date.day)

    dag.clear()
    # executor = DebugExecutor()
    executor = KubernetesExecutor()

    dag.run(start_date=logical_date, end_date=logical_date, executor=executor, run_at_least_once=True)


dag = DAG(
    dag_id='xcom_fuss',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dummy'],
)


def run_first(*args, **kwargs):
    ti = kwargs["task_instance"]
    ti.xcom_push(key="hell", value=["yeah!"])


first_oper = PythonOperator(
    dag=dag, task_id="first_oper",
    python_callable=run_first, provide_context=True,
    )


def run_second(*args, **kwargs):
    ti = kwargs["task_instance"]
    val_from_first = ti.xcom_pull(key="hell", task_ids="first_oper")
    print(">> runnin' second")
    print(val_from_first)


second_oper = PythonOperator(
    dag=dag, task_id="second_oper",
    python_callable=run_second, provide_context=True,
    )


first_oper >> second_oper


if __name__ == "__main__":
    run_dag_once(dag, datetime(2021, 5, 1))
