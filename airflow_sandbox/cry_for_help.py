from typing import Union

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.executors.debug_executor import DebugExecutor

from airflow_sandbox.args_checks import wheres_my_args_dude


def create_airflow_operator(name: str, dag: DAG, use_virtualenv: bool,
                            *args) -> Union[PythonVirtualenvOperator, PythonOperator]:

    if use_virtualenv:
        oper = PythonVirtualenvOperator(
            dag=dag, task_id=name, requirements=["dill"],
            python_callable=wheres_my_args_dude, op_args=args, use_dill=True,
            system_site_packages=False)
    else:
        oper = PythonOperator(dag=dag, task_id=name, python_callable=wheres_my_args_dude, op_args=args)

    return oper


if __name__ == "__main__":
    # Здесь я запускаю простенький оператор PythonOperator и PythonVirtualenvOperator
    # И оказывается, что в PythonOperator все kwargs-аргументы передаются (в частности, ds),
    # а в PythonVirutalenvOperator передаётся только часть kwargs-аргументов (и моего любимого любимый ds там нету)
    # Конкретный состав kwargs-аргументов может отличаться от версии к версии airflow,
    # я тестировался на apache-airflow==2.2.2


    # Я знаю, что сейчас 2k22 год, и надо работать через контексты и декораторы. Это специально для простоты отладки.
    dag = DAG(
        dag_id='simple_python_dag_2',
        schedule_interval="@daily",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['example'],
    )

    python_oper = create_airflow_operator("python_oper", dag, False, [1, 2, 3])
    venv_oper = create_airflow_operator("venv_oper", dag, True, [4, 5, 6])

    python_oper >> venv_oper

    dag.clear()
    dag.run(start_date=pendulum.datetime(2021, 5, 1, tz="Europe/Moscow"),
            end_date=pendulum.datetime(2021, 5, 2, tz="Europe/Moscow"),
            executor=DebugExecutor(), run_at_least_once=True)
