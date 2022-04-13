from typing import Union
import logging
import shutil
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.executors.debug_executor import DebugExecutor



def wheres_my_args_dude(*args, **kwargs):
    what_to_print = f"""
    
=====
ARGS
=====
{args}

=======
KWARGS
=======
{kwargs}

"""
    print(what_to_print)



def create_venv_operator(
        name: str, dag: DAG, use_virtualenv: bool, *args) -> Union[PythonVirtualenvOperator, PythonOperator]:


    # hello_world_operator = PythonOperator(dag=dag, task_id="hello_world", python_callable=hello_world, op_args=[1, 2, 3])

    if use_virtualenv:
        oper = PythonVirtualenvOperator(
            dag=dag, task_id=name, requirements=["dill"],
            python_callable=wheres_my_args_dude, op_args=args, use_dill=True)
    else:
        oper = PythonOperator(dag=dag, task_id=name, python_callable=wheres_my_args_dude)

    return oper




if __name__ == "__main__":
    # Я знаю, что сейчас 2k22 год, и надо работать через контексты и декораторы. Это специально для простоты отладки.
    dag = DAG(
        dag_id='simple_python_dag_2',
        schedule_interval="@daily",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['example'],
    )

    venv_oper_1 = create_venv_operator("oper1", dag, [1, 2, 3])
    venv_oper_2 = create_venv_operator("oper2", dag, [4, 5, 6])

    venv_oper_1 >> venv_oper_2

    dag.clear()
    # лол, чтобы брякпоинты заработали (и чтобы принты заработали), надо запускать с новой датой...
    dag.run(start_date=pendulum.datetime(2021, 5, 1, tz="Europe/Moscow"),
            end_date=pendulum.datetime(2021, 5, 2, tz="Europe/Moscow"),
            executor=DebugExecutor(), run_at_least_once=True)
