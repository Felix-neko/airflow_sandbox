
import logging
import shutil
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.executors.debug_executor import DebugExecutor

# Я знаю, что сейчас 2k22 год, и надо работать через контексты и декораторы. Это специально для простоты.
dag = DAG(
    dag_id='simple_python_dag_2',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)

# Вот эта функция печатает всё в логи при запуске через GUI Airflow,
# но ни хрена не печатает при запуске через python3 simply_python_dag.py
def hello_world():
    print("=================")
    print("Hello world!!!")
    print("=================")

hello_world_operator = PythonOperator(dag=dag, task_id="hello_world", python_callable=hello_world)


if __name__ == "__main__":
    dag.clear()
    # лол, чтобы брякпоинты заработали (и чтобы принты заработали), нало запускать с новой датой...
    dag.run(start_date=pendulum.now(), executor=DebugExecutor(),
            run_at_least_once=True)