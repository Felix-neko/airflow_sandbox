from pathlib import Path

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import fs_etl


def wheres_my_path_dude():


    file_path = Path(__file__).absolute()
    print(f">>>>>>>>>>\npath=={file_path}")
    print(list(file_path.parent.iterdir()))


dag = DAG('hello_world', description='Hello world example', schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


hello_operator = PythonOperator(task_id='path_task', python_callable=wheres_my_path_dude, dag=dag)

