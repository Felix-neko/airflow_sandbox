from pathlib import Path
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator


def do_nothing_and_sleep():
    import time
    print(">>> FALLING ASLEEP")
    time.sleep(300)
    print(">>> SLEEP OVER!")


dag = DAG('super_dooper_uber', description='Super Dooper Uber DAG', schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


hello_operator = PythonVirtualenvOperator(task_id='sleep_5_mins_2',
                                          python_callable=do_nothing_and_sleep, dag=dag,
                                          requirements=["sphinx"])



