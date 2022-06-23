import datetime
from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator

from fs_etl.airflow_interaction import run_dag_once

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
    run_dag_once(dag, datetime.datetime(2021, 5, 1))


# if __name__ == "__main__":
#
#     from fs_etl.etl_repo import EtlRepo
#     etl_repo = EtlRepo(repo_path)
#     etl_task = etl_repo.get_etl_task("SmallThinEtlTask")
#     print(etl_task.spark_session.sparkContext.getConf().getAll())