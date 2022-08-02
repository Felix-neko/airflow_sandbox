import datetime
from pathlib import Path
from airflow import DAG
import pendulum

from airflow.operators.python import PythonVirtualenvOperator

dag = DAG(
    dag_id='hello_world_dag',
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dummy'],
    render_template_as_native_obj=True,
)

# repo_path = Path(__file__).parent.absolute()
#
# mmb_oper = create_airflow_operator(repo_path, "MmbEtlTask", dag=dag, use_virtualenv=False, ti2="{{ti}}")
# big_and_fat_oper = create_airflow_operator(repo_path, "BigFatEtlTask", dag=dag, use_virtualenv=False,
#                                            data_from_mmb="{{ti.xcom_pull(key='mmb_data', task_ids='MmbEtlTask')}}")
# small_and_thin_oper = create_airflow_operator(repo_path, "SmallThinEtlTask", dag=dag, use_virtualenv=False)
#
# mmb_oper >> big_and_fat_oper >> small_and_thin_oper


def hello_world():
    print("Hello world!")


PythonVirtualenvOperator(python_callable=hello_world, python_version="3.7",
                         use_dill=True, system_site_packages=False, dag=dag, task_id="hello_world",
                         requirements=["numpy",
                                       "fs_etl",
                                       "--index-url=https://artapp.moscow.alfaintra.net/artifactory/api/pypi/pypi-remote/simple "
                                       "--trusted-host=artapp.moscow.alfaintra.net "
                                       "--extra-index-url=https://binary.alfabank.ru/artifactory/api/pypi/fs_etl-pypi/simple"
                                       ])



# if __name__ == "__main__":
#     with EtlRepo(repo_path) as etl_repo:
#         etl_task = etl_repo.get_etl_task("MmbEtlTask")
#         etl_task.run(datetime.date(2021, 1, 5))