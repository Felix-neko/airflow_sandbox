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

