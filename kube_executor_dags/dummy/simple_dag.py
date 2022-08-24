import datetime
from pathlib import Path
from airflow import DAG
import pendulum

from fs_etl.airflow_interaction import create_airflow_operator, run_dag_once
from fs_etl.etl_repo import EtlRepo

dag = DAG(
    dag_id='fs_etl_simple',
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dummy'],
    render_template_as_native_obj=True,
)

repo_path = Path(__file__).parent.absolute()

mmb_oper = create_airflow_operator(repo_path, "MmbEtlTask", dag=dag, use_virtualenv=True, ti2="{{ti}}")
# big_and_fat_oper = create_airflow_operator(repo_path, "BigFatEtlTask", dag=dag, use_virtualenv=False,
#                                            data_from_mmb="{{ti.xcom_pull(key='mmb_data', task_ids='MmbEtlTask')}}")
# small_and_thin_oper = create_airflow_operator(repo_path, "SmallThinEtlTask", dag=dag, use_virtualenv=False)
#
# mmb_oper >> big_and_fat_oper >> small_and_thin_oper


# if __name__ == "__main__":
#     run_dag_once(dag, datetime.date.today())


if __name__ == "__main__":
    with EtlRepo(repo_path) as etl_repo:
        # etl_task = etl_repo.get_etl_task("MmbEtlTask")
        # etl_task.run(datetime.date(2021, 1, 5))

        from devtools import debug
        debug(etl_repo.settings)
