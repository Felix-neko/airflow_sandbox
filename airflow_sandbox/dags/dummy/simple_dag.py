import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

import pendulum

from fs_etl.etl_repo import EtlRepo
from fs_etl.airflow_interaction import create_airflow_operator, run_dag_once, FsEtlDag


dag = FsEtlDag(render_template_as_native_obj=True)

repo_path = Path(__file__).parent.absolute()

mmb_oper = create_airflow_operator(repo_path, "MmbEtlTask", dag=dag, use_virtualenv=True, ti2="{{ti}}")

dummy_oper = create_airflow_operator(repo_path, "DummyEtlTask", dag=dag, use_virtualenv=False, ti3="{{ti}}")

def make_foo(*args, **kwargs):
    print("---> making foo!")
    print("make foo(...): args")
    print(args)
    print("make foo(...): kwargs")
    print(kwargs)



from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

MIN_REQUIREMENTS = ["cffi", "pyyaml", "jinja2", "sqlalchemy>=1.3", "psycopg2-binary>=2.9",
                    "typing-extensions", "pydantic<2", "pandas", "requirements-parser", "pyspark>=2.4,<3.4", "click",
                    "sqlparse", "cassandra-driver", "pyarrow", "cookiecutter", "inflection", "kubernetes"]
AIRFLOW_CONTEXT_KEYS = ["ds", "ts", "dag", "run_id", "dag_run", "macros", "data_interval_start", "data_interval_end"]
TASK_INSTANCE_CONTEXT_KEYS = ["ti", "task_instance"]


def create_baka_operator(dag: DAG):
    context_keys = AIRFLOW_CONTEXT_KEYS + TASK_INSTANCE_CONTEXT_KEYS
    context = {key: f"{{{{ {key} }}}}" for key in context_keys}
    oper = PythonOperator(
        dag=dag, task_id="baka_task",
        python_callable=make_foo, provide_context=True,
        op_args=[context, "hell", "yeah"], op_kwargs=context
        )

    return oper

baka_oper = create_baka_operator(dag)

# big_and_fat_oper = create_airflow_operator(repo_path, "BigFatEtlTask", dag=dag, use_virtualenv=False,
#                                            data_from_mmb="{{ti.xcom_pull(key='mmb_data', task_ids='MmbEtlTask')}}")
# small_and_thin_oper = create_airflow_operator(repo_path, "SmallThinEtlTask", dag=dag, use_virtualenv=False)
#
# mmb_oper >> big_and_fat_oper >> small_and_thin_oper

# dummy_oper >> mmb_oper

mmb_oper >> dummy_oper >> baka_oper

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import dill
#
# context = {"ts": "{{ ts }}", "dag_run": "{{ dag_run }}"}
#
#
# def make_foo(*args, **kwargs):
#     print("---> making foo!")
#     print("make foo(...): args")
#     print(args)
#     print("make foo(...): kwargs")
#     print(kwargs)
#
#
# make_foo_task = PythonVirtualenvOperator(
#     task_id='make_foo',
#     python_callable=make_foo,
#     use_dill=True,
#     system_site_packages=False,
#     op_args=[context],
#     requirements=[f"dill=={dill.__version__}", f"apache-airflow==2.8.0dev0", "psycopg2-binary >= 2.9, < 3",
#                   "pendulum==3.0.0b1", "lazy-object-proxy"],
#     dag=dag)
#
#
# mmb_oper >> make_foo_task

# if __name__ == "__main__":
#     run_dag_once(dag, scheduled_date=pendulum.datetime(2021, 1, 1))
    # dag.test(execution_date=pendulum.datetime(2021, 1, 1))

# import logging
# logging.getLogger().setLevel(logging.DEBUG)
# 
# 
if __name__ == "__main__":
    with EtlRepo(repo_path) as etl_repo:
        etl_task = etl_repo.get_etl_task("MmbEtlTask")
        etl_task.run(datetime.date(2021, 1, 1))