# version: 3

import datetime
import pendulum
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import dill

from strange_pickling_error.some_moar_code import make_foo


dag = DAG(
    dag_id='strange_pickling_error_dag',
    schedule_interval='0 5 * * 1',
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
)


context = {"ts": "{{ ts }}", "dag_run": "{{ dag_run }}"}


make_foo_task = PythonVirtualenvOperator(
    task_id='make_foo',
    python_callable=make_foo,
    use_dill=True,
    system_site_packages=False,
    op_args=[context],
    requirements=[f"dill=={dill.__version__}", f"apache-airflow=={airflow.__version__}", "psycopg2-binary >= 2.9, < 3",
                  f"pendulum=={pendulum.__version__}", "lazy-object-proxy"],
    dag=dag)