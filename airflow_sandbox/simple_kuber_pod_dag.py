import pendulum
from airflow import DAG
from datetime import datetime, timedelta

from airflow.executors.debug_executor import DebugExecutor

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


k = KubernetesPodOperator(
    name="hello-dry-run",
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    task_id="dry_run_demo",
    do_xcom_push=True,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('super_dooper_kuber', default_args=default_args,
          schedule_interval=timedelta(minutes=10),
          render_template_as_native_obj=True)

passing = KubernetesPodOperator(
    namespace='default',
    image="ubuntu",
    cmds=["/usr/bin/echo", "Alright!"],
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
    # annotations={"sidecar.istio.io/inject": "false"}
    )


if __name__ == "__main__":
    dag.clear()
    logical_date = pendulum.datetime(2021, 1, 1, tz="UTC")
    dag.run(start_date=logical_date, end_date=logical_date, local=True,
            run_at_least_once=True,
            executor=DebugExecutor()
            )
