import pendulum
from airflow import DAG
from datetime import datetime, timedelta

from airflow.executors.debug_executor import DebugExecutor

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


dag = DAG('super_dooper_kuber',
          schedule_interval="@daily",
          render_template_as_native_obj=True,
          start_date=pendulum.datetime(2020, 1, 1),
          catchup=False)


passing = KubernetesPodOperator(
    namespace='default',
    image="ubuntu",
    cmds=["sh", "-c", "echo 'ds=={{ ds }}';mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
    name="passing-test",
    task_id="passing-task",
    dag=dag,
    do_xcom_push=True,
    )


if __name__ == "__main__":
    dag.clear()
    dag.run(start_date=dag.start_date, end_date=dag.start_date, local=True,
            run_at_least_once=True,
            executor=DebugExecutor()
            )
