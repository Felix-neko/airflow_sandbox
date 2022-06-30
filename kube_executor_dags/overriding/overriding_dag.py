from pathlib import Path
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s


def do_nothing_and_sleep():
    # print(">>> FALLING ASLEEP")
    # time.sleep(3)
    # print(">>> SLEEP OVER!!!!")
    import fs_etl

    print(">>> FS ETL somehow imported!")

dag = DAG('overriding', description='Super Dooper Pod Overriding DAG', schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


overriding_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(name="overriding-pod"),
    kind="Pod",
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(name="overriding-container",
                            image="mlops-docker-snapshots.binary.alfabank.ru/fs_etl_basic:v0.3",
                            ports=[k8s.V1ContainerPort(80)])
        ]
    )
)


hello_operator = PythonOperator(task_id='sleep_5_mins', python_callable=do_nothing_and_sleep, dag=dag,
                                executor_config={
                                    "pod_override": overriding_pod
                                },
                                )

