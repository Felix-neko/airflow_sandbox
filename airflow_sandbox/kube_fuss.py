import time
import yaml

from kubernetes import client, config, watch
from kubernetes.config.kube_config import Configuration

from devtools import debug


if __name__ == "__main__":
    # config.load_kube_config(config_file="/home/felix/.kube/config", context="minikube")
    config.load_kube_config()  # for local environment
    v1 = client.CoreV1Api()
    pods = v1.list_pod_for_all_namespaces()
    volumes = v1.list_persistent_volume()

    vol_body = client.V1PersistentVolume(
        api_version="v1",
        kind="PersistentVolume",
        metadata={"name": "host-dags-volume", "labels": {"type": "local"}},
        spec={"storageClassName": "manual", "accessModes": ["ReadWriteMany"],
              "capacity": {"storage": "10Gi"},
              "hostPath": {"path": "/dags"}}
    )

    # w = watch.Watch()
    # for event in w.stream(func=v1.delete_persistent_volume, name="host-dags-volume"):
    #     print(event)
    #     if event["type"] == "DELETED":
    #         w.stop()

    # try:
    #     v1.delete_persistent_volume("host-dags-volume")
    #     v1.delete_namespaced_persistent_volume_claim("host-dags-pvc", "default")
    #     time.sleep(1)
    # except:
    #     pass
    #
    # vol = v1.create_persistent_volume(vol_body, pretty=True)
    # debug(vol)
    # print(type(vol))
    #
    # vol_claim_body = client.V1PersistentVolumeClaim(
    #     api_version="v1",
    #     kind="PersistentVolumeClaim",
    #     metadata={"name": "host-dags-pvc"},
    #     spec={"storageClassName": "manual", "accessModes": ["ReadWriteMany"],
    #           "resources": {"requests": {"storage": "3Gi"}}}
    # )
    #
    # vol_claim = v1.create_namespaced_persistent_volume_claim("default", vol_claim_body)
    #
    # pod = v1.create_namespaced_pod(namespace=default, body=pod_body)

    nginx_pod_dict = yaml.safe_load(open("../nginx_pod.yaml"))

    # pod = v1.create_namespaced_pod("default", body=nginx_pod_dict)

    pod_body = client.V1Pod(
        api_version=nginx_pod_dict["apiVersion"],
        kind=nginx_pod_dict["kind"],
        metadata=nginx_pod_dict["metadata"],
        spec=nginx_pod_dict["spec"]
    )

    pod = v1.create_namespaced_pod("default", pod_body)