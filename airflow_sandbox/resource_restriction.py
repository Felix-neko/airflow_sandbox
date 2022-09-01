from kubernetes import client, config
from kubernetes.config.kube_config import ConfigException
from kubernetes.client import models as k8s
# For port-forwarding (debug purposes)
# kubectl port-forward nginx-pod 9080:80


if __name__ == "__main__":

    try:
        config.load_incluster_config()
        is_running_in_kube = True
    except ConfigException:
        is_running_in_kube = False
        config.load_kube_config()

    v1 = client.CoreV1Api()

    pods = v1.list_pod_for_all_namespaces()
    volumes = v1.list_persistent_volume()

    nginx_pod = k8s.V1Pod(
        kind="Pod",
        metadata=k8s.V1ObjectMeta(name="nginx-pod"),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="nginx-container", image="nginx:latest", ports=[k8s.V1ContainerPort(80)],
                    resources=k8s.V1ResourceRequirements(
                        requests={'cpu': '200m', "memory": "200Mi"}, limits={})
                )
            ]
        )
    )

    api_response = v1.create_namespaced_pod(namespace="default", body=nginx_pod)

    print(api_response)



